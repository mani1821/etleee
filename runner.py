"""
The flow for each table — read this file first to understand the framework.

    1. schema           columns + types (from Oracle ALL_TAB_COLUMNS / Unity Catalog information_schema)
    2. row_count        exact COUNT(*) on both sides
    3. row_hash         every row (<= sample_threshold_rows) or a sample-and-lookup sample:
                        null keys, duplicate keys, missing / extra keys, row hash, differing columns
    4. column_profiles  per column: nulls, distinct, min / max / sum, text length
    4b. column_distributions  text / date / timestamp columns: row counts per value / length / day / hour
    5. incremental      (tables with an incremental block) rows written by this load run:
                        delta row count, delta keys, delta values
"""
from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .checks import (RowSetResult, compare_distributions, compare_profiles, compare_rows, compare_rowsets, compare_schema, key_json,
                     normalize, try_cache, try_unpersist)
from .config import Settings, TableSpec, build_spec, load_config
from .readers import make_reader
from .results import Recorder
from .sampling import keyless_fingerprint_rows, keyless_rows, rows_to_compare


@dataclass
class ParityRun:
    run_id: str
    summary: DataFrame      # one row per table per validation (flat)
    summary_nested: DataFrame   # one row per table; validations and failed checks nested inside
    results: DataFrame      # one row per check
    failed: bool            # any FAIL or ERROR
    report_html: str = ""   # the run report (displayHTML(run.report_html) in a notebook)
    report_text: str = ""
    report_file: Optional[str] = None   # where the HTML report was written (settings.report_path)
    email_sent: bool = False
    email_error: Optional[str] = None
    label: str = ""                     # run name in combined reports (default: YAML file name)
    results_table: Optional[str] = None
    mismatch_table: Optional[str] = None


def _diff_cols(per_column: dict, limit: int = 6) -> str:
    items = [f"{c}({n:,})" for c, n in list(per_column.items())[:limit]]
    return ", ".join(items) + (", ..." if len(per_column) > limit else "")


class TableValidator:
    def __init__(self, src, tgt, settings: Settings, rec: Recorder):
        self.src, self.tgt, self.s, self.rec = src, tgt, settings, rec

    def row_tolerance(self, spec: TableSpec) -> float:
        return spec.row_mismatch_tolerance_pct if spec.row_mismatch_tolerance_pct is not None \
            else self.s.row_mismatch_tolerance_pct

    # ------------------------------------------------------------------ entry point
    def run(self, spec: TableSpec):
        # timestamp truncation: the table's own value, else the settings value; "none" switches it off for a table
        ts = spec.ts_truncate if spec.ts_truncate is not None else self.s.ts_truncate
        spec.ts_truncate = None if ts is None or str(ts).lower() == "none" else str(ts).lower()
        cols, non_keys = self.check_schema(spec)
        keyed = bool(spec.keys)                 # no keys = no grain: whole-row comparison instead
        if not (spec.incremental and spec.incremental.delta_only):
            n_src, n_tgt = self.check_row_count(spec)
            if keyed:
                self.check_rows(spec, cols, non_keys, n_src, n_tgt)
            else:
                self.check_rows_keyless(spec, cols, n_src, n_tgt)
        if spec.incremental:
            if keyed:
                self.check_incremental(spec, cols, non_keys)
            else:
                self.check_incremental_keyless(spec, cols)

    # ------------------------------------------------------------------ 1. schema
    def check_schema(self, spec: TableSpec):
        t, excl, V = spec.name, set(spec.exclude_cols), "schema"
        sc, tc = self.src.columns(spec.name), self.tgt.columns(spec.target_name)
        r = compare_schema(sc, tc, excl)
        ok_cols = not r.only_src and not r.only_tgt
        self.rec.rec(t, V, "columns", "PASS" if ok_cols else "FAIL", r.n_src, r.n_tgt,
                     f"only_in_src={r.only_src} only_in_tgt={r.only_tgt}")
        nullable = f"; nullable differs (info): {r.nullable_diffs}" if r.nullable_diffs else ""
        self.rec.rec(t, V, "types", "PASS" if not r.type_diffs else "FAIL",
                     detail="; ".join(r.type_diffs) + nullable)
        parts = []
        if not ok_cols:
            parts.append(f"only_in_src={r.only_src} only_in_tgt={r.only_tgt}")
        if r.type_diffs:
            parts.append("; ".join(r.type_diffs))
        self.rec.note(t, V, "; ".join(parts) if parts else f"columns {r.n_src} = {r.n_tgt}, types match")

        tgt_names = {c.name for c in tc}
        cols = [c.name for c in sc if c.name not in excl and c.name in tgt_names]
        missing_keys = [k for k in spec.keys if k not in cols]
        if missing_keys:
            raise ValueError(f"key column(s) {missing_keys} not found on both sides")
        return cols, [c for c in cols if c not in spec.keys]

    # ------------------------------------------------------------------ 2. row count
    def check_row_count(self, spec: TableSpec):
        t = spec.name
        n_src, n_tgt = self.src.count(spec.name, spec.filter), self.tgt.count(spec.target_name, spec.filter)
        diff_pct = abs(n_src - n_tgt) / max(n_src, 1) * 100
        tol = self.s.count_tolerance_pct
        self.rec.rec(t, "row_count", "row_count", self._within(abs(n_src - n_tgt), diff_pct, tol), n_src, n_tgt,
                     f"diff_pct={diff_pct:.4f}, count tolerance {tol}%")
        d = n_tgt - n_src
        self.rec.note(t, "row_count", f"{n_src:,} vs {n_tgt:,}" + (f" ({d:+,})" if d else ""))
        return n_src, n_tgt

    # ------------------------------------------------------------------ 3 + 4. rows and profiles
    def check_rows(self, spec: TableSpec, cols, non_keys, n_src: int, n_tgt: int):
        t, V = spec.name, "row_hash"
        rows = rows_to_compare(self.src, self.tgt, spec, cols, self.s, spec.filter, spec.filter,
                               n_src, n_tgt, cache=try_cache)
        scope = rows.scope
        if rows.notes:
            self.rec.rec(t, V, "sampling", "INFO", detail=" | ".join(rows.notes), scope=scope)
        src_df = try_cache(normalize(rows.src, spec.ts_truncate))
        tgt_df = try_cache(normalize(rows.tgt, spec.ts_truncate))
        try:
            r = compare_rows(src_df, tgt_df, spec.keys, non_keys, spec.num_tolerance)
            rec = lambda check, ok, src=None, tgt=None, detail="", status=None: self.rec.rec(
                t, V, check, status or ("PASS" if ok else "FAIL"), src, tgt, detail, scope)
            rec("src_null_keys", r.src_null_keys == 0, r.src_null_keys, detail=f"{r.src_null_keys} rows with a NULL key")
            rec("tgt_null_keys", r.tgt_null_keys == 0, tgt=r.tgt_null_keys, detail=f"{r.tgt_null_keys} rows with a NULL key")
            dup_note = " (left out of the value comparison)" if r.dup_excluded else ""
            rec("src_duplicate_keys", r.src_dup_keys == 0, r.src_dup_keys, detail=f"{r.src_dup_keys} keys with more than one row{dup_note}")
            rec("tgt_duplicate_keys", r.tgt_dup_keys == 0, tgt=r.tgt_dup_keys, detail=f"{r.tgt_dup_keys} keys with more than one row{dup_note}")
            self._dup_mismatches(t, V, spec.keys, r)
            # missing / extra keys: same tolerance as the row count (count_tolerance_pct), within = WARN
            ctol, est = self.s.count_tolerance_pct, ", estimated from the sample" if scope == "sample" else ""
            mpct, xpct = r.missing / max(r.src_keys, 1) * 100, r.extra / max(r.tgt_keys, 1) * 100
            rec("missing_keys", None, r.missing, status=self._within(r.missing, mpct, ctol),
                detail=f"{r.missing:,} of {r.src_keys:,} source keys not in target ({mpct:.4f}%{est}), "
                       f"count tolerance {ctol}%")
            rec("extra_keys", None, tgt=r.extra, status=self._within(r.extra, xpct, ctol),
                detail=f"{r.extra:,} of {r.tgt_keys:,} target keys not in source ({xpct:.4f}%{est}), "
                       f"count tolerance {ctol}%")
            if r.missing:
                self.rec.mismatches(t, V, "missing_in_target", r.missing_df.select(key_json(spec.keys).alias("key")))
            if r.extra:
                self.rec.mismatches(t, V, "extra_in_target", r.extra_df.select(key_json(spec.keys).alias("key")))
            ok = self._hash_checks(t, V, scope, r, spec, kind="value_mismatch")
            self._rows_compared(t, V, scope, r.src_rows, r.tgt_rows, n_src, n_tgt)

            note = self._scope_text(scope, r.src_rows, r.tgt_rows) + ": "
            if r.src_null_keys or r.tgt_null_keys:
                note += f"NULL keys {r.src_null_keys} / {r.tgt_null_keys}; "
            note += f"{r.missing:,} missing, {r.extra:,} extra; "
            if r.dup_excluded:
                note += f"{r.dup_excluded:,} duplicated key(s) left out of the value comparison; "
            if r.differing:
                note += f"{r.differing:,} of {r.matched:,} row{'s' if r.matched != 1 else ''} differ: {_diff_cols(r.per_column)}"
                note += " (within row tolerance)" if ok else ""
            else:
                note += f"{r.matched:,} matched rows identical"
            self.rec.note(t, V, note)

            # 4. column profiles: non-key columns, rows whose key exists on both sides
            #    (missing / extra rows are already reported above; here we compare value distributions)
            common = src_df.select(*spec.keys).distinct().join(tgt_df.select(*spec.keys).distinct(), spec.keys)
            if r.dup_keys_df is not None:                # same rows as the value comparison
                common = common.join(r.dup_keys_df, spec.keys, "left_anti")
            src_p, tgt_p = src_df.join(common, spec.keys, "left_semi"), tgt_df.join(common, spec.keys, "left_semi")
            diffs = compare_profiles(src_p, tgt_p, non_keys, spec.num_tolerance, r.within_tolerance)
            bad_status = "WARN" if self.row_tolerance(spec) > 0 else "FAIL"
            for c, d in diffs.items():
                self.rec.rec(t, "column_profiles", f"profile:{c}", bad_status if d else "PASS",
                             detail="; ".join(d), scope=scope)
            bad = {c: d for c, d in diffs.items() if d}
            self.rec.note(t, "column_profiles", "; ".join(f"{c} {d[0]}" + (" ..." if len(d) > 1 else "")
                                                          for c, d in list(bad.items())[:4])
                          + (f"; +{len(bad) - 4} more columns" if len(bad) > 4 else ""))
            self._record_distributions(spec, scope, src_p, tgt_p, non_keys)
        finally:
            try_unpersist(src_df, tgt_df)

    def _rows_compared(self, t, V, scope, n_cmp_s, n_cmp_t, n_tab_s, n_tab_t, check="rows_compared", extra=""):
        """INFO row: how many rows were compared (src_value / tgt_value) out of how many in the table / delta."""
        if "sample" in scope:
            how = (f"sample: {n_cmp_s / max(n_tab_s, 1) * 100:.2f}% of {n_tab_s:,} source rows, "
                   f"{n_cmp_t / max(n_tab_t, 1) * 100:.2f}% of {n_tab_t:,} target rows")
        elif "fingerprint" in scope:
            how = "all rows, via bucket fingerprints"
        else:
            how = "all rows"
        self.rec.rec(t, V, check, "INFO", n_cmp_s, n_cmp_t,
                     f"{n_cmp_s:,} source rows vs {n_cmp_t:,} target rows compared ({how}){extra}", scope)

    @staticmethod
    def _scope_text(scope, n_s, n_t):
        return f"{'sample' if 'sample' in scope else 'all'} {n_s:,} vs {n_t:,} rows"

    def _dup_mismatches(self, t, V, keys, r):
        """Example duplicated keys (with their number of rows) for the mismatch table."""
        for side, n, df in (("src", r.src_dup_keys, r.src_dup_df), ("tgt", r.tgt_dup_keys, r.tgt_dup_df)):
            if n:
                self.rec.mismatches(t, V, f"duplicate_key_{side}", df.orderBy(F.desc("count")).select(
                    key_json(keys).alias("key"), F.col("count").cast("string").alias(f"{side}_value")))

    def _hash_checks(self, t, V, scope, r, spec, kind: str, check: str = "hash_match",
                     tol: Optional[float] = None, warn_within: bool = False) -> bool:
        """hash_match + one column_diff check per differing column. Returns True if within tolerance.
        tol: % of matched rows allowed to differ (default: the table's row tolerance);
        warn_within: a non-zero difference within tolerance is WARN instead of PASS."""
        tol = self.row_tolerance(spec) if tol is None else tol
        if r.matched == 0:                       # nothing on both sides: a PASS would mean nothing
            self.rec.rec(t, V, check, "WARN", 0, 0, "no rows exist on both sides to compare values "
                         "(check missing / extra keys and the filters)", scope)
            return True
        pct = r.differing / max(r.matched, 1) * 100
        ok = pct <= tol
        dup = f"; {r.dup_excluded:,} duplicated key(s) left out" if r.dup_excluded else ""
        status = "FAIL" if not ok else ("WARN" if r.differing else "PASS")      # differences within tolerance = WARN
        self.rec.rec(t, V, check, status, r.matched - r.differing, r.matched,
                     f"{r.differing:,} of {r.matched:,} matched rows differ ({pct:.4f}%), row tolerance {tol}%{dup}"
                     + (f"; columns: {_diff_cols(r.per_column, 20)}" if r.per_column else ""), scope)
        if check == "hash_match":
            # each column on its own: % of matched rows that differ in this column vs the same row tolerance
            for c, n in r.per_column.items():
                cpct = n / r.matched * 100
                self.rec.rec(t, V, f"column_diff:{c}", "WARN" if cpct <= tol else "FAIL", r.matched - n, r.matched,
                             f"{n:,} of {r.matched:,} matched rows differ in {c} ({cpct:.4f}%), row tolerance {tol}%",
                             scope)
        if r.differing:
            self.rec.mismatches(t, V, kind, r.diff_df)
        return ok

    # ------------------------------------------------------------------ 4b. column distributions
    def _record_distributions(self, spec: TableSpec, scope, src, tgt, cols):
        """distribution:<COL> per text / date / timestamp column: row counts per value / length / day / month / hour."""
        if not self.s.distributions:
            return
        t, V = spec.name, "column_distributions"
        tol = spec.distribution_tolerance_pct if spec.distribution_tolerance_pct is not None \
            else self.s.distribution_tolerance_pct
        rows_tol = spec.distribution_tolerance_rows if spec.distribution_tolerance_rows is not None \
            else self.s.distribution_tolerance_rows
        res, mism = compare_distributions(src, tgt, cols, self.s.distribution_max_categories, tol, rows_tol)
        limit = f"{tol}% or {rows_tol:,} rows" if rows_tol else f"{tol}%"
        if not res:
            return
        bad = {}
        for c, d in res.items():
            kinds = ", ".join(f"{k} ({n:,} buckets)" for k, n in d["kinds"].items())
            fmt = lambda xs: "; ".join(f"{k} {b}: {s_:,} vs {t_:,}" for k, b, s_, t_ in xs)
            within = f"; {d['n_within']:,} more within tolerance" if d["n_within"] else ""
            if d["n_diff"]:
                bad[c] = d["diff"][0] if d["diff"] else ("?", "?", 0, 0)
                self.rec.rec(t, V, f"distribution:{c}", "FAIL", d["n_diff"], sum(d["kinds"].values()),
                             f"{d['n_diff']:,} bucket(s) differ beyond {limit}{within} - by {kinds}: {fmt(d['diff'])}", scope)
            elif d["n_within"]:                          # differences, all within tolerance: WARN, not a silent PASS
                self.rec.rec(t, V, f"distribution:{c}", "WARN", d["n_within"], sum(d["kinds"].values()),
                             f"{d['n_within']:,} bucket(s) differ, all within {limit} - by {kinds}: {fmt(d['within'])}", scope)
            else:
                self.rec.rec(t, V, f"distribution:{c}", "PASS", detail=f"identical by {kinds}", scope=scope)
        if bad:
            self.rec.mismatches(t, V, "distribution", mism)
        self.rec.note(t, V, "; ".join(f"{c} {k} {b}: {s_:,} vs {t_:,}" for c, (k, b, s_, t_) in list(bad.items())[:4])
                      + (f"; +{len(bad) - 4} more columns" if len(bad) > 4 else ""))

    # ------------------------------------------------------------------ 5. incremental
    def _delta_tol(self, spec: TableSpec, name: str) -> float:
        """Incremental tolerance: the table's incremental block, else the setting (values: else row tolerance)."""
        v = getattr(spec.incremental, name)
        if v is not None:
            return v
        if name == "count_tolerance_pct":
            return self.s.delta_count_tolerance_pct
        if name == "key_tolerance_pct":
            return self.s.delta_key_tolerance_pct
        return self.s.delta_value_tolerance_pct if self.s.delta_value_tolerance_pct is not None \
            else self.row_tolerance(spec)

    @staticmethod
    def _within(diff: int, pct: float, tol: float) -> str:
        return "PASS" if diff == 0 else ("WARN" if pct <= tol else "FAIL")

    def _delta_count(self, t, V, spec, n_s, n_t, sw, tw):
        tol = self._delta_tol(spec, "count_tolerance_pct")
        pct = abs(n_s - n_t) / max(n_s, 1) * 100
        self.rec.rec(t, V, "delta_row_count", self._within(abs(n_s - n_t), pct, tol), n_s, n_t,
                     f"rows written by this run, diff {pct:.4f}% (tolerance {tol}%) | source: {sw} | target: {tw}")

    def check_incremental(self, spec: TableSpec, cols, non_keys):
        t, V, inc, keys = spec.name, "incremental", spec.incremental, spec.keys
        sw, tw = inc.source_delta, inc.target_delta
        n_s, n_t = self.src.count(spec.name, sw), self.tgt.count(spec.target_name, tw)
        if n_s == 0 and n_t == 0:
            self.rec.rec(t, V, "delta_row_count", "WARN", 0, 0, "both deltas are empty: check the delta conditions")
            self.rec.note(t, V, "both deltas empty: check the delta conditions / run window")
            return
        self._delta_count(t, V, spec, n_s, n_t, sw, tw)

        # delta keys: every key each run touched (key columns only)
        ks = self.src.read(spec.name, keys, sw, n_s).dropna().distinct()
        kt = self.tgt.read(spec.target_name, keys, tw, n_t).dropna().distinct()
        missed, extra = ks.join(kt, keys, "left_anti"), kt.join(ks, keys, "left_anti")
        n_missed, n_extra = missed.count(), extra.count()
        ktol = self._delta_tol(spec, "key_tolerance_pct")
        kpct = (n_missed + n_extra) / max(ks.count(), 1) * 100
        self.rec.rec(t, V, "delta_keys", self._within(n_missed + n_extra, kpct, ktol), n_missed, n_extra,
                     f"src_value = touched by the source run only, tgt_value = touched by the target run only; "
                     f"{kpct:.4f}% of the source delta keys (tolerance {ktol}%)")
        if n_missed:
            self.rec.mismatches(t, V, "delta_missing_in_target", missed.select(key_json(keys).alias("key")))
        if n_extra:
            self.rec.mismatches(t, V, "delta_extra_in_target", extra.select(key_json(keys).alias("key")))

        # delta values: rows both runs touched (sampled if the delta is big)
        rows = rows_to_compare(self.src, self.tgt, spec, cols, self.s, sw, tw, n_s, n_t, delta=True, cache=try_cache)
        if rows.notes:
            self.rec.rec(t, V, "delta_sampling", "INFO", detail=" | ".join(rows.notes), scope=rows.scope)
        src_df = try_cache(normalize(rows.src, spec.ts_truncate))
        tgt_df = try_cache(normalize(rows.tgt, spec.ts_truncate))
        try:
            r = compare_rows(src_df, tgt_df, keys, non_keys, spec.num_tolerance, compare_keys=False)
            ok = self._hash_checks(t, V, rows.scope, r, spec, kind="delta_value_mismatch", check="delta_values",
                                   tol=self._delta_tol(spec, "value_tolerance_pct"), warn_within=True)
            self._rows_compared(t, V, rows.scope, r.src_rows, r.tgt_rows, n_s, n_t, check="delta_rows_compared")
        finally:
            try_unpersist(src_df, tgt_df)
        note = (f"delta {n_s:,} vs {n_t:,}; keys: {n_missed:,} missed, {n_extra:,} extra; "
                f"values on {self._scope_text(rows.scope, r.src_rows, r.tgt_rows)}: ")
        if r.dup_excluded:
            note += f"{r.dup_excluded:,} duplicated delta key(s) left out; "
        if r.differing:
            note += f"{r.differing:,} row{'s differ' if r.differing != 1 else ' differs'}: {_diff_cols(r.per_column)}" + (" (within row tolerance)" if ok else "")
        else:
            note += "values match"
        self.rec.note(t, V, note)


    # ------------------------------------------------------------------ 3 + 4 without keys
    def _rowset_status(self, spec, r, tol: Optional[float] = None) -> tuple:
        """(status for a non-zero count, pct): FAIL, or WARN when the tolerance (default: row tolerance) covers it."""
        tol = self.row_tolerance(spec) if tol is None else tol
        pct = r.differing / max(r.src_rows, r.tgt_rows, 1) * 100
        return ("WARN" if r.differing and pct <= tol else "FAIL"), pct

    def _keyless_compare(self, spec: TableSpec, cols, src_where, tgt_where, n_src: int, n_tgt: int, delta: bool):
        """
        Whole-row comparison for a table (or delta) without keys.
          small, or keyless_method = sample : rows compared directly (all rows / row-hash bucket sample)
          big (default)                      : whole-table bucket fingerprints, then row detail for differing buckets
        Returns (RowSetResult, scope, mode_text, src_all, tgt_all, fingerprint_info or None).
        """
        big = not spec.force_full and max(n_src, n_tgt) > self.s.sample_threshold_rows
        base = "delta_" if delta else ""
        if not big or self.s.keyless_method == "sample":
            a, b, scope, notes = keyless_rows(self.src, self.tgt, spec, cols, self.s, src_where, tgt_where,
                                              n_src, n_tgt, delta=delta, cache=try_cache)
            mode = "no keys: rows matched by all compared columns" + (" | " + " | ".join(notes) if notes else "")
            return compare_rowsets(a, b, cols), scope, mode, a, b, None

        a, b, da, db, fp = keyless_fingerprint_rows(self.src, self.tgt, spec, cols, self.s, src_where, tgt_where,
                                                     n_src, n_tgt)
        if da is None:                                   # every bucket identical: nothing to detail
            r = RowSetResult(src_rows=n_src, tgt_rows=n_tgt, matched=min(n_src, n_tgt))
        else:
            da, db = try_cache(da), try_cache(db)
            r = compare_rowsets(da, db, cols)
        mode = (f"no keys: whole-table fingerprints of every row in {fp['buckets']:,} buckets: "
                f"{fp['used'] - fp['differing']:,} identical, {fp['differing']:,} differ; row detail from "
                f"{fp['detailed']:,} bucket(s), {fp['detail_rows']:,} rows" + ("" if fp["complete"] else
                f" (max_detail_rows={self.s.max_detail_rows:,}: row counts cover only those buckets)"))
        return r, base + "fingerprint", mode, a, b, fp

    def check_rows_keyless(self, spec: TableSpec, cols, n_src: int, n_tgt: int):
        """No keys: rows are matched by ALL compared columns (whole-row multiset comparison)."""
        t, V = spec.name, "row_hash"
        r, scope, mode, src_df, tgt_df, fp = self._keyless_compare(spec, cols, spec.filter, spec.filter,
                                                                     n_src, n_tgt, delta=False)
        self.rec.rec(t, V, "comparison_mode", "INFO", scope=scope, detail=mode)
        try:
            bad, pct = self._rowset_status(spec, r)
            st = lambda n: "PASS" if n == 0 else bad
            if fp:
                self.rec.rec(t, V, "bucket_fingerprints", "PASS" if not fp["differing"] else bad,
                             fp["differing"], fp["used"],
                             f"{fp['differing']:,} of {fp['used']:,} buckets differ (every row of the table checked)", scope)
            part = "" if not fp or fp["complete"] else f" (in {fp['detailed']} of {fp['differing']} differing buckets)"
            self.rec.rec(t, V, "rows_only_in_src", st(r.only_src), r.only_src, None,
                         f"{r.only_src:,} rows in source with no identical row in target{part}", scope)
            self.rec.rec(t, V, "rows_only_in_tgt", st(r.only_tgt), None, r.only_tgt,
                         f"{r.only_tgt:,} rows in target with no identical row in source{part}", scope)
            self.rec.rec(t, V, "row_copy_counts", st(r.copy_diff_rows), r.copy_src_rows, r.copy_tgt_rows,
                         f"{r.copy_diff_values:,} distinct row(s) on both sides but a different number of times: "
                         f"src_value = their copies in source ({r.copy_src_rows:,}), tgt_value = their copies in "
                         f"target ({r.copy_tgt_rows:,}), difference {r.copy_diff_rows:,} rows{part}", scope)
            if fp:
                self._rows_compared(t, V, scope, n_src, n_tgt, n_src, n_tgt,
                                    extra=f"; {fp['detail_rows']:,} rows of the differing buckets compared row by row")
            else:
                self._rows_compared(t, V, scope, r.src_rows, r.tgt_rows, n_src, n_tgt)
            self.rec.mismatches(t, V, "rows_only_in_src", r.only_src_df)
            self.rec.mismatches(t, V, "rows_only_in_tgt", r.only_tgt_df)
            self.rec.mismatches(t, V, "row_copy_count", r.copies_df)
            if fp:
                note = (f"no keys, all {n_src:,} vs {n_tgt:,} rows via fingerprints: "
                        f"{fp['differing']:,} of {fp['used']:,} buckets differ; ")
            else:
                note = f"no keys, {self._scope_text(scope, r.src_rows, r.tgt_rows)}: "
            if r.differing:
                note += (f"{r.only_src:,} only in source, {r.only_tgt:,} only in target, "
                         f"{r.copy_diff_values:,} with different copy counts ({r.copy_src_rows:,} vs "
                         f"{r.copy_tgt_rows:,} copies){part}")
                note += " (within row tolerance)" if bad == "WARN" else ""
            else:
                note += f"all {max(r.matched, 0):,} rows identical"
            self.rec.note(t, V, note)

            # 4. column profiles over all compared rows (no pairing possible without keys)
            diffs = compare_profiles(src_df, tgt_df, cols, spec.num_tolerance)
            bad_status = "WARN" if self.row_tolerance(spec) > 0 else "FAIL"
            for c, d in diffs.items():
                self.rec.rec(t, "column_profiles", f"profile:{c}", bad_status if d else "PASS",
                             detail="; ".join(d), scope=scope)
            bad_cols = {c: d for c, d in diffs.items() if d}
            self.rec.note(t, "column_profiles", "; ".join(f"{c} {d[0]}" + (" ..." if len(d) > 1 else "")
                                                          for c, d in list(bad_cols.items())[:4])
                          + (f"; +{len(bad_cols) - 4} more columns" if len(bad_cols) > 4 else ""))
            self._record_distributions(spec, scope, src_df, tgt_df, cols)
        finally:
            try_unpersist(src_df, tgt_df)

    def check_incremental_keyless(self, spec: TableSpec, cols):
        """No keys: delta row count, then the delta rows compared as whole rows."""
        t, V, inc = spec.name, "incremental", spec.incremental
        sw, tw = inc.source_delta, inc.target_delta
        n_s, n_t = self.src.count(spec.name, sw), self.tgt.count(spec.target_name, tw)
        if n_s == 0 and n_t == 0:
            self.rec.rec(t, V, "delta_row_count", "WARN", 0, 0, "both deltas are empty: check the delta conditions")
            self.rec.note(t, V, "both deltas empty: check the delta conditions / run window")
            return
        self._delta_count(t, V, spec, n_s, n_t, sw, tw)
        r, scope, mode, src_df, tgt_df, fp = self._keyless_compare(spec, cols, sw, tw, n_s, n_t, delta=True)
        if fp or scope.endswith("sample"):
            self.rec.rec(t, V, "delta_comparison_mode", "INFO", detail=mode, scope=scope)
        try:
            bad, pct = self._rowset_status(spec, r, self._delta_tol(spec, "value_tolerance_pct"))
            part = "" if not fp or fp["complete"] else f" (in {fp['detailed']} of {fp['differing']} differing buckets)"
            self.rec.rec(t, V, "delta_rows", "PASS" if not r.differing else bad, r.only_src, r.only_tgt,
                         f"rows written by one run with no identical row from the other: {r.only_src:,} source only, "
                         f"{r.only_tgt:,} target only, {r.copy_diff_values:,} with different copy counts{part}", scope)
            if fp:
                self._rows_compared(t, V, scope, n_s, n_t, n_s, n_t, check="delta_rows_compared",
                                    extra=f"; {fp['detail_rows']:,} rows of the differing buckets compared row by row")
            else:
                self._rows_compared(t, V, scope, r.src_rows, r.tgt_rows, n_s, n_t, check="delta_rows_compared")
            self.rec.mismatches(t, V, "delta_rows_only_in_src", r.only_src_df)
            self.rec.mismatches(t, V, "delta_rows_only_in_tgt", r.only_tgt_df)
        finally:
            try_unpersist(src_df, tgt_df)
        self.rec.note(t, V, f"delta {n_s:,} vs {n_t:,} (no keys); " +
                      ("rows identical" if not r.differing else
                       f"{r.only_src:,} rows only in source run, {r.only_tgt:,} only in target run{part}"))


# ====================================================================== public entry points
def run_parity(config: str, env: Optional[str] = None, tables: Optional[list] = None,
               params: Optional[dict] = None, raise_on_fail: bool = False,
               spark: Optional[SparkSession] = None, summary_style: str = "tree",
               parallel_tables: Optional[int] = None, email_to: Optional[str] = None,
               report_path: Optional[str] = None, send_email: bool = True, label: Optional[str] = None) -> ParityRun:
    """
    Validate the tables in `config` for environment `env`.

        run = run_parity("/Workspace/Shared/etl_parity/configs/tables.yaml", env="databricks",
                         tables=["FACT_SALES"], params={"SRC_BATCH_ID": "101"})
        display(run.summary)

    params fill ${NAME} placeholders in filters / delta conditions (environment variables work too).
    raise_on_fail=True raises an error when any check FAILs or ERRORs (useful in jobs).
    summary_style: "tree" (per table, nested; default) or "table" (one flat line per table per validation).
    parallel_tables: tables validated at the same time (default: settings.parallel_tables, 1 = one by one).
    email_to: e-mail the run report to these addresses (comma-separated); overrides settings.email.to.
    report_path: folder for the HTML report file; overrides settings.report_path.
    send_email: False = build the report but do not e-mail it (e.g. when send_report() mails one combined report
                for several runs afterwards).
    label: name of this run in reports (default: the YAML file name, e.g. icd_full).
    """
    started = time.time()
    spark = spark or SparkSession.getActiveSession() or SparkSession.builder.appName("etl_parity").getOrCreate()
    cfg = load_config(config, env)
    s = cfg.settings
    values = {**os.environ, **{k: str(v) for k, v in (params or {}).items()}}
    src = make_reader(spark, cfg.source, s.read_partitions)
    tgt = make_reader(spark, cfg.target, s.read_partitions)
    rec = Recorder(spark, s.results_table, s.mismatch_table, cfg.env, s.mismatch_sample_limit)
    only = {x.upper() for x in tables} if tables else None
    validator = TableValidator(src, tgt, s, rec)
    todo = [t for t in cfg.tables if not only or str(t.get("name", "?")).upper() in only]
    workers = max(1, min(parallel_tables or s.parallel_tables, len(todo) or 1))

    def one(tcfg):
        name, t0 = str(tcfg.get("name", "?")).upper(), time.time()
        print(f"validating {name} ...", flush=True)
        try:
            validator.run(build_spec(tcfg, values, cfg.env))
        except Exception as e:                     # one bad table must not stop the others
            msg = f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}"
            rec.rec(name, "run", "run", "ERROR", detail=msg)
            rec.note(name, "run", msg[:200])
        if workers > 1:
            print(f"  done {name} ({time.time() - t0:,.0f}s)", flush=True)

    if workers == 1:
        for tcfg in todo:
            one(tcfg)
    else:
        print(f"{len(todo)} tables, {workers} at a time", flush=True)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="parity") as pool:
            list(pool.map(one, todo))

    from .notify import _label
    rec.meta = dict(config=config, label=label or _label(config), duration=round(time.time() - started, 1))
    results = rec.save()
    rec.print_summary(summary_style)
    summary = rec.summary_df()
    failed = any(r["status"] in ("FAIL", "ERROR") for r in rec.summary_rows())
    run = ParityRun(rec.run_id, summary, rec.nested_df(), results, failed, label=rec.meta["label"],
                    results_table=s.results_table, mismatch_table=s.mismatch_table)
    _report(run, rec, s, email_to if send_email else None, report_path, send_email)
    if raise_on_fail and failed:
        raise RuntimeError(f"ETL parity run {rec.run_id} has failures; see {s.results_table}")
    return run


def _report(run: ParityRun, rec, s, email_to: Optional[str], report_path: Optional[str], send: bool = True):
    """HTML / text report on the ParityRun; optionally written to a file and e-mailed. Never fails the run."""
    from . import notify
    rep = notify.build_report([rec])
    run.report_html, run.report_text = notify.to_html(rep), notify.to_text(rep)
    run.report_file, run.email_sent, run.email_error = _deliver(
        rep, run.report_html, run.report_text, report_path or s.report_path, s.email if send else None, email_to)


def _deliver(rep, html_text, text, folder, email_cfg, email_to):
    """Write the report file and / or e-mail it. Returns (file, sent, error); never raises."""
    from . import notify
    path, sent, err = None, False, None
    if folder:
        try:
            path = notify.write_report(html_text, folder, rep["runs"][0]["run_id"] if len(rep["runs"]) == 1
                                       else "_".join(r["run_id"] for r in rep["runs"]))
            print(f"report: {path}")
        except Exception as e:
            print(f"report not written to {folder}: {type(e).__name__}: {e}")
    cfg = dict(email_cfg or {})
    if email_to:
        cfg["to"] = email_to
    if cfg.get("to") and notify.should_send(cfg, rep):
        try:
            notify.send_email(cfg, notify.subject(rep, cfg.get("subject_prefix", "[ETL parity]")), text, html_text)
            sent = True
            print(f"e-mail sent to {cfg['to']}")
        except Exception as e:                   # a mail problem must not hide the validation result
            err = f"{type(e).__name__}: {e}"
            print(f"e-mail NOT sent: {err}")
    return path, sent, err


@dataclass
class ParityReport:
    subject: str
    report_html: str
    report_text: str
    run_ids: list
    overall: str                        # PASS | FAIL | ERROR over all runs
    report_file: Optional[str] = None
    email_sent: bool = False
    email_error: Optional[str] = None


def send_report(runs, config: Optional[str] = None, email_to: Optional[str] = None,
                report_path: Optional[str] = None, send_on: Optional[str] = None,
                spark: Optional[SparkSession] = None) -> ParityReport:
    """
    ONE report (and e-mail) for one or several finished runs, e.g. a full-load YAML and an incremental YAML:

        full  = run_parity(full_yaml,  env="databricks", send_email=False)
        delta = run_parity(delta_yaml, env="databricks", send_email=False)
        send_report([full, delta], config=full_yaml)

    runs     ParityRun objects and / or run_id strings; None / "" entries are skipped (a run that did not happen),
             so the same call works when only one of the YAMLs was run.
    config   YAML whose settings give results_table / mismatch_table / email / report_path (needed for run_id strings;
             with ParityRun objects only for the e-mail settings). email_to / report_path / send_on override them.
    The runs are read back from the results table, so this also works in a later job task than the runs themselves.
    """
    from . import notify
    from .results import Recorder
    spark = spark or SparkSession.getActiveSession() or SparkSession.builder.appName("etl_parity").getOrCreate()
    s = load_config(config).settings if config else None
    items = [r for r in (runs if isinstance(runs, (list, tuple)) else [runs]) if r]
    if not items:
        raise ValueError("send_report: no runs given (all entries empty)")
    recs = []
    for r in items:
        run_id = r.run_id if isinstance(r, ParityRun) else str(r).strip()
        rt = (r.results_table if isinstance(r, ParityRun) else None) or (s.results_table if s else None)
        mt = (r.mismatch_table if isinstance(r, ParityRun) else None) or (s.mismatch_table if s else None)
        if not rt:
            raise ValueError(f"send_report: results table unknown for run {run_id}: pass config=<yaml>")
        recs.append(Recorder.load(spark, rt, mt or "-", run_id))
    rep = notify.build_report(recs)
    html_text, text = notify.to_html(rep), notify.to_text(rep)
    cfg = dict((s.email if s else None) or {})
    if send_on:
        cfg["send_on"] = send_on
    path, sent, err = _deliver(rep, html_text, text, report_path or (s.report_path if s else None), cfg, email_to)
    return ParityReport(notify.subject(rep, cfg.get("subject_prefix", "[ETL parity]")), html_text, text,
                        [r["run_id"] for r in rep["runs"]], rep["overall"], path, sent, err)


def main(argv=None):
    """Command line / job entry point: exit code 1 when any check FAILs or ERRORs."""
    ap = argparse.ArgumentParser(description="ETL migration parity validation")
    ap.add_argument("config", help="path to the YAML config")
    ap.add_argument("--env", help="environment from the config (default: default_env)")
    ap.add_argument("--tables", help="comma-separated subset of tables")
    ap.add_argument("--email-to", help="e-mail the run report to these addresses (comma-separated)")
    ap.add_argument("--parallel", type=int, help="tables validated at the same time (default: settings)")
    ap.add_argument("--param", action="append", default=[], metavar="NAME=VALUE",
                    help="value for a ${NAME} placeholder (repeatable)")
    a = ap.parse_args(argv)
    params = {}
    for p in a.param:
        k, sep, v = p.partition("=")
        if not sep:
            ap.error(f"--param must be NAME=VALUE, got {p!r}")
        params[k] = v
    run = run_parity(a.config, a.env, a.tables.split(",") if a.tables else None, params, parallel_tables=a.parallel,
                     email_to=a.email_to)
    raise SystemExit(1 if run.failed else 0)
