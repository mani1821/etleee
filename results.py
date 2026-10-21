"""
Results: every check is recorded here, written to the results table, and summarised
per table per validation.

    <results_table>    one row per check   (run_id, table_name, validation, check_name, status, ...)
    <mismatch_table>   example bad rows    (run_id, table_name, kind, key JSON, column, src_value, tgt_value)
Both are created on the first run (in an existing catalog/schema) and appended to afterwards.
"""
from __future__ import annotations

import datetime as dt
import json
import threading
import uuid
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

VALIDATION_ORDER = ["run", "schema", "row_count", "row_hash", "column_profiles", "column_distributions", "incremental"]
SEVERITY = {"PASS": 0, "SKIP": 1, "WARN": 2, "FAIL": 3, "ERROR": 4}       # INFO rows are not counted

RESULT_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("run_ts", T.TimestampType()), ("env", T.StringType()),
    ("table_name", T.StringType()), ("validation", T.StringType()), ("scope", T.StringType()),
    ("check_name", T.StringType()), ("status", T.StringType()), ("src_value", T.StringType()),
    ("tgt_value", T.StringType()), ("detail", T.StringType())]])

RUN_ROW = "__run__"           # table_name of the run_info row in the results table

MISMATCH_SCHEMA = T.StructType([T.StructField("run_id", T.StringType()), T.StructField("run_ts", T.TimestampType())] +
                               [T.StructField(n, T.StringType()) for n in
                                ("env", "table_name", "validation", "kind", "key", "column", "src_value", "tgt_value")])
SUMMARY_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("table_name", T.StringType()), ("validation", T.StringType()),
    ("status", T.StringType()), ("checks", T.IntegerType()), ("pass", T.IntegerType()),
    ("fail", T.IntegerType()), ("warn", T.IntegerType()), ("skip", T.IntegerType()),
    ("error", T.IntegerType()), ("details", T.StringType())]])


def _score(v: dict) -> str:
    """'3/5 passed' plus the checks within tolerance / failed / skipped, e.g. '3/5 passed, 1 warn, 1 fail'."""
    extra = [f"{v[k]} {k}" for k in ("warn", "fail", "skip", "error") if v[k]]
    return f"{v['pass']}/{v['checks']} passed" + (", " + ", ".join(extra) if extra else "")


class Recorder:
    def __init__(self, spark: SparkSession, results_table: str, mismatch_table: str,
                 env: Optional[str], mismatch_limit: int):
        self.spark, self.results_table, self.mismatch_table = spark, results_table, mismatch_table
        self.env, self.limit = env, mismatch_limit
        self.run_id = uuid.uuid4().hex[:8]
        self.run_ts = dt.datetime.now().replace(microsecond=0)
        self.rows: list[tuple] = []
        self.notes: dict[tuple, str] = {}        # (table, validation) -> one-line summary text
        self.scopes: dict[tuple, str] = {}       # (table, validation) -> full / sample / ...
        self.mismatch_rows: list[tuple] = []     # example bad rows, written in save()
        self._lock = threading.Lock()            # tables may be validated in parallel threads
        self.meta: dict = {}                     # config, label, duration: saved as a run_info row

    # ---- recording
    def rec(self, table: str, validation: str, check: str, status: str, src=None, tgt=None,
            detail: str = "", scope: Optional[str] = None):
        row = (self.run_id, self.run_ts, self.env, table, validation, scope, check, status,
               None if src is None else str(src), None if tgt is None else str(tgt), (detail or "")[:4000])
        with self._lock:
            self.rows.append(row)
            if scope and validation == "row_hash":
                self.scopes[(table, validation)] = scope

    def note(self, table: str, validation: str, text: str):
        with self._lock:
            self.notes[(table, validation)] = text

    def mismatches(self, table: str, validation: str, kind: str, df: Optional[DataFrame]):
        """Keep up to `limit` example rows (df: key[, column, src_value, tgt_value], key = JSON string).
        The rows are computed now (in the table's own thread) and written once, with the results, in save():
        one write per run, and no two threads appending to the same table."""
        if df is None:
            return
        for c in ("column", "src_value", "tgt_value"):
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))
        got = df.select(F.col("key").cast("string"), F.col("column").cast("string"),
                        F.col("src_value").cast("string"), F.col("tgt_value").cast("string")).limit(self.limit).collect()
        rows = [(self.run_id, self.run_ts, self.env, table, validation, kind, r[0], r[1], r[2], r[3]) for r in got]
        with self._lock:
            self.mismatch_rows.extend(rows)

    # ---- saving
    def save(self) -> DataFrame:
        if self.mismatch_rows:
            (self.spark.createDataFrame(self.mismatch_rows, MISMATCH_SCHEMA)
             .write.mode("append").option("mergeSchema", "true").saveAsTable(self.mismatch_table))
        # besides the checks, keep what a report needs later (send_report() rebuilds it from this table):
        # one "summary" INFO row per table per validation, and one run_info row for the run itself
        extra = [(self.run_id, self.run_ts, self.env, t, v, self.scopes.get((t, v)), "summary", "INFO", None, None,
                  (text or "")[:4000]) for (t, v), text in self.notes.items()]
        extra.append((self.run_id, self.run_ts, self.env, RUN_ROW, "run_info", None, "run_info", "INFO", None, None,
                      json.dumps(self.meta, default=str)[:4000]))
        df = self.spark.createDataFrame(self.rows, RESULT_SCHEMA)
        self.spark.createDataFrame(self.rows + extra, RESULT_SCHEMA) \
            .write.mode("append").option("mergeSchema", "true").saveAsTable(self.results_table)
        return df

    @classmethod
    def load(cls, spark: SparkSession, results_table: str, mismatch_table: str, run_id: str) -> "Recorder":
        """A finished run, rebuilt from the results table (for reports made after the run, e.g. in a later job task)."""
        rows = [tuple(r) for r in spark.table(results_table).where(F.col("run_id") == run_id)
                .select(*[f.name for f in RESULT_SCHEMA.fields]).collect()]
        if not rows:
            raise ValueError(f"run_id {run_id!r} not found in {results_table}")
        rec = cls(spark, results_table, mismatch_table, rows[0][2], 0)
        rec.run_id, rec.run_ts = run_id, rows[0][1]
        for r in rows:
            if r[3] == RUN_ROW:
                try:
                    rec.meta = json.loads(r[10] or "{}")
                except ValueError:
                    rec.meta = {}
            elif r[6] == "summary" and r[7] == "INFO":
                rec.notes[(r[3], r[4])] = r[10] or ""
            else:
                rec.rows.append(r)
                if r[5] and r[4] == "row_hash":
                    rec.scopes[(r[3], r[4])] = r[5]
        return rec

    # ---- summary: one row per table per validation
    def summary_rows(self) -> list[dict]:
        groups: dict[tuple, list[str]] = {}
        for r in self.rows:
            if r[7] == "INFO":
                continue
            groups.setdefault((r[3], r[4]), []).append(r[7])
        out = []
        order = {v: i for i, v in enumerate(VALIDATION_ORDER)}
        for (table, val), statuses in sorted(groups.items(), key=lambda kv: (kv[0][0], order.get(kv[0][1], 99))):
            scope = self.scopes.get((table, val))
            out.append(dict(
                run_id=self.run_id, table_name=table,
                validation=f"{val} [{scope}]" if scope else val,
                status=max(statuses, key=lambda s: SEVERITY.get(s, 0)), checks=len(statuses),
                **{k: statuses.count(k.upper()) for k in ("pass", "fail", "warn", "skip", "error")},
                details=self.notes.get((table, val), "")))
        return out

    def summary_df(self) -> DataFrame:
        rows = self.summary_rows()
        return self.spark.createDataFrame([tuple(r[f.name] for f in SUMMARY_SCHEMA) for r in rows], SUMMARY_SCHEMA)

    def print_table(self):
        """Flat summary: one line per table per validation."""
        rows = self.summary_rows()
        cols = ["table_name", "validation", "status", "checks", "pass", "fail", "warn", "skip", "error", "details"]
        heads = ["table", "validation", "status", "checks", "pass", "fail", "warn", "skip", "error", "details"]
        widths = [max([len(h)] + [len(str(r[c])) for r in rows]) for c, h in zip(cols, heads)]
        widths[-1] = len("details")
        right = {"checks", "pass", "fail", "warn", "skip", "error"}
        fmt = lambda vals: "  ".join(str(v).rjust(w) if c in right else str(v).ljust(w)
                                     for v, w, c in zip(vals, widths, cols)).rstrip()
        print(f"\nETL parity results   run_id={self.run_id}   env={self.env or '-'}   {self.run_ts}\n")
        print(fmt(heads))
        print(fmt(["-" * w for w in widths]))
        for r in rows:
            print(fmt([r[c] for c in cols]))
        tables = {}
        for r in rows:
            tables[r["table_name"]] = max(tables.get(r["table_name"], 0), SEVERITY[r["status"]])
        bad = sum(1 for v in tables.values() if v >= SEVERITY["FAIL"])
        by_status = {s: sum(1 for r in rows if r["status"] == s) for s in SEVERITY}
        print(f"\n{len(tables)} tables: {len(tables) - bad} passed, {bad} failed   |   {len(rows)} validations: "
              + ", ".join(f"{n} {s}" for s, n in by_status.items() if n))
        print(f"details: {self.results_table} / {self.mismatch_table}  (run_id = '{self.run_id}')\n")

    # ---- nested summary: table -> validations -> checks that did not pass
    ICON = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "SKIP": "-", "ERROR": "E"}

    def _issues(self, table: str, validation: str) -> list[dict]:
        return [dict(check=r[6], status=r[7], src_value=r[8], tgt_value=r[9], detail=r[10])
                for r in self.rows if r[3] == table and r[4] == validation and r[7] not in ("PASS", "INFO")]

    def nested_rows(self) -> list[dict]:
        tables: dict[str, list[dict]] = {}
        for v in self.summary_rows():
            base = v["validation"].split(" [")[0]
            tables.setdefault(v["table_name"], []).append({**v, "issues": self._issues(v["table_name"], base)})
        out = []
        for t, vals in tables.items():
            status = max((v["status"] for v in vals), key=lambda st: SEVERITY.get(st, 0))
            bad = sum(1 for v in vals if SEVERITY[v["status"]] >= SEVERITY["FAIL"])
            out.append(dict(table_name=t, status=status, validations_failed=bad, validations=len(vals), details=vals))
        return out

    def print_tree(self, max_issues: int = 8):
        """Nested summary: one block per table, its validations, and under each the checks that did not pass."""
        rows = self.nested_rows()
        vw = max([len(v["validation"]) for t in rows for v in t["details"]] + [10])
        sw = max([len(_score(v)) for t in rows for v in t["details"]] + [12])
        print(f"\nETL parity results   run_id={self.run_id}   env={self.env or '-'}   {self.run_ts}\n")
        for t in rows:
            head = "all validations passed" if t["status"] == "PASS" else \
                f"{t['validations_failed']} of {t['validations']} validations failed"
            print(f"{self.ICON.get(t['status'], '?')} {t['table_name']}   {t['status']}   ({head})")
            vals = t["details"]
            for i, v in enumerate(vals):
                last_v = i == len(vals) - 1
                branch, cont = ("└─", "   ") if last_v else ("├─", "│  ")
                score = _score(v)
                print(f"   {branch} {self.ICON.get(v['status'], '?')} {v['validation'].ljust(vw)}  {v['status']:<5}  "
                      f"{score:<{sw}}  {v['details']}".rstrip())
                issues = v["issues"]
                for j, it in enumerate(issues[:max_issues]):
                    leaf = "└─" if j == min(len(issues), max_issues) - 1 and len(issues) <= max_issues else "├─"
                    vals_txt = "" if it["src_value"] is None and it["tgt_value"] is None else \
                        f" [src={it['src_value'] if it['src_value'] is not None else '-'}, " \
                        f"tgt={it['tgt_value'] if it['tgt_value'] is not None else '-'}]"
                    print(f"   {cont}   {leaf} {self.ICON.get(it['status'], '?')} {it['check']}{vals_txt}: {it['detail']}"[:220])
                if len(issues) > max_issues:
                    print(f"   {cont}   └─ ... {len(issues) - max_issues} more")
            print()
        bad = sum(1 for t in rows if SEVERITY[t["status"]] >= SEVERITY["FAIL"])
        print(f"{len(rows)} tables: {len(rows) - bad} passed, {bad} failed")
        print(f"details: {self.results_table} / {self.mismatch_table}  (run_id = '{self.run_id}')\n")

    def print_summary(self, style: str = "tree"):
        self.print_tree() if style == "tree" else self.print_table()

    def nested_df(self) -> DataFrame:
        """One row per table; validations (and their failed checks) as nested, expandable columns."""
        issue = T.StructType([T.StructField(n, T.StringType()) for n in ("check", "status", "src_value", "tgt_value", "detail")])
        val = T.StructType([T.StructField("validation", T.StringType()), T.StructField("status", T.StringType()),
                            *[T.StructField(n, T.IntegerType()) for n in ("checks", "pass", "fail", "warn", "skip", "error")],
                            T.StructField("details", T.StringType()), T.StructField("issues", T.ArrayType(issue))])
        schema = T.StructType([T.StructField("run_id", T.StringType()), T.StructField("table_name", T.StringType()),
                               T.StructField("status", T.StringType()), T.StructField("validations_failed", T.IntegerType()),
                               T.StructField("validations", T.ArrayType(val))])
        data = [(self.run_id, t["table_name"], t["status"], t["validations_failed"],
                 [tuple(v[f.name] for f in val.fields[:-1]) + ([tuple(i[f.name] for f in issue.fields) for i in v["issues"]],)
                  for v in t["details"]]) for t in self.nested_rows()]
        return self.spark.createDataFrame(data, schema)
