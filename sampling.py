"""
Which rows get compared.

    small table (<= sample_threshold_rows) or force_full  -> every row, both sides
    big table, Databricks tables                           -> key-hash sample: the same keys on both sides
                                                              (hash of the normalised key < sample fraction)
    big table, Oracle                                      -> sample-and-lookup, both directions:

        1. sample the SOURCE (sample_pct %, at most max_sample_rows)   -> rows A
        2. fetch the same keys from the TARGET                          -> rows B
        3. sample the TARGET the same way                               -> rows C
        4. fetch C's keys (those not already in A) from the SOURCE      -> rows D
        compare  source = A + D   with   target = B + C(new keys)

    Keys sampled from the source find rows missing in the target (A key, no B row);
    keys sampled from the target find extra rows in the target (C key, no D row).
"""
from __future__ import annotations

import math

from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import DataFrame

from .config import Settings, TableSpec


@dataclass
class ComparedRows:
    src: DataFrame
    tgt: DataFrame
    scope: str                          # "full" | "sample" | "delta" | "delta_sample"
    notes: list = field(default_factory=list)


def sample_fraction(n_rows: int, s: Settings) -> float:
    """sample_pct of the rows, capped at max_sample_rows."""
    return min(s.sample_pct / 100.0, s.max_sample_rows / max(n_rows, 1))


def rows_to_compare(src_reader, tgt_reader, spec: TableSpec, cols: list[str], s: Settings,
                    src_where: Optional[str], tgt_where: Optional[str], n_src: int, n_tgt: int,
                    delta: bool = False, cache=lambda df: df) -> ComparedRows:
    keys = spec.keys
    if spec.force_full or max(n_src, n_tgt) <= s.sample_threshold_rows:
        return ComparedRows(src_reader.read(spec.name, cols, src_where, n_src),
                            tgt_reader.read(spec.target_name, cols, tgt_where, n_tgt),
                            "delta" if delta else "full")

    from .readers import TableReader
    if isinstance(src_reader, TableReader) and isinstance(tgt_reader, TableReader):
        # Databricks tables: the SAME keys on both sides, picked by a hash of the (normalised) key. Deterministic
        # (no random sample that could change between two reads, no cache needed), and no lookups: a key in the
        # sample of one side but not of the other is missing / extra on its own.
        from .checks import key_bucket
        f = min(s.sample_pct / 100.0, s.max_sample_rows / max(n_src, n_tgt, 1))
        cut = max(1, int(round(f * 1_000_000)))
        a = src_reader.read(spec.name, cols, src_where, n_src)
        b = tgt_reader.read(spec.target_name, cols, tgt_where, n_tgt)
        a = a.where(key_bucket(a, keys, spec.ts_truncate) < cut)
        b = b.where(key_bucket(b, keys, spec.ts_truncate) < cut)
        note = (f"key-hash sample: the same {cut / 10_000:g}% of keys on both sides (deterministic) "
                f"of {n_src:,} / {n_tgt:,} rows")
        return ComparedRows(a, b, "delta_sample" if delta else "sample", [note])

    f_src, f_tgt = sample_fraction(n_src, s), sample_fraction(n_tgt, s)
    ts = spec.ts_truncate
    # key-hash sampling: the same rows on every recomputation (see TableReader.sample)
    a = cache(src_reader.sample(spec.name, cols, src_where, n_src, f_src, s.sample_seed, keys=keys, ts_truncate=ts))
    b, note_b = tgt_reader.lookup(spec.target_name, cols, tgt_where, keys, a, n_tgt, ts_truncate=ts)
    c = cache(tgt_reader.sample(spec.target_name, cols, tgt_where, n_tgt, f_tgt, s.sample_seed + 1,  # own seed:
                                keys=keys, ts_truncate=ts))                                          # independent of A
    # target keys not already sampled - compared normalised, like the lookups and the row comparison
    from .checks import with_norm_keys
    cn, nk = with_norm_keys(c, keys, ts)
    an, _ = with_norm_keys(a.select(*keys), keys, ts)
    c_new = cn.join(an.select(*nk).distinct(), nk, "left_anti").select(*c.columns)
    d, note_d = src_reader.lookup(spec.name, cols, src_where, keys, c_new, n_src, ts_truncate=ts)
    notes = [f"source sample {f_src:.4%} of {n_src:,} rows, target lookup: {note_b}",
             f"target sample {f_tgt:.4%} of {n_tgt:,} rows, source lookup: {note_d}"]
    return ComparedRows(a.unionByName(d), b.unionByName(c_new),
                        "delta_sample" if delta else "sample", notes)


# ------------------------------------------------------------------------------- tables without keys
KEYLESS_BUCKETS = 1000


def keyless_rows(src_reader, tgt_reader, spec: TableSpec, cols: list[str], s: Settings,
                 src_where: Optional[str], tgt_where: Optional[str], n_src: int, n_tgt: int,
                 delta: bool = False, cache=lambda df: df):
    """
    Rows for a whole-row comparison (no keys), normalized.
    Small tables: every row. Big tables: the same row-hash buckets on both sides (identical rows always
    hash to the same bucket, so each kept bucket is compared exactly) -- sample_pct %, capped at max_sample_rows.
    Without keys there is nothing to look up, so big tables are read in full and filtered in Spark.
    Returns (src_df, tgt_df, scope, notes).
    """
    from .checks import normalize, row_hash
    from pyspark.sql import functions as F
    a = normalize(src_reader.read(spec.name, cols, src_where, n_src), spec.ts_truncate)
    b = normalize(tgt_reader.read(spec.target_name, cols, tgt_where, n_tgt), spec.ts_truncate)
    base = "delta_rows" if delta else "rows"
    if spec.force_full or max(n_src, n_tgt) <= s.sample_threshold_rows:
        return cache(a), cache(b), base, []
    keep = max(1, math.ceil(sample_fraction(max(n_src, n_tgt), s) * KEYLESS_BUCKETS))
    cond = F.pmod(F.xxhash64(row_hash(cols)), F.lit(KEYLESS_BUCKETS)) < keep
    note = (f"no keys: whole-row comparison on {keep}/{KEYLESS_BUCKETS} row-hash buckets "
            f"(~{100 * keep / KEYLESS_BUCKETS:g}% of rows) of {n_src:,} / {n_tgt:,} rows")
    return cache(a.where(cond)), cache(b.where(cond)), base + "_sample", [note]


def keyless_fingerprint_rows(src_reader, tgt_reader, spec: TableSpec, cols: list[str], s: Settings,
                             src_where: Optional[str], tgt_where: Optional[str], n_src: int, n_tgt: int):
    """
    Big tables without keys: compare whole-table bucket fingerprints (every row), then return only the rows
    of the differing buckets (at most max_detail_buckets) for the row-by-row comparison.
    Returns (src_all, tgt_all, src_detail, tgt_detail, info).
    """
    from .checks import bucket_fingerprints, differing_buckets, normalize, row_bucket
    a = normalize(src_reader.read(spec.name, cols, src_where, n_src), spec.ts_truncate)
    b = normalize(tgt_reader.read(spec.target_name, cols, tgt_where, n_tgt), spec.ts_truncate)
    B = s.fingerprint_buckets or min(100_000, max(1_000, max(n_src, n_tgt) // 2_000))
    fs, ft = bucket_fingerprints(a, cols, B), bucket_fingerprints(b, cols, B)
    diff = differing_buckets(fs, ft)
    detail, rows = [], 0                      # take differing buckets until max_detail_rows (per side)
    for x in diff:
        n = max(fs.get(x, (0, 0))[0], ft.get(x, (0, 0))[0])
        if detail and rows + n > s.max_detail_rows:
            break
        detail.append(x)
        rows += n
    info = dict(buckets=B, used=len(set(fs) | set(ft)), differing=len(diff), detailed=len(detail),
                detail_rows=rows, complete=len(detail) == len(diff))
    cond = row_bucket(cols, B).isin(detail) if detail else None
    return a, b, (a.where(cond) if detail else None), (b.where(cond) if detail else None), info
