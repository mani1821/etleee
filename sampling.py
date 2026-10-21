"""
Which rows get compared.

    small table (<= sample_threshold_rows) or force_full  -> every row, both sides
    big table                                              -> sample-and-lookup, both directions:

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

    f_src, f_tgt = sample_fraction(n_src, s), sample_fraction(n_tgt, s)
    a = cache(src_reader.sample(spec.name, cols, src_where, n_src, f_src, s.sample_seed))
    b, note_b = tgt_reader.lookup(spec.target_name, cols, tgt_where, keys, a, n_tgt)
    c = cache(tgt_reader.sample(spec.target_name, cols, tgt_where, n_tgt, f_tgt, s.sample_seed + 1))  # own seed: independent of A
    c_new = c.join(a.select(*keys).distinct(), keys, "left_anti")      # target keys not already sampled
    d, note_d = src_reader.lookup(spec.name, cols, src_where, keys, c_new, n_src)
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
