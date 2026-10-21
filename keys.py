"""
Key discovery: find the column(s) that identify a row, for tables without declared keys.

Built for very large tables: brute-force COUNT(DISTINCT ...) per column combination is never run.
Per table:

  0. metadata       declared primary key (Unity Catalog), column names and types;
                    technical columns (surrogate keys, batch ids, load timestamps) are set aside
  1. file sample    profile ~1% of the table's FILES: drop columns with NULLs, constants, floats, long text
  2. HLL search     ONE pass over the full table, reading only the candidate columns, estimates the
                    distinct count of every single column, then pairs, then triples (HyperLogLog,
                    no shuffle). A combination whose distinct count ~= row count is a candidate key.
  3. exact verify   rows are split into hash buckets of the candidate key; all copies of a duplicated
                    key land in the same bucket, so checking some buckets is exact for those buckets
                    (all buckets for tables up to exact_below_rows)
  4. target match   source key values are looked up in the target table: a business key is found,
                    a surrogate key generated independently by each ETL is not
  5. output         one row per table with the proposed key, confidence and evidence, plus the YAML
                    entries for configs/tables.yaml -- to be reviewed before use

    from etl_parity.keys import discover_keys, to_yaml
    found = discover_keys(spark, [("cat.sch.fact_sales_src", "cat.sch.fact_sales_tgt")],
                          results_table="cat.sch.parity_key_candidates")
    print(to_yaml(found))
"""
from __future__ import annotations

import datetime as dt
import itertools
import random
import re
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ----------------------------------------------------------------------------- name rules
TECHNICAL = r"(^ETL_|_ETL_|_SK$|^SK_|BATCH|LOAD_(TS|DT|DATE|ID)|INSERT(ED)?_(TS|DT|DATE|BY)|UPDATE[D]?_(TS|DT|DATE|BY)|" \
            r"CREATE[D]?_(TS|DT|DATE|BY)|MODIFIED|AUDIT|_HASH$|ROW_?HASH|RECORD_SOURCE|^DW_|_DW$|INGEST|_RUN_ID$)"
KEY_LIKE = r"(_ID$|^ID$|_KEY$|_NBR$|_NUM$|_NO$|_CD$|_CODE$|_NUMBER$)"
DATE_LIKE = r"(_DT$|_DATE$|^DT$|_PERIOD$|_MONTH$|_YEAR$)"
DESCRIPTIVE = r"(NAME|DESC|DESCRIPTION|ADDR|ADDRESS|COMMENT|NOTE|TEXT|MSG|MESSAGE|TITLE|LABEL)"


@dataclass
class KeySettings:
    file_sample_pct: float = 1.0          # step 1: % of files profiled (at least min_sample_files)
    min_sample_files: int = 2
    max_candidates: int = 15              # columns kept for the combination search
    max_key_size: int = 3                 # largest combination tried
    pairs_extended: int = 20              # level 3 only extends the best N pairs
    hll_rsd: float = 0.01                 # HyperLogLog relative error (0.01 = +-1%)
    unique_ratio: float = 0.97            # distinct/rows needed to become a candidate (allows HLL error)
    max_text_len: int = 200               # longer strings are not key material
    exact_below_rows: int = 5_000_000     # tables up to this size are verified on every row
    total_buckets: int = 100              # step 3: hash buckets ...
    verify_buckets: int = 10              # ... of which this many are checked exactly (10%)
    full_verify: bool = False             # True = also run one exact check on all rows (one shuffle)
    match_sample: int = 1000              # step 4: source key values looked up in the target
    min_match_pct: float = 90.0           # below this the candidate is treated as a surrogate key
    max_verified: int = 3                 # candidates verified per table before giving up
    technical_pattern: str = TECHNICAL
    seed: int = 42


@dataclass
class KeyResult:
    source_table: str
    target_table: Optional[str]
    status: str                            # FOUND | NOT_FOUND | ERROR
    keys: list = field(default_factory=list)
    confidence: Optional[str] = None       # HIGH | MEDIUM | LOW
    rows: Optional[int] = None
    distinct_ratio: Optional[float] = None
    verify: Optional[str] = None
    match_pct: Optional[float] = None
    alternatives: list = field(default_factory=list)
    exclude_cols: list = field(default_factory=list)
    notes: list = field(default_factory=list)


def _m(pattern: str, name: str) -> bool:
    return re.search(pattern, name.upper()) is not None


def name_score(col: str) -> int:
    return 2 if _m(KEY_LIKE, col) else 1 if _m(DATE_LIKE, col) else 0


# ----------------------------------------------------------------------------- step 0: metadata
def declared_key(spark: SparkSession, table: str) -> list:
    """Primary key declared in Unity Catalog (informational constraint), if any."""
    parts = table.split(".")
    if len(parts) != 3:
        return []
    cat, sch, tbl = parts
    try:
        rows = spark.sql(f"""
            SELECT k.column_name FROM `{cat}`.information_schema.table_constraints c
            JOIN `{cat}`.information_schema.key_column_usage k
              ON k.constraint_catalog = c.constraint_catalog AND k.constraint_schema = c.constraint_schema
             AND k.constraint_name = c.constraint_name
            WHERE c.constraint_type = 'PRIMARY KEY' AND lower(c.table_schema) = '{sch.lower()}'
              AND lower(c.table_name) = '{tbl.lower()}'
            ORDER BY k.ordinal_position""").collect()
        return [r[0].upper() for r in rows]
    except Exception:
        return []


def _usable_type(dtype) -> bool:
    return isinstance(dtype, (T.StringType, T.IntegerType, T.LongType, T.ShortType, T.ByteType,
                              T.DecimalType, T.DateType, T.TimestampType, T.BooleanType))


# ----------------------------------------------------------------------------- step 1: file sample
def file_sample(df: DataFrame, s: KeySettings) -> DataFrame:
    """Rows from a random ~file_sample_pct % of the table's files (reads only those files)."""
    try:
        files = df.inputFiles()
    except Exception:
        files = []
    if len(files) > s.min_sample_files:
        k = max(s.min_sample_files, round(len(files) * s.file_sample_pct / 100))
        picked = random.Random(s.seed).sample(files, min(k, len(files)))
        names = [f.rstrip("/").split("/")[-1] for f in picked]     # file names: path spellings differ (file:/ vs file:///)
        try:
            sample = df.where(F.substring_index(F.col("_metadata.file_path"), "/", -1).isin(names))
            if sample.limit(1).count():
                return sample
        except Exception:
            pass
    return df.limit(1_000_000)          # few files (small table) or no file metadata: first rows only


def profile_sample(df: DataFrame, cols: list, s: KeySettings) -> dict:
    dtypes = {f.name: f.dataType for f in df.schema.fields}
    exprs = [F.count(F.lit(1)).alias("_rows")]
    for c in cols:
        exprs += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}|nulls"),
                  F.approx_count_distinct(c, s.hll_rsd).alias(f"{c}|distinct")]
        if isinstance(dtypes[c], T.StringType):
            exprs.append(F.max(F.length(c)).alias(f"{c}|max_len"))
    r = df.agg(*exprs).first().asDict()
    return {"rows": r["_rows"], **{c: {"nulls": r[f"{c}|nulls"] or 0, "distinct": r[f"{c}|distinct"] or 0,
                                         "max_len": r.get(f"{c}|max_len")} for c in cols}}


# ----------------------------------------------------------------------------- step 2: HLL search
def _combo_hash(cols):
    return F.xxhash64(*[F.col(c) for c in cols])


def hll_distinct(df: DataFrame, combos: list, s: KeySettings, with_nulls: list = ()) -> tuple:
    """One pass: row count, exact NULL counts for with_nulls, approx distinct count of every combo."""
    exprs = [F.count(F.lit(1)).alias("_rows")]
    exprs += [F.sum(F.col(c).isNull().cast("int")).alias(f"nulls|{c}") for c in with_nulls]
    exprs += [F.approx_count_distinct(_combo_hash(cb) if len(cb) > 1 else F.col(cb[0]), s.hll_rsd).alias(f"d|{i}")
              for i, cb in enumerate(combos)]
    r = df.agg(*exprs).first().asDict()
    return r["_rows"], {c: r[f"nulls|{c}"] or 0 for c in with_nulls}, \
        {tuple(cb): (r[f"d|{i}"] or 0) for i, cb in enumerate(combos)}


def search_keys(df: DataFrame, cands: list, s: KeySettings, position: dict, notes: list) -> tuple:
    """Level-wise: singles, then pairs, then triples (extending the best pairs). Returns (rows, nulls, ranked)."""
    singles = [(c,) for c in cands]
    rows, nulls, est = hll_distinct(df, singles, s, with_nulls=cands)
    notes.append(f"pass 1: {len(singles)} columns over {rows:,} rows")
    non_null = [c for c in cands if nulls[c] == 0]
    if len(non_null) < len(cands):
        notes.append(f"dropped (NULLs in full table): {[c for c in cands if nulls[c]]}")
    est = {k: v for k, v in est.items() if all(c in non_null for c in k)}
    ratio = lambda k: est[k] / max(rows, 1)
    found = [k for k in est if ratio(k) >= s.unique_ratio]

    level, prev = 1, sorted(est, key=ratio, reverse=True)
    while not found and level < s.max_key_size and len(non_null) > level:
        level += 1
        if level == 2:
            combos = list(itertools.combinations(sorted(non_null, key=position.get), 2))
        else:
            base = prev[:s.pairs_extended]
            combos = sorted({tuple(sorted(set(b) | {c}, key=position.get)) for b in base for c in non_null if c not in b})
        if not combos:
            break
        _, _, e = hll_distinct(df, combos, s)
        est.update(e)
        notes.append(f"pass {level}: {len(combos)} combinations of {level} columns")
        found = [k for k in e if ratio(k) >= s.unique_ratio]
        prev = sorted(e, key=ratio, reverse=True)

    # rank: fewest columns, most key-like names, earliest columns in the table. The ratio is NOT used
    # to rank: above unique_ratio its differences are HyperLogLog noise (verification decides).
    ranked = sorted(found, key=lambda k: (len(k), -sum(name_score(c) for c in k), [position[c] for c in k]))
    # minimal keys only: drop combinations that contain a smaller candidate
    minimal = [k for k in ranked if not any(set(o) < set(k) for o in ranked)]
    return rows, nulls, [(list(k), ratio(k)) for k in minimal]


# ----------------------------------------------------------------------------- step 3: exact verify
def verify_unique(df: DataFrame, keys: list, rows: int, s: KeySettings) -> tuple:
    """(ok, text). Exact on every row for small tables, else on verify_buckets of total_buckets hash buckets."""
    if rows <= s.exact_below_rows or s.full_verify:
        dups = df.groupBy(*keys).count().where("count > 1").limit(1000).count()
        return dups == 0, f"exact on all {rows:,} rows: {'no' if not dups else dups} duplicate key value(s)"
    sub = df.select(*keys).where(F.pmod(_combo_hash(keys), F.lit(s.total_buckets)) < s.verify_buckets)
    dups = sub.groupBy(*keys).count().where("count > 1").limit(1000).count()
    pct = 100 * s.verify_buckets / s.total_buckets
    return dups == 0, (f"exact on {s.verify_buckets}/{s.total_buckets} hash buckets (~{pct:g}% of rows): "
                       f"{'no' if not dups else dups} duplicate key value(s)")


# ----------------------------------------------------------------------------- step 4: target match
def match_pct(src: DataFrame, tgt: DataFrame, keys: list, s: KeySettings) -> Optional[float]:
    """% of sampled source key values that also exist in the target."""
    tcols = {c.upper(): c for c in tgt.columns}
    if any(k not in tcols for k in keys):
        return 0.0
    probe = (src.select(*keys).where(F.pmod(_combo_hash(keys), F.lit(1000)) == 7)    # pseudo-random slice
             .dropDuplicates().limit(s.match_sample))
    probe = probe if probe.count() >= 50 else src.select(*keys).dropDuplicates().limit(s.match_sample)
    n = probe.count()
    if n == 0:
        return None
    t = tgt.select(*[F.col(tcols[k]).alias(k) for k in keys])
    hit = t.join(F.broadcast(probe), keys, "left_semi").dropDuplicates().count()   # probe keys found in target
    return 100.0 * hit / n


# ----------------------------------------------------------------------------- per table
def _upper(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.upper() for c in df.columns])


def discover_table(spark: SparkSession, source: str, target: Optional[str], s: KeySettings) -> KeyResult:
    res = KeyResult(source, target, "NOT_FOUND")
    src = _upper(spark.table(source))
    tgt = _upper(spark.table(target)) if target else None
    fields = {f.name: f.dataType for f in src.schema.fields}
    position = {c: i for i, c in enumerate(src.columns)}

    # 0. metadata
    res.exclude_cols = [c for c in src.columns if _m(s.technical_pattern, c)]
    declared = declared_key(spark, source)
    if declared:
        res.notes.append(f"declared primary key: {declared}")

    usable = [c for c in src.columns if c not in res.exclude_cols and _usable_type(fields[c])]
    skipped_type = [c for c in src.columns if c not in res.exclude_cols and not _usable_type(fields[c])]
    if skipped_type:
        res.notes.append(f"not key material (type): {skipped_type}")

    # 1. file sample profile
    prof = profile_sample(file_sample(src, s), usable, s)
    n = max(prof["rows"], 1)
    pool = []
    for c in usable:
        p = prof[c]
        if p["nulls"] or p["distinct"] <= 1:
            continue
        if p["max_len"] is not None and p["max_len"] > s.max_text_len:
            continue
        pool.append(c)
    descriptive = [c for c in pool if _m(DESCRIPTIVE, c) and c not in declared]
    rank = lambda c: (-(c in declared), -name_score(c), -prof[c]["distinct"] / n, position[c])
    primary = sorted([c for c in pool if c not in descriptive], key=rank)[:s.max_candidates]
    res.notes.append(f"file sample: {prof['rows']:,} rows; candidate columns: {primary}"
                     + (f"; held back (descriptive): {descriptive}" if descriptive else ""))

    # 2-4. a declared key is tried first; otherwise search, then verify and match the best candidates
    def accept(keys, ratio, rows, alternatives, extra_note=None) -> bool:
        ok, text = verify_unique(src, keys, rows, s)
        if not ok:
            res.notes.append(f"{keys} rejected: {text}")
            return False
        mp = match_pct(src, tgt, keys, s) if tgt is not None else None
        if mp is not None and mp < s.min_match_pct:
            res.notes.append(f"{keys} rejected: only {mp:.1f}% of its values exist in the target "
                             f"(looks like a surrogate / ETL-generated key)")
            return False
        res.status, res.keys, res.distinct_ratio, res.verify, res.match_pct = "FOUND", keys, ratio, text, mp
        res.rows, res.alternatives = rows, [k for k in alternatives if k != keys][:5]
        exact = text.startswith("exact on all")
        key_like = bool(declared) or any(name_score(c) for c in keys)
        good_match = mp is None or mp >= 98
        res.confidence = "HIGH" if exact and key_like and good_match else "MEDIUM" if (key_like or exact) else "LOW"
        if mp is None:
            res.notes.append("no target table given: surrogate-key check skipped")
        if extra_note:
            res.confidence = "LOW"
            res.notes.append(extra_note)
        return True

    if declared:
        rows = src.count()
        if accept(declared, None, rows, []):
            return res
        res.notes.append("declared primary key failed verification; searching")
        declared = []

    attempts = [(primary, None)]
    if descriptive:
        attempts.append((sorted(primary + descriptive, key=rank)[:s.max_candidates],
                         "key uses descriptive column(s) (names, descriptions): review carefully"))
    for cands, warn in attempts:
        rows, _, ranked = search_keys(src, cands, s, position, res.notes)
        res.rows = rows
        keys_only = [k for k, _ in ranked]
        for keys, ratio in ranked[:s.max_verified]:
            if accept(keys, ratio, rows, keys_only, warn if any(c in descriptive for c in keys) else None):
                return res
    res.notes.append("no unique, non-null column combination found: the table may contain duplicate rows")
    return res


# ----------------------------------------------------------------------------- public API
TableList = Union[list, DataFrame]


def _pairs(tables: TableList, target_for: Optional[Callable[[str], str]]) -> list:
    if isinstance(tables, DataFrame):
        rows = tables.collect()
        tables = [(r[0], r[1] if len(r) > 1 else None) for r in rows]
    out = []
    for t in tables:
        src, tgt = (t if isinstance(t, (tuple, list)) else (t, None))
        if tgt is None and target_for:
            tgt = target_for(src)
        out.append((src, tgt))
    return out


RESULT_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("run_ts", T.TimestampType()), ("source_table", T.StringType()),
    ("target_table", T.StringType()), ("status", T.StringType()), ("keys", T.ArrayType(T.StringType())),
    ("confidence", T.StringType()), ("rows", T.LongType()), ("distinct_ratio", T.DoubleType()),
    ("verify", T.StringType()), ("target_match_pct", T.DoubleType()),
    ("alternatives", T.ArrayType(T.ArrayType(T.StringType()))), ("exclude_cols", T.ArrayType(T.StringType())),
    ("notes", T.ArrayType(T.StringType()))]])


def discover_keys(spark: SparkSession, tables: TableList, target_for: Optional[Callable[[str], str]] = None,
                  settings: Optional[KeySettings] = None, results_table: Optional[str] = None) -> list:
    """
    tables       list of source table names, list of (source, target) pairs, or a DataFrame whose first
                 column is the source table and optional second column the target table
                 (e.g. spark.sql("<your query listing the full-load tables>"))
    target_for   derive the target name when only sources are given, e.g. lambda t: t.replace("_src", "_tgt")
    results_table  if given, one row per table is appended there (created on first run)
    Returns a list of KeyResult.
    """
    s = settings or KeySettings()
    run_id, run_ts = uuid.uuid4().hex[:8], dt.datetime.now().replace(microsecond=0)
    results = []
    for src, tgt in _pairs(tables, target_for):
        print(f"discovering keys: {src} ...", flush=True)
        try:
            r = discover_table(spark, src, tgt, s)
        except Exception as e:
            r = KeyResult(src, tgt, "ERROR", notes=[f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}"])
        results.append(r)
        print(f"  {r.status} {r.keys or ''} {r.confidence or ''}", flush=True)
    if results_table:
        rows = [(run_id, run_ts, r.source_table, r.target_table, r.status, r.keys, r.confidence, r.rows,
                 r.distinct_ratio, r.verify, r.match_pct, r.alternatives, r.exclude_cols, r.notes) for r in results]
        spark.createDataFrame(rows, RESULT_SCHEMA).write.mode("append").option("mergeSchema", "true") \
            .saveAsTable(results_table)
    return results


def results_df(spark: SparkSession, results: list) -> DataFrame:
    rows = [(None, None, r.source_table, r.target_table, r.status, r.keys, r.confidence, r.rows,
             r.distinct_ratio, r.verify, r.match_pct, r.alternatives, r.exclude_cols, r.notes) for r in results]
    return spark.createDataFrame(rows, RESULT_SCHEMA).drop("run_id", "run_ts")


def to_yaml(results: list, name_for: Callable[[str], str] = lambda t: t.split(".")[-1].upper(),
            min_confidence: str = "LOW") -> str:
    """tables: entries for configs/tables.yaml (FOUND keys at or above min_confidence). Review before use."""
    order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
    lines = ["tables:"]
    for r in results:
        if r.status == "NOT_FOUND":            # no grain: the framework compares whole rows
            lines.append(f"  - name: {name_for(r.source_table)}")
            lines.append("    keys: []          # no key found: whole-row comparison (review)")
            if r.exclude_cols:
                lines.append(f"    exclude_cols: [{', '.join(r.exclude_cols)}]")
            continue
        if r.status != "FOUND" or order[r.confidence] < order[min_confidence]:
            lines.append(f"  # {name_for(r.source_table)}: {r.status} - review manually")
            continue
        lines.append(f"  - name: {name_for(r.source_table)}")
        if r.target_table and name_for(r.target_table) != name_for(r.source_table):
            lines.append(f"    target_name: {name_for(r.target_table)}")
        lines.append(f"    keys: [{', '.join(r.keys)}]          # confidence {r.confidence}"
                     + (f", target match {r.match_pct:.1f}%" if r.match_pct is not None else ""))
        if r.exclude_cols:
            lines.append(f"    exclude_cols: [{', '.join(r.exclude_cols)}]")
    return "\n".join(lines)
