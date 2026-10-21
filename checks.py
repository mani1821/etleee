"""
The comparisons themselves. Pure Spark: every function takes DataFrames and returns plain results;
recording and writing them is done by the runner.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

NULL_TOKEN = "<NULL>"
SEP = "\u0001"


# --------------------------------------------------------------------------- helpers
_NTZ = getattr(T, "TimestampNTZType", None)          # Spark 3.4+

def normalize(df: DataFrame, ts_truncate: Optional[str] = None) -> DataFrame:
    """Remove representation differences that are not data differences."""
    out = []
    for f in df.schema.fields:
        c = F.col(f.name)
        if isinstance(f.dataType, T.StringType):
            c = F.when(F.trim(c) == "", F.lit(None)).otherwise(F.trim(c))        # 'A ' = 'A', '' = NULL
        elif isinstance(f.dataType, T.DecimalType):
            c = c.cast(T.DecimalType(38, 10))                                     # NUMBER(10,2) vs NUMBER
        elif isinstance(f.dataType, (T.DoubleType, T.FloatType)):
            c = F.round(c.cast("double"), 8)
        elif isinstance(f.dataType, T.TimestampType) and ts_truncate:
            c = F.date_trunc(ts_truncate, c)
        elif _NTZ and isinstance(f.dataType, _NTZ) and ts_truncate:            # TIMESTAMP_NTZ: same, keep the type
            c = F.date_trunc(ts_truncate, c).cast(_NTZ())
        out.append(c.alias(f.name))
    return df.select(*out)


def row_hash(cols: list[str]):
    return F.sha2(F.concat_ws(SEP, *[F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in cols]), 256)


def is_numeric(dtype) -> bool:
    return isinstance(dtype, (T.DecimalType, T.DoubleType, T.FloatType, T.IntegerType, T.LongType,
                              T.ShortType, T.ByteType))


def try_cache(df: DataFrame) -> DataFrame:
    """Cache where allowed; Databricks serverless rejects caching, so there the DataFrame is used as is."""
    try:
        return df.cache()
    except Exception:
        return df


def try_unpersist(*dfs: DataFrame):
    for df in dfs:
        try:
            df.unpersist()
        except Exception:
            pass


def key_json(keys: list[str]):
    return F.to_json(F.struct(*[F.col(k) for k in keys]))


# --------------------------------------------------------------------------- 1. schema
@dataclass
class SchemaResult:
    only_src: list
    only_tgt: list
    type_diffs: list
    nullable_diffs: list
    n_src: int
    n_tgt: int


def compare_schema(src_cols, tgt_cols, exclude: set) -> SchemaResult:
    s = {c.name: c for c in src_cols}
    g = {c.name: c for c in tgt_cols}
    both = [n for n in s if n in g and n not in exclude]
    return SchemaResult(
        only_src=sorted(set(s) - set(g) - exclude),
        only_tgt=sorted(set(g) - set(s) - exclude),
        type_diffs=[f"{n} {s[n].type} vs {g[n].type}" for n in both if s[n].type.lower() != g[n].type.lower()],
        nullable_diffs=[n for n in both if s[n].nullable != g[n].nullable],
        n_src=len([n for n in s if n not in exclude]), n_tgt=len([n for n in g if n not in exclude]))


# --------------------------------------------------------------------------- 3. rows: keys + hash + columns
@dataclass
class RowResult:
    src_rows: int = 0
    tgt_rows: int = 0
    src_null_keys: int = 0
    tgt_null_keys: int = 0
    src_dup_keys: int = 0
    tgt_dup_keys: int = 0
    missing: int = 0                 # keys in source, not in target
    extra: int = 0                   # keys in target, not in source
    matched: int = 0                 # keys on both sides
    src_keys: int = 0                # distinct non-NULL keys compared (source)
    tgt_keys: int = 0                # distinct non-NULL keys compared (target)
    hash_diffs: int = 0              # matched rows whose row hash differs (before value tolerance)
    differing: int = 0               # matched rows with a real difference (beyond num_tolerance)
    per_column: dict = field(default_factory=dict)
    within_tolerance: dict = field(default_factory=dict)   # numeric column -> rows differing but within num_tolerance
    skipped: bool = False            # kept for compatibility: values are always compared now
    dup_excluded: int = 0            # keys duplicated on either side: left out of the value comparison
    src_dup_df: Optional[DataFrame] = None   # duplicated keys + copies (source)
    tgt_dup_df: Optional[DataFrame] = None   # duplicated keys + copies (target)
    dup_keys_df: Optional[DataFrame] = None  # every key duplicated on either side
    missing_df: Optional[DataFrame] = None
    extra_df: Optional[DataFrame] = None
    diff_df: Optional[DataFrame] = None   # long format: key, column, src_value, tgt_value


def compare_rows(src: DataFrame, tgt: DataFrame, keys: list[str], non_keys: list[str],
                 num_tolerance: float = 0.0, compare_keys: bool = True) -> RowResult:
    r = RowResult()
    any_null = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])
    stats = lambda df: df.agg(F.count(F.lit(1)).alias("rows"),
                              F.sum(any_null.cast("int")).alias("null_keys")).first()
    s_stats, t_stats = stats(src), stats(tgt)
    r.src_rows, r.tgt_rows = s_stats["rows"], t_stats["rows"]
    r.src_null_keys, r.tgt_null_keys = s_stats["null_keys"] or 0, t_stats["null_keys"] or 0
    src, tgt = src.where(~any_null), tgt.where(~any_null)
    dups = lambda df: df.groupBy(*keys).count().where("count > 1")
    r.src_dup_df, r.tgt_dup_df = dups(src), dups(tgt)
    r.src_dup_keys, r.tgt_dup_keys = r.src_dup_df.count(), r.tgt_dup_df.count()

    sk, tk = src.select(*keys).distinct(), tgt.select(*keys).distinct()
    if compare_keys:
        r.missing_df = sk.join(tk, keys, "left_anti")
        r.extra_df = tk.join(sk, keys, "left_anti")
        r.missing, r.extra = r.missing_df.count(), r.extra_df.count()
        r.src_keys, r.tgt_keys = sk.count(), tk.count()
    if r.src_dup_keys or r.tgt_dup_keys:
        # one row per key is needed to pair rows: leave only the duplicated keys out, compare all the others
        r.dup_keys_df = r.src_dup_df.select(*keys).unionByName(r.tgt_dup_df.select(*keys)).distinct()
        r.dup_excluded = r.dup_keys_df.count()
        src, tgt = src.join(r.dup_keys_df, keys, "left_anti"), tgt.join(r.dup_keys_df, keys, "left_anti")

    s = src.select(*keys, row_hash(non_keys).alias("_h_s"))
    g = tgt.select(*keys, row_hash(non_keys).alias("_h_t"))
    matched = s.join(g, keys, "inner")
    r.matched = matched.count()
    hash_diff = matched.where(F.col("_h_s") != F.col("_h_t")).select(*keys)
    r.hash_diffs = hash_diff.count()
    if not r.hash_diffs or not non_keys:
        return r

    # rows whose hash differs: compare column by column (applies num_tolerance)
    sd = src.join(hash_diff, keys).select(*keys, *[F.col(c).alias(f"{c}__src") for c in non_keys])
    td = tgt.join(hash_diff, keys).select(*keys, *[F.col(c).alias(f"{c}__tgt") for c in non_keys])
    cmp = sd.join(td, keys)
    dtypes = {f.name: f.dataType for f in src.schema.fields}
    conds = {}
    for c in non_keys:
        a, b = F.col(f"{c}__src"), F.col(f"{c}__tgt")
        if num_tolerance and is_numeric(dtypes[c]):
            conds[c] = (a.isNull() != b.isNull()) | (F.abs(a - b) > F.lit(num_tolerance))
        else:
            conds[c] = ~a.eqNullSafe(b)
    conds = {c: F.coalesce(e, F.lit(False)) for c, e in conds.items()}
    within = {c: (~F.col(f"{c}__src").eqNullSafe(F.col(f"{c}__tgt"))) & ~conds[c]
              for c in non_keys if num_tolerance and is_numeric(dtypes[c])}
    agg = cmp.agg(F.sum(reduce(lambda x, y: x | y, conds.values()).cast("int")).alias("_rows"),
                  *[F.sum(e.cast("int")).alias(c) for c, e in conds.items()],
                  *[F.sum(F.coalesce(e, F.lit(False)).cast("int")).alias(f"{c}|within") for c, e in within.items()]
                  ).first().asDict()
    r.within_tolerance = {k.split("|")[0]: agg.pop(k) or 0 for k in [k for k in agg if k.endswith("|within")]}
    r.differing = agg.pop("_rows") or 0
    r.per_column = dict(sorted(((c, v) for c, v in agg.items() if v), key=lambda kv: -kv[1]))
    # long format for the mismatch table: one row per (key, differing column)
    cells = F.array(*[F.when(e, F.struct(F.lit(c).alias("column"),
                                         F.col(f"{c}__src").cast("string").alias("src_value"),
                                         F.col(f"{c}__tgt").cast("string").alias("tgt_value")))
                      for c, e in conds.items()])
    r.diff_df = (cmp.select(key_json(keys).alias("key"), F.explode(cells).alias("cell"))
                 .where(F.col("cell").isNotNull())
                 .select("key", "cell.column", "cell.src_value", "cell.tgt_value"))
    return r


# --------------------------------------------------------------------------- 4. column profiles
def compare_profiles(src: DataFrame, tgt: DataFrame, cols: list[str], num_tolerance: float = 0.0,
                     tolerated_rows: Optional[dict] = None) -> dict:
    """
    {column: [differences]} for every compared column ([] = profile matches).
      every column : nulls, distinct (numeric columns: no distinct when num_tolerance is set)
      text         : min / max length
      date / ts    : min / max
    tolerated_rows is no longer used (it scaled the removed numeric sum check); kept for compatibility.
    """
    dtypes = {f.name: f.dataType for f in src.schema.fields}

    def exprs(c):
        col, dtype = F.col(c), dtypes[c]
        e = [F.sum(col.isNull().cast("int")).alias(f"{c}|nulls")]
        if not (num_tolerance and is_numeric(dtype)):     # distinct counts would flag tolerated differences
            e.append(F.approx_count_distinct(col).alias(f"{c}|distinct"))
        # numeric columns: nulls (+ distinct) only - min / max / sum were removed (values are compared row by row
        # in row_hash / column_diff; sums mostly repeated those findings)
        if isinstance(dtype, T.StringType):
            e += [F.min(F.length(col)).alias(f"{c}|min_len"), F.max(F.length(col)).alias(f"{c}|max_len")]
        elif isinstance(dtype, (T.TimestampType, T.DateType)):
            e += [F.min(col).alias(f"{c}|min"), F.max(col).alias(f"{c}|max")]
        return e

    all_exprs = [e for c in cols for e in exprs(c)] + [F.count(F.lit(1)).alias("_rows")]
    ps, pt = src.agg(*all_exprs).first().asDict(), tgt.agg(*all_exprs).first().asDict()
    n_rows = max(ps.pop("_rows"), pt.pop("_rows"), 1)
    out = {c: [] for c in cols}
    for k, a in ps.items():
        b = pt[k]
        if a == b:
            continue
        c, stat = k.split("|")
        out[c].append(f"{stat} {_fmt(a)} vs {_fmt(b)}")
    return out


def _fmt(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int,)) and not isinstance(v, bool):
        return f"{v:,}"
    try:
        f = float(v)
        return f"{f:,.2f}" if abs(f) >= 1000 else f"{f:g}"
    except (TypeError, ValueError):
        return str(v)


# --------------------------------------------------------------------------- 3b. rows without keys
@dataclass
class RowSetResult:
    """Whole-row comparison for tables without keys: both sides as multisets of rows."""
    src_rows: int = 0
    tgt_rows: int = 0
    only_src: int = 0              # rows (counting copies) that exist only in the source
    only_tgt: int = 0              # rows that exist only in the target
    copy_diff_values: int = 0      # distinct rows present on both sides but a different number of times
    copy_diff_rows: int = 0        # sum of |source copies - target copies| for those rows
    matched: int = 0               # rows present on both sides (counting copies)
    copy_src_rows: int = 0         # rows with a different copy count: their copies in the source ...
    copy_tgt_rows: int = 0         # ... and in the target
    only_src_df: Optional[DataFrame] = None   # mismatch-table format: key (= whole row as JSON), src_value, tgt_value
    only_tgt_df: Optional[DataFrame] = None
    copies_df: Optional[DataFrame] = None

    @property
    def differing(self) -> int:
        return self.only_src + self.only_tgt + self.copy_diff_rows


def compare_rowsets(src: DataFrame, tgt: DataFrame, cols: list[str]) -> RowSetResult:
    """
    Rows are matched by ALL compared columns (a hash of the whole row). Each distinct row is counted on
    both sides; a row is only in one side, or on both with the same or a different number of copies.
    Exact, including tables with fully duplicated rows.
    """
    hs = src.select(row_hash(cols).alias("_h"), *cols)
    ht = tgt.select(row_hash(cols).alias("_h"), *cols)
    j = (hs.groupBy("_h").agg(F.count(F.lit(1)).alias("_cs"))
         .join(ht.groupBy("_h").agg(F.count(F.lit(1)).alias("_ct")), "_h", "full_outer")
         .fillna(0, ["_cs", "_ct"]))
    both = (F.col("_cs") > 0) & (F.col("_ct") > 0)
    a = j.agg(F.sum("_cs").alias("s"), F.sum("_ct").alias("t"),
              F.sum(F.when(F.col("_ct") == 0, F.col("_cs")).otherwise(0)).alias("os"),
              F.sum(F.when(F.col("_cs") == 0, F.col("_ct")).otherwise(0)).alias("ot"),
              F.sum(F.when(both & (F.col("_cs") != F.col("_ct")), 1).otherwise(0)).alias("cv"),
              F.sum(F.when(both, F.abs(F.col("_cs") - F.col("_ct"))).otherwise(0)).alias("cr"),
              F.sum(F.least("_cs", "_ct")).alias("m"),
              F.sum(F.when(both & (F.col("_cs") != F.col("_ct")), F.col("_cs")).otherwise(0)).alias("ps"),
              F.sum(F.when(both & (F.col("_cs") != F.col("_ct")), F.col("_ct")).otherwise(0)).alias("pt")).first()
    r = RowSetResult(src_rows=a["s"] or 0, tgt_rows=a["t"] or 0, only_src=a["os"] or 0, only_tgt=a["ot"] or 0,
                     copy_diff_values=a["cv"] or 0, copy_diff_rows=a["cr"] or 0, matched=a["m"] or 0,
                     copy_src_rows=a["ps"] or 0, copy_tgt_rows=a["pt"] or 0)

    row_json = F.to_json(F.struct(*[F.col(c) for c in cols])).alias("key")
    def example(side_df, cond):
        sel = j.where(cond)
        return (side_df.join(sel, "_h", "inner").dropDuplicates(["_h"])
                .select(row_json, F.col("_cs").cast("string").alias("src_value"), F.col("_ct").cast("string").alias("tgt_value")))
    if r.only_src:
        r.only_src_df = example(hs, F.col("_ct") == 0)
    if r.only_tgt:
        r.only_tgt_df = example(ht, F.col("_cs") == 0)
    if r.copy_diff_values:
        r.copies_df = example(hs, both & (F.col("_cs") != F.col("_ct")))
    return r


# --------------------------------------------------------------------------- 3c. whole-table fingerprints (no keys)
def row_bucket(cols: list[str], buckets: int):
    """Bucket of a row: identical rows always get the same bucket, on both sides."""
    return F.pmod(F.xxhash64(row_hash(cols)), F.lit(buckets))


def bucket_fingerprints(df: DataFrame, cols: list[str], buckets: int) -> dict:
    """
    {bucket: (rows, hash_sum)} for the whole table in one pass. Each task pre-aggregates, so only about
    `buckets` small rows per task are shuffled -- never the table's rows. The sum is a DECIMAL(38,0):
    64-bit hashes summed over billions of rows would overflow a BIGINT (an error in ANSI mode).
    """
    h = F.xxhash64(row_hash(cols))
    fp = (df.select(h.alias("_h"))
          .groupBy(F.pmod(F.col("_h"), F.lit(buckets)).alias("_b"))
          .agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("_h").cast("decimal(38,0)")).alias("s")))
    return {r["_b"]: (r["n"], r["s"]) for r in fp.collect()}


def differing_buckets(fs: dict, ft: dict) -> list:
    """Buckets whose (rows, hash_sum) differ, biggest row-count difference first."""
    diff = [b for b in set(fs) | set(ft) if fs.get(b) != ft.get(b)]
    return sorted(diff, key=lambda b: (-abs(fs.get(b, (0, 0))[0] - ft.get(b, (0, 0))[0]), b))


# --------------------------------------------------------------------------- 4b. column distributions
def distribution_buckets(df: DataFrame, cols: list[str], max_categories: int) -> list[tuple]:
    """
    How each text / date / timestamp column is split into buckets (decided once, on the source side):
      text, <= max_categories distinct values : one bucket per value           ("values")
      text, more distinct values              : one bucket per text length     ("length")
      date                                    : per day (<= 400 days) else per month
      timestamp                               : per day / month, AND per hour of day ("hour": shows time-zone shifts)
    Returns [(column, kind, bucket expression)].
    """
    dtypes = {f.name: f.dataType for f in df.schema.fields}
    cand = [c for c in cols if isinstance(dtypes.get(c), (T.StringType, T.DateType, T.TimestampType))]
    if not cand:
        return []
    est = df.agg(*[F.approx_count_distinct(F.to_date(c) if isinstance(dtypes[c], T.TimestampType) else F.col(c))
                   .alias(c) for c in cand]).first().asDict()
    null = F.lit(NULL_TOKEN)
    out = []
    for c in cand:
        col, n = F.col(c), est[c] or 0
        if isinstance(dtypes[c], T.StringType):
            if n <= max_categories:
                out.append((c, "values", F.coalesce(col, null)))
            else:
                out.append((c, "length", F.coalesce(F.length(col).cast("string"), null)))
        else:
            day = n <= 400
            out.append((c, "day" if day else "month", F.coalesce(F.date_format(col, "yyyy-MM-dd" if day else "yyyy-MM"), null)))
            if isinstance(dtypes[c], T.TimestampType):
                out.append((c, "hour", F.coalesce(F.date_format(col, "HH"), null)))
    return out


def compare_distributions(src: DataFrame, tgt: DataFrame, cols: list[str], max_categories: int = 1000,
                          tolerance_pct: float = 0.0, tolerance_rows: int = 0) -> tuple:
    """
    Row counts per bucket for every text / date / timestamp column, both sides, in ONE aggregation per side.
    A bucket fails when |source - target| > tolerance_pct % of the larger count AND > tolerance_rows
    (within either limit = tolerated: counted in n_within, reported as WARN).
    Returns ({column: {"kinds": {kind: buckets}, "diff": [(kind, bucket, src, tgt), ...], "n_diff": int,
                       "n_within": int, "within": [(kind, bucket, src, tgt), ...]}},
             mismatch DataFrame (key = {column, kind, bucket} JSON, src_value / tgt_value = counts)).
    """
    specs = distribution_buckets(src, cols, max_categories)
    if not specs:
        return {}, None
    cells = F.explode(F.array(*[F.struct(F.lit(c).alias("column"), F.lit(k).alias("kind"), e.cast("string").alias("bucket"))
                                for c, k, e in specs])).alias("x")
    count = lambda df, name: df.select(cells).select("x.*").groupBy("column", "kind", "bucket").agg(F.count(F.lit(1)).alias(name))
    j = (count(src, "s").join(count(tgt, "t"), ["column", "kind", "bucket"], "full_outer").fillna(0, ["s", "t"])
         .withColumn("d", F.abs(F.col("s") - F.col("t"))))
    j = (j.withColumn("bad", (F.col("d") > F.greatest("s", "t") * F.lit(tolerance_pct / 100.0))
                      & (F.col("d") > F.lit(tolerance_rows)))
          .withColumn("tol", (F.col("d") > 0) & ~F.col("bad")))
    sizes = j.groupBy("column", "kind").agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("bad").cast("int")).alias("nd"),
                                            F.sum(F.col("tol").cast("int")).alias("nt")).collect()
    out = {c: {"kinds": {}, "diff": [], "n_diff": 0, "n_within": 0, "within": []} for c, _, _ in specs}
    for r in sizes:
        out[r["column"]]["kinds"][r["kind"]] = r["n"]
        out[r["column"]]["n_diff"] += r["nd"] or 0
        out[r["column"]]["n_within"] += r["nt"] or 0
    w = j.where("bad")
    # the 5 biggest differences of EACH column, failing and tolerated separately
    # (a global top-N could leave a column with differences but no example)
    from pyspark.sql.window import Window
    rank = Window.partitionBy("column", "bad").orderBy(F.desc("d"), "kind", "bucket")
    top = (j.where("d > 0").withColumn("_rn", F.row_number().over(rank))
           .where("_rn <= 5").orderBy("column", "_rn").collect())
    for r in top:
        out[r["column"]]["diff" if r["bad"] else "within"].append((r["kind"], r["bucket"], r["s"], r["t"]))
    mism = w.select(F.to_json(F.struct("column", "kind", "bucket")).alias("key"), F.col("column"),
                    F.col("s").cast("string").alias("src_value"), F.col("t").cast("string").alias("tgt_value"))
    return out, mism
