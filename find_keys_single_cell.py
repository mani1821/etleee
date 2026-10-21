# ============================== FIND KEY COLUMNS: paste this whole cell into a Databricks notebook ==============================
# Input: the tables, plus one list of columns to ignore in ALL tables. Output: the key columns of each table.
SCHEMA = "it_bronze_dev.test_only"   # catalog.schema the tables live in
LOAD_TYPE = "full"                  # "full": keys over the whole table
                                    # "delta": keys over each table's latest load window (incremental tables)
TABLES = [                          # full : table names           e.g. "fact_sales_src"
    "fact_sales_src",               # delta: (table, incremental column, watermark value) tuples, e.g.
    "dim_customer_src",             #        ("fact_sales_src", "LOAD_TS", "2026-10-19T10:24:22.858+00:00")
]                                   # (a table name containing a "." is used as is, otherwise SCHEMA. is added)
TABLES_QUERY = None                 # or a SQL query; its rows are added to TABLES:
                                    #   full : 1st column = table name
                                    #   delta: 1st = table name, 2nd = incremental column, 3rd = watermark value
# TABLES_QUERY = """SELECT table_name, incr_column, watermark_value FROM my_catalog.my_schema.etl_control
#                   WHERE load_type = 'INCREMENTAL'"""
# delta window per table:  <column> BETWEEN '<watermark date> 00:00:00' AND '<watermark value>'
#   e.g. watermark 2026-10-19T10:24:22.858+00:00 -> LOAD_TS BETWEEN '2026-10-19 00:00:00' AND '2026-10-19T10:24:22.858+00:00'
YAML_NAME = lambda t: t.split(".")[-1].upper()   # table name written in the YAML entries (adjust to your naming)
EXCLUDE_COLS = ["SALES_SK", "CUSTOMER_SK", "ETL_BATCH_ID", "LOAD_TS", "UPDATE_TS"]   # never used as key columns, any table

AUTO_EXCLUDE_TECHNICAL = True    # also skip columns that look technical (*_SK, ETL_*, *BATCH*, LOAD_TS, INSERT_TS, ...)
MAX_KEY_COLUMNS = 3              # largest combination tried
EXACT_BELOW_ROWS = 5_000_000     # tables up to this size: uniqueness verified on every row
VERIFY_PCT = 10                  # bigger tables: verified exactly on this % of rows (hash buckets of the key)
DUPLICATE_TOLERANCE_PCT = 0.01   # a key may have duplicates on up to this % of rows ...
DUPLICATE_TOLERANCE_ROWS = 10    # ... or up to this many duplicate rows, whichever is larger (0 and 0 = strictly unique)
# ===================================================================================================================================
import itertools, random, re
from pyspark.sql import functions as F, types as T

TECHNICAL = (r"(^ETL_|_ETL_|_SK$|^SK_|BATCH|LOAD_(TS|DT|DATE|ID)|INSERT(ED)?_(TS|DT|DATE|BY)|UPDATE[D]?_(TS|DT|DATE|BY)|"
             r"CREATE[D]?_(TS|DT|DATE|BY)|MODIFIED|AUDIT|_HASH$|ROW_?HASH|RECORD_SOURCE|^DW_|INGEST|_RUN_ID$)")
KEY_LIKE, DATE_LIKE = r"(_ID$|^ID$|_KEY$|_NBR$|_NUM$|_NO$|_CD$|_CODE$|_NUMBER$)", r"(_DT$|_DATE$|_PERIOD$|_MONTH$|_YEAR$)"
DESCRIPTIVE = r"(NAME|DESC|ADDR|COMMENT|NOTE|TEXT|MSG|MESSAGE|TITLE|LABEL)"
KEY_TYPES = (T.StringType, T.IntegerType, T.LongType, T.ShortType, T.ByteType, T.DecimalType, T.DateType, T.TimestampType)
RSD, UNIQUE_RATIO = 0.01, 0.97   # HyperLogLog error (+-1%) and the distinct/rows ratio that makes a candidate

name_score = lambda c: 2 if re.search(KEY_LIKE, c) else 1 if re.search(DATE_LIKE, c) else 0
combo_hash = lambda cols: F.xxhash64(*[F.col(c) for c in cols])
excluded_everywhere = {c.upper() for c in EXCLUDE_COLS}
assert LOAD_TYPE in ("full", "delta"), 'LOAD_TYPE must be "full" or "delta"'


def watermark_value(watermark) -> str:
    """The watermark as text, e.g. 2026-10-19T10:24:22.858+00:00 (datetimes from Python keep their offset)."""
    import datetime as _dt
    wm = watermark.isoformat() if isinstance(watermark, (_dt.datetime, _dt.date)) else str(watermark).strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}([T ][0-9:.]*)?(Z|[+-]\d{2}:?\d{2})?$", wm):
        raise ValueError(f"watermark value {wm!r} is not a date/timestamp like 2026-10-19T10:24:22.858+00:00")
    return wm


def watermark_filter(column, watermark):
    """<column> BETWEEN '<watermark day> 00:00:00' AND '<watermark>'
    e.g. LOAD_TS BETWEEN '2026-10-19 00:00:00' AND '2026-10-19T10:24:22.858+00:00'
    (the start has no offset, so it is read in the session time zone - UTC by default on Databricks)."""
    wm = watermark_value(watermark)
    return f"{column} BETWEEN '{wm[:10]} 00:00:00' AND '{wm}'"


def file_sample(df, pct=1.0):
    """Rows from ~pct % of the table's files (only those files are read); first rows for small tables."""
    try:
        files = df.inputFiles()
        if len(files) > 2:
            picked = random.Random(42).sample(files, max(2, round(len(files) * pct / 100)))
            names = [f.rstrip("/").split("/")[-1] for f in picked]
            s = df.where(F.substring_index(F.col("_metadata.file_path"), "/", -1).isin(names))
            if s.limit(1).count():
                return s
    except Exception:
        pass
    return df.limit(1_000_000)


def estimate(df, combos, null_cols=()):
    """One pass, no shuffle: row count, NULL counts, HyperLogLog distinct count of every combination."""
    exprs = [F.count(F.lit(1)).alias("_rows")] + [F.sum(F.col(c).isNull().cast("int")).alias(f"n|{c}") for c in null_cols]
    exprs += [F.approx_count_distinct(combo_hash(cb) if len(cb) > 1 else F.col(cb[0]), RSD).alias(f"d|{i}")
              for i, cb in enumerate(combos)]
    r = df.agg(*exprs).first().asDict()
    return r["_rows"], {c: r[f"n|{c}"] or 0 for c in null_cols}, {cb: r[f"d|{i}"] or 0 for i, cb in enumerate(combos)}


def duplicates(df, keys, rows):
    """Exact duplicate count: all rows for small tables, else VERIFY_PCT % of the hash buckets of the key
    (all copies of a key share a bucket, so each checked bucket is exact; totals are scaled up)."""
    sub, scope, scale = df.select(*keys), f"all {rows:,} rows", 1.0
    if rows > EXACT_BELOW_ROWS:
        sub = sub.where(F.pmod(combo_hash(keys), F.lit(100)) < VERIFY_PCT)
        scope, scale = f"~{VERIFY_PCT}% of rows (hash buckets)", 100.0 / VERIFY_PCT
    dup = sub.groupBy(*keys).count().where("count > 1")
    st = dup.agg(F.count(F.lit(1)).alias("values"), F.sum(F.col("count") - 1).alias("extra"),
                 F.max("count").alias("max")).first()
    values, extra = st["values"] or 0, st["extra"] or 0
    examples = [{k: r[k] for k in keys} | {"copies": r["count"]} for r in dup.orderBy(F.desc("count")).limit(3).collect()] if values else []
    return dict(values=values, extra=extra, max=st["max"] or 0, scope=scope, scale=scale,
                extra_est=round(extra * scale), examples=examples)


def verdict(d, rows):
    """'unique' | 'near' (duplicates within tolerance) | 'fail', plus a readable explanation."""
    if not d["values"]:
        return "unique", f"verified on {d['scope']}: no duplicates"
    allowed = max(DUPLICATE_TOLERANCE_PCT / 100 * rows, DUPLICATE_TOLERANCE_ROWS)
    est = "" if d["scale"] == 1 else f" (~{d['extra_est']:,} in the whole table)"
    text = (f"verified on {d['scope']}: {d['values']:,} key value(s) duplicated, {d['extra']:,} extra row(s){est}, "
            f"max {d['max']} copies; e.g. {d['examples']}")
    return ("near" if d["extra_est"] <= allowed else "fail"), text


def find_key(table, where=None, incr_col=None):
    df = spark.table(table if "." in table else f"{SCHEMA}.{table}")
    df = df.toDF(*[c.upper() for c in df.columns])
    if where:                                            # delta: only the latest load window
        df = df.where(where)
        if not df.limit(1).count():
            return dict(table=table, keys=[], confidence=None, rows=0, skipped=[], alternatives=[], rejected=[],
                        check=f"no rows in the delta window ({where}): nothing to analyse")
    types, pos = {f.name: f.dataType for f in df.schema.fields}, {c: i for i, c in enumerate(df.columns)}
    skipped = [c for c in df.columns if c in excluded_everywhere or c == (incr_col or "").upper()
               or (AUTO_EXCLUDE_TECHNICAL and re.search(TECHNICAL, c))]
    usable = [c for c in df.columns if c not in skipped and isinstance(types[c], KEY_TYPES)]

    # 1. profile a small file sample: drop columns with NULLs or long text (only what a sample can PROVE;
    #    "constant" is not decided here: tables stored sorted / clustered make a file sample look constant)
    smp = file_sample(df)
    exprs = [F.count(F.lit(1)).alias("_rows")]
    for c in usable:
        exprs += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}|n"), F.approx_count_distinct(c, RSD).alias(f"{c}|d")]
        if isinstance(types[c], T.StringType):
            exprs.append(F.max(F.length(c)).alias(f"{c}|len"))
    p = smp.agg(*exprs).first().asDict()
    pool = [c for c in usable if not p[f"{c}|n"] and (p.get(f"{c}|len") or 0) <= 200]
    rank = lambda c: (-name_score(c), -(p[f"{c}|d"] or 0), pos[c])
    descriptive = [c for c in pool if re.search(DESCRIPTIVE, c)]
    attempts = [sorted([c for c in pool if c not in descriptive], key=rank)[:15]]
    if descriptive:
        attempts.append(sorted(pool, key=rank)[:15])          # names / descriptions only if nothing else works

    rejected, tried = [], set()
    for cands in attempts:
        # 2. HyperLogLog search over the full table: singles, then pairs, then triples (from the best pairs)
        rows, nulls, est = estimate(df, [(c,) for c in cands], cands)
        ok_cols = [c for c in cands if nulls[c] == 0 and est[(c,)] > 1]           # no NULLs, not constant (full table)
        est = {k: v for k, v in est.items() if k[0] in ok_cols}
        found, size, last = [k for k, v in est.items() if v >= UNIQUE_RATIO * rows], 1, None
        while not found and size < MAX_KEY_COLUMNS and len(ok_cols) > size:
            size += 1
            if size == 2:
                combos = list(itertools.combinations(sorted(ok_cols, key=pos.get), 2))
            else:
                best = sorted(last, key=last.get, reverse=True)[:20]
                combos = sorted({tuple(sorted(set(b) | {c}, key=pos.get)) for b in best for c in ok_cols if c not in b})
            _, _, last = estimate(df, combos)
            found = [k for k, v in last.items() if v >= UNIQUE_RATIO * rows]
        found.sort(key=lambda k: (len(k), -sum(name_score(c) for c in k), [pos[c] for c in k]))
        found = [k for k in found if not any(set(o) < set(k) for o in found)]      # minimal combinations only
        # 3. exact verification, in ranking order: the first candidate that is unique, or has duplicates
        #    within the tolerance (a "near key"), is the answer
        near = None
        for keys in [k for k in found if k not in tried][:3]:
            tried.add(keys)
            v, how = verdict(duplicates(df, list(keys), rows), rows)
            if v == "unique":
                conf = ("HIGH" if how.startswith("verified on all") and any(name_score(c) for c in keys) else
                        "LOW" if any(c in descriptive for c in keys) else "MEDIUM")
                return dict(table=table, keys=list(keys), confidence=conf, rows=rows, check=how, skipped=skipped,
                            alternatives=[list(k) for k in found if k != keys][:3], rejected=rejected)
            if v == "near":
                near = (keys, how)
                break
            rejected.append(f"{list(keys)}: {how}")
        if near:
            keys, how = near
            alts = [list(k) for k in found if k != keys][:3]
            # is there a strictly unique key with one more column? (one HLL pass + one exact check)
            others = [c for c in ok_cols if c not in keys]
            if others and len(keys) < MAX_KEY_COLUMNS:
                _, _, ext = estimate(df, [tuple(sorted(set(keys) | {c}, key=pos.get)) for c in others])
                for k in sorted(ext, key=lambda k: (-ext[k], -sum(name_score(c) for c in k)))[:1]:
                    if verdict(duplicates(df, list(k), rows), rows)[0] == "unique":
                        alts.insert(0, list(k) + ["<- strictly unique"])
            return dict(table=table, keys=list(keys), confidence="LOW" if any(c in descriptive for c in keys) else "MEDIUM",
                        rows=rows, check="NEAR KEY (duplicates within tolerance) - " + how, skipped=skipped,
                        alternatives=alts, rejected=rejected)
    return dict(table=table, keys=[], confidence=None, rows=rows, skipped=skipped, alternatives=[], rejected=rejected,
                check="no unique, non-NULL combination found (duplicate rows, or key needs more columns / excluded columns)")


items = []                                                   # (table, incremental column, watermark)
for t in TABLES:
    items.append(tuple(t) if isinstance(t, (tuple, list)) else (t, None, None))
if TABLES_QUERY:
    q = spark.sql(TABLES_QUERY)
    if LOAD_TYPE == "delta" and len(q.columns) >= 3 and isinstance(q.schema[2].dataType, (T.TimestampType, T.DateType)):
        # format timestamps in Spark WITH their offset, so the driver's local time zone cannot shift the day
        q = q.withColumn(q.columns[2], F.date_format(F.col(q.columns[2]), "yyyy-MM-dd'T'HH:mm:ss.SSSxxx"))
    rows_q = [r for r in q.collect() if r[0]]
    print(f"TABLES_QUERY returned {len(rows_q)} row(s)")
    for r in rows_q:
        if LOAD_TYPE == "delta":
            if len(r) < 3:
                raise ValueError("LOAD_TYPE = 'delta' needs TABLES_QUERY to return: table, incremental column, watermark")
            items.append((str(r[0]).strip(), str(r[1]).strip(), r[2]))
        else:
            items.append((str(r[0]).strip(), None, None))
items = list(dict.fromkeys(items))                          # drop repeats, keep order
print(f"{len(items)} table(s) to check in {SCHEMA} ({LOAD_TYPE} load)\n")

KEYS, report = {}, []
for t, col, wm in items:
    print(f"... {t}", flush=True)
    where = None
    try:
        if LOAD_TYPE == "delta":
            if not col or wm is None:
                raise ValueError("delta mode needs an incremental column and a watermark value for this table")
            where = watermark_filter(col, wm)
        r = find_key(t, where, col)
    except Exception as e:
        r = dict(table=t, keys=[], confidence="ERROR", rows=None, check=f"{type(e).__name__}: {str(e).splitlines()[0]}",
                 skipped=[], alternatives=[], rejected=[])
    r["window"] = where
    KEYS[t] = r["keys"] or KEYS.get(t, [])          # the same table twice: keep a key that was found
    report.append(r)

print("\n" + "=" * 100)
for r in report:
    print(f"\n{r['table']}")
    if r["keys"]:
        shown = f"{r['keys']}   ({r['confidence']})"
    elif r["confidence"] == "ERROR":
        shown = "- (error, see check)"
    elif r["rows"] == 0:
        shown = "- (no rows to analyse)"
    else:
        shown = "NOT FOUND -> use keys: [] (whole-row comparison)"
    print(f"   keys        : {shown}")
    if r.get("window"):
        print(f"   delta window: {r['window']}")
    print(f"   rows        : {r['rows']:,}" + (" (in the delta window)" if r.get("window") else "")
          if r["rows"] is not None else "   rows        : -")
    print(f"   check       : {r['check']}")
    if r["alternatives"]:
        print(f"   alternatives: {r['alternatives']}")
    if r["rejected"]:
        print(f"   rejected    : {r['rejected']}")
    print(f"   not considered: {r['skipped']}")
print("\nKEYS =", KEYS)

# ---------------------------------------------------------------- tables: entries for configs/tables.yaml (review first)
print("\n# ===== paste under tables: in configs/tables.yaml (review first) =====")
for r in report:
    if r["confidence"] == "ERROR" or r["rows"] == 0:
        print(f"  # {YAML_NAME(r['table'])}: skipped - {r['check'][:100]}")
        continue
    print(f"  - name: {YAML_NAME(r['table'])}")
    note = f"   # {r['confidence']}" if r["keys"] else "   # no key found: whole-row comparison"
    print(f"    keys: [{', '.join(r['keys'])}]{note}")
    if r["skipped"]:
        print(f"    exclude_cols: [{', '.join(r['skipped'])}]")
    if r.get("window"):
        w = r["window"].replace('"', '\\"')
        print("    incremental:")
        print(f'      source_delta: "{w}"')
        print(f'      target_delta: "{w}"')
        print("      delta_only: false")
display(spark.createDataFrame([(r["table"], LOAD_TYPE, r.get("window"), r["keys"], r["confidence"], r["rows"], r["check"])
                               for r in report],
                              "table string, load_type string, delta_window string, keys array<string>, "
                              "confidence string, rows long, check string"))
