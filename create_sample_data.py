# Databricks notebook source
# MAGIC %md
# MAGIC # Create sample tables for trying the ETL parity framework
# MAGIC Writes `<schema>.<table>_src` (existing ETL output) and `<schema>.<table>_tgt` (new ETL output) into an
# MAGIC **existing** schema, with known bugs planted on the target side. Nothing else is created.
# MAGIC
# MAGIC | table | rows | planted differences on the target side |
# MAGIC |---|---|---|
# MAGIC | fact_sales | 600,000 | 30 rows missing, 5 extra, 300 wrong amounts (+5.00), ~3,000 amounts +0.01 (within tolerance), trailing spaces (ignored), incremental run missed 600 updates and rewrote 240 other rows |
# MAGIC | dim_customer | ~2,000 | 1 version missing, wrong end dates / current flags on 3 customers, 2 updates missed by the incremental run |
# MAGIC | dim_product | 500 | column CATEGORY missing, BRAND added, LIST_PRICE decimal(10,4) instead of (10,2), 3 duplicated rows |
# MAGIC | fact_inventory_snapshot | 5,000 | none (named fact_inv_snapshot on the target side): must PASS everything |
# MAGIC | fact_events | 700,700 | **no key at all**, real duplicate rows; 50 rows missing, 20 extra, ~30 changed amounts, 350 duplicates missing |
# MAGIC
# MAGIC Params for run_parity that match this data: `{"SRC_BATCH_ID":"101","TGT_BATCH_ID":"9001","SRC_RUN_START":"2026-10-13 02:00:00","TGT_RUN_START":"2026-10-13 04:00:00"}`

# COMMAND ----------

import datetime as dt
from decimal import Decimal

from pyspark.sql import functions as F

OLD_TS = F.lit("2026-09-01 00:00:00").cast("timestamp")
SRC_RUN = F.lit("2026-10-13 02:30:00").cast("timestamp")
TGT_RUN = F.lit("2026-10-13 04:30:00").cast("timestamp")


def build(spark, schema: str):
    def save(df, table, side):
        name = f"{schema}.{table.lower()}_{side}"
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
        print(f"  wrote {name}: {spark.table(name).count():,} rows")

    # ---------------------------------------------------------------- FACT_SALES (600k rows -> sampled)
    lines = spark.createDataFrame([(1,), (2,)], "ORDER_LINE_NBR INT")
    base = (spark.range(1, 300_001).select(F.col("id").cast("int").alias("ORDER_ID"))
            .crossJoin(lines)
            .withColumn("NET_AMT", ((F.col("ORDER_ID") * 7 + F.col("ORDER_LINE_NBR") * 13) % 50000 / 100 + 1)
                        .cast("decimal(12,2)"))
            .withColumn("STATUS", F.element_at(F.array(F.lit("OPEN"), F.lit("CLOSED"), F.lit("SHIPPED")),
                                               (F.col("ORDER_ID") % 3 + 1).cast("int")))
            .withColumn("SALE_DT", F.date_add(F.lit("2026-01-01").cast("date"), (F.col("ORDER_ID") % 285).cast("int"))))
    oid, line = F.col("ORDER_ID"), F.col("ORDER_LINE_NBR")
    in_src_run = oid % 50 == 0
    src = (base.withColumn("SALES_SK", (oid * 10 + line).cast("long"))
           .withColumn("ETL_BATCH_ID", F.when(in_src_run, 101).otherwise(100))
           .withColumn("LOAD_TS", F.when(in_src_run, SRC_RUN).otherwise(OLD_TS)))
    in_tgt_run = (in_src_run & (oid % 1000 != 0)) | (oid % 2500 == 25)        # missed 600 updates, rewrote 240 rows
    tgt = (base.where(~((oid % 9973 == 0) & (line == 1)))                       # 30 rows missing
           .withColumn("NET_AMT", F.when((oid % 997 == 0) & (line == 2), F.col("NET_AMT") + 5)          # 300 wrong
                                   .when((oid % 101 == 0) & (line == 1), F.col("NET_AMT") + Decimal("0.01"))
                                   .otherwise(F.col("NET_AMT")).cast("decimal(12,2)"))
           .withColumn("STATUS", F.when(oid % 503 == 0, F.concat("STATUS", F.lit(" "))).otherwise(F.col("STATUS")))
           .withColumn("SALES_SK", (oid * 10 + line + 5_000_000).cast("long"))
           .withColumn("ETL_BATCH_ID", F.when(in_tgt_run, 9001).otherwise(9000))
           .withColumn("LOAD_TS", F.when(in_tgt_run, TGT_RUN).otherwise(OLD_TS)))
    extra = spark.createDataFrame(
        [(300_000 + i, 1, Decimal("9.99"), "OPEN", dt.date(2026, 10, 1), 9_000_000 + i, 9000, dt.datetime(2026, 9, 1))
         for i in range(1, 6)], tgt.schema)                                       # 5 extra rows
    print("FACT_SALES")
    save(src, "FACT_SALES", "src")
    save(tgt.unionByName(extra), "FACT_SALES", "tgt")

    # ---------------------------------------------------------------- DIM_CUSTOMER (history rows)
    high = dt.date(9999, 12, 31)
    cust_schema = ("CUSTOMER_NBR INT, EFF_FROM_DT DATE, EFF_TO_DT DATE, CURRENT_FLG STRING, CUSTOMER_NAME STRING, "
                   "SEGMENT STRING, CUSTOMER_SK LONG, UPDATE_TS TIMESTAMP")

    def history(c, run_ts):
        n, rows, start = 1 + c % 3, [], dt.date(2025, 1, 1) + dt.timedelta(days=c % 30)
        for v in range(n):
            last = v == n - 1
            end = high if last else start + dt.timedelta(days=89)
            touched = c % 10 == 0 and v >= n - 2
            rows.append([c, start, end, "Y" if last else "N", f"Customer {c} v{v + 1}", ["RETAIL", "SMB", "ENT"][v % 3],
                         c * 10 + v, run_ts if touched else dt.datetime(2026, 1, 1)])
            if not last:
                start = end + dt.timedelta(days=1)
        return rows

    src_c, tgt_c = [], []
    for c in range(1, 1001):
        src_c += [tuple(r) for r in history(c, dt.datetime(2026, 10, 13, 2, 30))]
        t = history(c, dt.datetime(2026, 10, 13, 4, 30))
        if c == 7:
            t[0][2], t[0][3] = high, "Y"                 # old version never closed
        if c == 13:
            t[1][1] = t[1][1] + dt.timedelta(days=5)     # version starts 5 days late (different key)
        if c == 22:
            t[0][2] = t[1][1] + dt.timedelta(days=10)    # wrong end date
        if c == 40:
            t = [t[0]]                                   # this run's change never applied
            t[0][2], t[0][3], t[0][7] = high, "Y", dt.datetime(2026, 1, 1)
        for r in t:
            r[6] += 5_000_000
        tgt_c += [tuple(r) for r in t]
    print("DIM_CUSTOMER")
    save(spark.createDataFrame(src_c, cust_schema), "DIM_CUSTOMER", "src")
    save(spark.createDataFrame(tgt_c, cust_schema), "DIM_CUSTOMER", "tgt")

    # ---------------------------------------------------------------- DIM_PRODUCT (schema + duplicates)
    prod = [(f"P{p:04d}", f"Product {p}", Decimal(f"{(p % 90) + 10}.99"), ["TOYS", "FOOD", "HOME"][p % 3])
            for p in range(1, 501)]
    src_p = spark.createDataFrame(prod, "PRODUCT_CODE STRING, PRODUCT_NAME STRING, LIST_PRICE DECIMAL(10,2), CATEGORY STRING")
    tgt_p = (src_p.drop("CATEGORY").withColumn("LIST_PRICE", F.col("LIST_PRICE").cast("decimal(10,4)"))
             .withColumn("BRAND", F.lit("ACME")))
    tgt_p = tgt_p.unionByName(tgt_p.where(F.col("PRODUCT_CODE").isin("P0010", "P0020", "P0030")))
    print("DIM_PRODUCT")
    save(src_p, "DIM_PRODUCT", "src")
    save(tgt_p, "DIM_PRODUCT", "tgt")

    # ---------------------------------------------------------------- FACT_INVENTORY_SNAPSHOT (clean)
    inv = (spark.range(0, 5000)
           .select(F.date_add(F.lit("2026-10-01").cast("date"), (F.col("id") % 5).cast("int")).alias("SNAPSHOT_DT"),
                   (F.col("id") % 50 + 1).cast("int").alias("STORE_ID"),
                   (F.col("id") / 250 + 1).cast("int").alias("SKU_ID"),
                   (F.col("id") % 37).cast("int").alias("ON_HAND_QTY"))
           .dropDuplicates(["SNAPSHOT_DT", "STORE_ID", "SKU_ID"]))
    print("FACT_INVENTORY_SNAPSHOT")
    save(inv.withColumn("ETL_BATCH_ID", F.lit(100)), "FACT_INVENTORY_SNAPSHOT", "src")
    save(inv.withColumn("ETL_BATCH_ID", F.lit(9000)), "FACT_INV_SNAPSHOT", "tgt")

    # ---------------------------------------------------------------- FACT_EVENTS (no key at all, has duplicate rows)
    ev = (spark.range(0, 700_000)
          .select(F.col("id"),
                  (F.col("id") % 5000).cast("int").alias("USER_ID"),
                  F.element_at(F.array(*[F.lit(x) for x in ("VIEW", "CLICK", "CART", "BUY")]),
                               (F.col("id") % 4 + 1).cast("int")).alias("EVENT_TYPE"),
                  F.date_add(F.lit("2026-08-01").cast("date"), (F.col("id") % 60).cast("int")).alias("EVENT_DT"),
                  (F.col("id") % 97 + 0.5).cast("decimal(10,2)").alias("AMOUNT"),
                  F.when(F.col("id") % 50 == 0, 101).otherwise(100).alias("ETL_BATCH_ID")))
    i = F.col("id")
    src_ev = ev.unionByName(ev.where(i % 1000 == 0))                          # 700 rows loaded twice (real duplicates)
    tgt_ev = (ev.where(~(i % 14000 == 7))                                      # 50 rows dropped
              .withColumn("AMOUNT", F.when(i % 23333 == 1, F.col("AMOUNT") + 1).otherwise(F.col("AMOUNT")).cast("decimal(10,2)"))  # ~30 changed
              .withColumn("ETL_BATCH_ID", F.when(i % 50 == 0, 9001).otherwise(9000)))
    tgt_ev = tgt_ev.unionByName(tgt_ev.where((i % 1000 == 0) & (i % 2000 != 0)))       # only half the duplicates
    extra = spark.range(0, 20).select((F.col("id") + 1_000_000).alias("id"), F.lit(99999).alias("USER_ID"),
                                      F.lit("VIEW").alias("EVENT_TYPE"), F.lit("2026-09-30").cast("date").alias("EVENT_DT"),
                                      F.lit(1.5).cast("decimal(10,2)").alias("AMOUNT"), F.lit(9000).alias("ETL_BATCH_ID"))
    print("FACT_EVENTS (no key)")
    save(src_ev.drop("id"), "FACT_EVENTS", "src")
    save(tgt_ev.unionByName(extra).drop("id"), "FACT_EVENTS", "tgt")

    print("\nDone. Validate with env='databricks' and these params (they match this sample data):")
    print('  {"SRC_BATCH_ID":"101","TGT_BATCH_ID":"9001","SRC_RUN_START":"2026-10-13 02:00:00","TGT_RUN_START":"2026-10-13 04:00:00"}')

# COMMAND ----------

if __name__ == "__main__" or "dbutils" in globals():
    try:
        dbutils.widgets.text("schema", "it_bronze_dev.test_only")      # noqa: F821  (Databricks only)
        SCHEMA = dbutils.widgets.get("schema")                          # noqa: F821
    except NameError:
        import sys
        SCHEMA = sys.argv[1] if len(sys.argv) > 1 else "test_only"
    from pyspark.sql import SparkSession
    build(SparkSession.getActiveSession() or SparkSession.builder.getOrCreate(), SCHEMA)
