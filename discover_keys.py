# Databricks notebook source
# MAGIC %md
# MAGIC # Discover key columns for full-load tables
# MAGIC Finds the column(s) that identify a row in each table, without declared constraints and without
# MAGIC brute-force distinct counts. Results are proposals: review them before copying into `configs/tables.yaml`.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/skk983@ftr.com/parity_framework")      # folder that contains etl_parity/
from etl_parity import discover_keys, to_yaml, KeySettings
from etl_parity.keys import results_df

# 1. The tables: your query. First column = source table (catalog.schema.table),
#    optional second column = target table. Replace with your own query.
tables = spark.sql("""
    SELECT 'it_bronze_dev.test_only.fact_sales_src'   AS source_table, 'it_bronze_dev.test_only.fact_sales_tgt'   AS target_table UNION ALL
    SELECT 'it_bronze_dev.test_only.dim_customer_src',                  'it_bronze_dev.test_only.dim_customer_tgt'
""")

# 2. If your query returns only source tables, derive the target name instead (or set to None):
target_for = None          # e.g. lambda t: t.replace("_src", "_tgt")

# 3. Where to keep the results (created on first run, appended after)
RESULTS_TABLE = "it_bronze_dev.test_only.parity_key_candidates"

settings = KeySettings(
    exact_below_rows=5_000_000,   # tables up to this size: verified on every row
    verify_buckets=10,            # bigger tables: 10 of 100 hash buckets verified exactly
    full_verify=False,            # True = one exact check on all rows for big tables too (one shuffle)
    min_match_pct=90.0,           # key values must exist in the target (rejects surrogate keys)
)

# COMMAND ----------

found = discover_keys(spark, tables, target_for=target_for, settings=settings, results_table=RESULTS_TABLE)
display(results_df(spark, found))

# COMMAND ----------

# MAGIC %md ## Proposed `tables:` entries (review, then copy into configs/tables.yaml)

# COMMAND ----------

# Table name in the YAML: last part of the source name, without a _src suffix (adjust to your naming)
name_for = lambda t: t.split(".")[-1].upper().removesuffix("_SRC").removesuffix("_TGT")
print(to_yaml(found, name_for=name_for))
