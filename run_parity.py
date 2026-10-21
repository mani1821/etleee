# Databricks notebook source
# MAGIC %md
# MAGIC # Run the ETL parity validation
# MAGIC Fill in the widgets at the top and run all. Results land in the tables named in the config
# MAGIC (`parity_results`, `parity_mismatches`). Use a classic cluster with the Oracle JDBC driver for `env = oracle`;
# MAGIC serverless works for `env = databricks`.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("config", "../configs/tables.yaml", "Config (YAML path)")
dbutils.widgets.dropdown("env", "databricks", ["databricks", "oracle"], "Environment")
dbutils.widgets.text("tables", "", "Tables (comma-separated, empty = all)")
# Default params match the sample data from create_sample_data; for real runs use your batch ids / run start times
dbutils.widgets.text("params", '{"SRC_BATCH_ID":"101","TGT_BATCH_ID":"9001","SRC_RUN_START":"2026-10-13 02:00:00","TGT_RUN_START":"2026-10-13 04:00:00"}', "Params (JSON for ${NAME} placeholders)")
dbutils.widgets.dropdown("send_email", "true", ["true", "false"], "E-mail this run's report (false = a later send_report task mails one combined report)")

# Without the wheel installed, import the package from this repo / folder (parent of notebooks/)
try:
    import etl_parity
except ImportError:
    sys.path.insert(0, os.path.dirname(os.getcwd()))
    import etl_parity
from etl_parity import run_parity

print("etl_parity", etl_parity.__version__)

# COMMAND ----------

config = dbutils.widgets.get("config")
config = config if config.startswith("/") else os.path.normpath(os.path.join(os.getcwd(), config))
tables = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()] or None

run = run_parity(config, env=dbutils.widgets.get("env"), tables=tables,
                 params=json.loads(dbutils.widgets.get("params") or "{}"),
                 send_email=dbutils.widgets.get("send_email") == "true")

# in a job: hand the run id to a later send_report task (dbutils.jobs.taskValues; ignored outside jobs)
try:
    dbutils.jobs.taskValues.set(key="run_id", value=run.run_id)
except Exception:
    pass

# COMMAND ----------

# MAGIC %md ## Run report (the same page the e-mail contains)

# COMMAND ----------

displayHTML(run.report_html)

# COMMAND ----------

# MAGIC %md ## Summary per table (expand a row to see its validations and the checks that failed)

# COMMAND ----------

display(run.summary_nested)

# COMMAND ----------

# MAGIC %md ## Flat summary: one row per table per validation

# COMMAND ----------

display(run.summary)

# COMMAND ----------

# MAGIC %md ## Every check that did not pass

# COMMAND ----------

display(run.results.where("status NOT IN ('PASS', 'INFO')").orderBy("table_name", "validation", "check_name"))

# COMMAND ----------

# MAGIC %md ## Example bad rows from this run

# COMMAND ----------

from etl_parity.config import load_config
mismatch_table = load_config(config, dbutils.widgets.get("env")).settings.mismatch_table
display(spark.table(mismatch_table).where(f"run_id = '{run.run_id}'").orderBy("table_name", "kind"))
