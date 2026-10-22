# Databricks notebook source
# MAGIC %md
# MAGIC # Send ONE report for the runs of this job
# MAGIC Last task of a job that runs the full-load YAML and / or the incremental YAML (each with `send_email = false`).
# MAGIC It collects the run id of every upstream task that ran, reads those runs back from `parity_results`,
# MAGIC and sends one combined e-mail. A task that did not run (or failed before saving) is simply left out.
# MAGIC
# MAGIC Job setup: make this task depend on the validation tasks with **Run if: All done**, so the e-mail also goes
# MAGIC out when a validation task fails or is skipped. Without a job, fill `run_ids` by hand.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("config", "../configs/tables.yaml", "Config with results_table / email settings (YAML)")
dbutils.widgets.text("tasks", "full_load,incremental", "Upstream task keys (comma-separated)")
dbutils.widgets.text("run_ids", "", "Or: run ids (comma-separated)")
dbutils.widgets.text("latest_yamls", "", "Or: YAML paths (comma-separated): the latest run of each in the last 24 h")
dbutils.widgets.text("email_to", "", "E-mail to (empty = settings.email.to)")

try:
    import etl_parity
except ImportError:
    sys.path.insert(0, os.path.dirname(os.getcwd()))
    import etl_parity
from etl_parity import send_report

# COMMAND ----------

config = dbutils.widgets.get("config")
config = config if config.startswith("/") else os.path.normpath(os.path.join(os.getcwd(), config))
run_ids = [r.strip() for r in dbutils.widgets.get("run_ids").split(",") if r.strip()]
latest = [y.strip() for y in dbutils.widgets.get("latest_yamls").split(",") if y.strip()]
if not run_ids and not latest:                    # in a job: the run id each validation task published
    for task in [t.strip() for t in dbutils.widgets.get("tasks").split(",") if t.strip()]:
        try:
            rid = dbutils.jobs.taskValues.get(taskKey=task, key="run_id", default="", debugValue="")
        except Exception:                         # task not in this job run / did not run
            rid = ""
        print(f"{task}: {rid or '(did not run)'}")
        if rid:
            run_ids.append(rid)

report = send_report(run_ids, config=config, latest=latest or None, email_to=dbutils.widgets.get("email_to") or None)
print(report.subject)
print("e-mail sent" if report.email_sent else f"e-mail not sent: {report.email_error or 'not configured / send_on'}")

# COMMAND ----------

displayHTML(report.report_html)
