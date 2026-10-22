import sys
ROOT = "/Workspace/Users/skk983@ftr.com/parity_framework_v3"
sys.path.insert(0, ROOT)

import etl_parity
from etl_parity import run_parity, send_report
print(etl_parity.__version__)                       # 3.10.0 (or 3.9.3)

FULL_YAML  = f"{ROOT}/configs/icd_full_rerun.yaml"  # set to None to skip
DELTA_YAML = f"{ROOT}/configs/icd_delta.yaml"       # set to None to skip
ENV        = "databricks"
EMAIL_TO   = None        # None = use settings.email.to from the YAML; or "a@ftr.com, b@ftr.com"



runs = []
if FULL_YAML:
    runs.append(run_parity(FULL_YAML, env=ENV, send_email=False))
if DELTA_YAML:
    runs.append(run_parity(DELTA_YAML, env=ENV, send_email=False))
print("run ids:", [r.run_id for r in runs])


report = send_report(runs, config=FULL_YAML or DELTA_YAML, email_to=EMAIL_TO)
print(report.subject)
print("e-mail sent" if report.email_sent else f"e-mail NOT sent: {report.email_error or 'no recipients / send_on'}")


displayHTML(report.report_html)
