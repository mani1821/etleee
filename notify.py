"""
Run report (HTML + plain text) and e-mail.

    settings:
      report_path: /Workspace/Users/me@corp.com/parity_reports   # optional: writes parity_<run_id>.html there
      email:                                                      # optional: no block = no e-mail
        to: "a@corp.com, b@corp.com"
        send_on: always            # always | issues (any WARN/FAIL/ERROR) | failure (any FAIL/ERROR) | never
        smtp_server: mailrelay.ftr.com
        smtp_port: 25
        sender: noreply@ftr.databricks.com
        starttls: true
        subject_prefix: "[ETL parity]"

Report layout:
  1. run summary   - total tables, tables passed; per validation: tables passed (incl. within tolerance) of tables run
  2. summary table - one row per table, one column per validation: PASS / FAIL + aggregated checks
  3. details       - the checks that did not pass, per table
A validation passes when none of its checks FAILs (WARN = within tolerance counts as passed);
a table passes when all its validations pass.
A failed e-mail never fails the run: the error is printed and recorded on the ParityRun (email_error).
"""
from __future__ import annotations

import html
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

SEVERITY = {"PASS": 0, "INFO": 0, "SKIP": 1, "WARN": 2, "FAIL": 3, "ERROR": 4}
COLORS = {"PASS": "#1a7f37", "WARN": "#9a6700", "FAIL": "#cf222e", "ERROR": "#8250df", "SKIP": "#57606a", "-": "#8c959f"}
BG = {"PASS": "#dafbe1", "WARN": "#fff8c5", "FAIL": "#ffebe9", "ERROR": "#fbefff", "SKIP": "#f6f8fa", "-": "#f6f8fa"}
EMAIL_KEYS = {"to", "send_on", "smtp_server", "smtp_port", "sender", "starttls", "subject_prefix", "report_link", "subject"}
VALIDATIONS = [("schema", "Schema"), ("row_count", "Row count"), ("row_hash", "Row hash"),
               ("column_profiles", "Column profiles"), ("column_distributions", "Distributions"),
               ("incremental", "Incremental")]
LABEL = {"PASS": "passed", "WARN": "within tolerance", "FAIL": "failed", "ERROR": "error", "SKIP": "skipped"}


# ------------------------------------------------------------------ report content
def build_report(recs, duration_s: Optional[float] = None, config_path: Optional[str] = None,
                 max_issues: int = 6) -> dict:
    """
    Everything the report needs, from one or more finished runs (Recorder objects, live or loaded with
    Recorder.load). Several runs (e.g. a full-load YAML and an incremental YAML) make ONE combined report.
    """
    recs = recs if isinstance(recs, (list, tuple)) else [recs]
    runs, out = [], []
    for rec in recs:
        meta = dict(rec.meta or {})
        if duration_s is not None and len(recs) == 1:
            meta["duration"] = duration_s
        if config_path and len(recs) == 1:
            meta.setdefault("config", config_path)
        label = meta.get("label") or _label(meta.get("config")) or rec.run_id
        runs.append(dict(label=label, run_id=rec.run_id, run_ts=str(rec.run_ts), env=rec.env or "-",
                         config=meta.get("config") or "-", duration=_dur(meta.get("duration") or 0)))
        out += _tables(rec, label, max_issues)
    # per validation: tables passed (incl. within tolerance) / failed / not run
    by_val = []
    for key, label in VALIDATIONS:
        cells = [t["cells"][key] for t in out if key in t["cells"]]
        by_val.append(dict(key=key, label=label, ran=len(cells), not_run=len(out) - len(cells),
                           passed=sum(1 for c in cells if c["status"] == "PASS"),
                           within=sum(1 for c in cells if c["status"] == "PASS" and c["warn"]),
                           failed=sum(1 for c in cells if c["status"] in ("FAIL", "ERROR"))))
    counts = {s: sum(1 for t in out if t["status"] == s) for s in ("PASS", "FAIL", "ERROR")}
    overall = "ERROR" if counts["ERROR"] else "FAIL" if counts["FAIL"] else "PASS"
    ids = [r["run_id"] for r in runs]
    total = sum(float((rec.meta or {}).get("duration") or 0) for rec in recs) if duration_s is None or len(recs) > 1 \
        else duration_s
    return dict(runs=runs, multi=len(runs) > 1, run_id=", ".join(ids), run_ts=runs[0]["run_ts"] if runs else "",
                env=", ".join(sorted({r["env"] for r in runs})), config=", ".join(r["config"] for r in runs),
                duration=_dur(total), tables=out, counts=counts, overall=overall, by_validation=by_val,
                run_filter=(f"run_id = '{ids[0]}'" if len(ids) == 1 else
                            "run_id IN (" + ", ".join(f"'{i}'" for i in ids) + ")"),
                results_table=recs[0].results_table, mismatch_table=recs[0].mismatch_table)


def _label(config: Optional[str]) -> str:
    """Short run name from the YAML file name: /Workspace/.../icd_full_41-50.yaml -> icd_full_41-50."""
    return os.path.splitext(os.path.basename(str(config)))[0] if config else ""


def _tables(rec, label: str, max_issues: int) -> list:
    tables = rec.nested_rows()
    facts: dict = {}
    for r in rec.rows:                         # (run_id, run_ts, env, table, validation, scope, check, status, src, tgt, detail)
        t, check = r[3], r[6]
        if check == "row_count":
            facts.setdefault(t, {})["rows"] = (r[8], r[9])
        elif check == "rows_compared":
            facts.setdefault(t, {})["compared"] = (r[8], r[9], r[5])
        elif check == "delta_row_count":         # delta-only runs: show the delta instead of the table
            facts.setdefault(t, {}).setdefault("delta_rows", (r[8], r[9], "delta"))
        elif check == "delta_rows_compared":
            facts.setdefault(t, {})["delta_compared"] = (r[8], r[9], (r[5] or "delta"))
    for f in facts.values():
        f.setdefault("rows", f.get("delta_rows"))
        f.setdefault("compared", f.get("delta_compared"))
    out = []
    for t in tables:
        f = facts.get(t["table_name"], {})
        bad = [v for v in t["details"] if SEVERITY.get(v["status"], 0) >= SEVERITY["WARN"]]
        cells = {}                             # validation -> aggregated checks for this table
        for v in t["details"]:
            base = v["validation"].split(" [")[0]
            scope = v["validation"][len(base):].strip(" []")
            cells[base] = dict(status=_verdict(v), scope=scope, checks=v["checks"],
                               **{k: v[k] for k in ("pass", "warn", "fail", "skip", "error")})
        verdicts = [c["status"] for c in cells.values()]
        status = "ERROR" if "ERROR" in verdicts else "FAIL" if "FAIL" in verdicts else "PASS"
        out.append(dict(
            run=label, table=t["table_name"], status=status, cells=cells,
            validations=len(t["details"]), failed=t["validations_failed"],
            rows=f.get("rows"), compared=f.get("compared"), finding=_main_finding(bad),
            issues=[dict(validation=v["validation"], status=v["status"], details=v["details"],
                         checks=list(v["issues"])[:max_issues], more=max(0, len(v["issues"]) - max_issues))
                    for v in bad]))
    return sorted(out, key=lambda x: (-SEVERITY.get(x["status"], 0), x["table"]))


def _verdict(v: dict) -> str:
    """A validation passes when none of its checks fails: PASS and WARN (within tolerance) checks both count as passed."""
    return "ERROR" if v["error"] else "FAIL" if v["fail"] else "PASS"


def _main_finding(bad: list) -> str:
    """The most telling problem first: run error, schema, row comparison, ... in validation order."""
    order = ["run", "schema", "row_hash", "incremental", "column_distributions", "column_profiles", "row_count"]
    rank = lambda v: next((i for i, o in enumerate(order) if v["validation"].startswith(o)), 99)
    worst = max((SEVERITY.get(v["status"], 0) for v in bad), default=0)
    top = sorted([v for v in bad if SEVERITY.get(v["status"], 0) == worst], key=rank)
    return f'{top[0]["validation"]}: {top[0]["details"]}' if top else ""


def _cut(text: str, n: int) -> str:
    return text if len(text) <= n else text[:n].rsplit(" ", 1)[0] + " ..."


def _dur(s: float) -> str:
    s = int(s)
    return f"{s // 3600}h {s % 3600 // 60}m" if s >= 3600 else f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"


def _n(v) -> str:
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "-" if v is None else str(v)


def _rows_txt(t) -> tuple:
    rows = (f"{_n(t['rows'][0])} vs {_n(t['rows'][1])}" + (" (delta)" if len(t["rows"]) > 2 else "")) if t["rows"] else "-"
    if t["compared"]:
        s, g, scope = t["compared"]
        kind = "sample" if "sample" in (scope or "") else "fingerprints" if "fingerprint" in (scope or "") else \
            "delta" if "delta" in (scope or "") else "all"
        comp = f"{_n(s)} vs {_n(g)} ({kind})"
    else:
        comp = "-"
    return rows, comp


def _checks_txt(c: Optional[dict]) -> str:
    """Aggregated checks of one table x validation, e.g. '9 checks: 5 pass, 4 within tolerance'."""
    if not c:
        return "not run"
    parts = [f"{c['pass']} pass"] + [f"{c[k]} {name}" for k, name in
                                     (("warn", "within tolerance"), ("fail", "fail"), ("error", "error"), ("skip", "skipped")) if c[k]]
    return f"{c['checks']} check{'s' if c['checks'] != 1 else ''}: " + ", ".join(parts)


def subject(rep: dict, prefix: str = "[ETL parity]", fixed: Optional[str] = None) -> str:
    """settings.email.subject = a fixed subject (e.g. ICD_DPI_Validation_Report_Notification); else a generated one."""
    if fixed:
        return fixed
    c, n = rep["counts"], len(rep["tables"])
    what = f"{c['PASS']} of {n} tables passed" + (f", {c['FAIL']} failed" if c["FAIL"] else "") + \
           (f", {c['ERROR']} errors" if c["ERROR"] else "")
    runs = ", ".join(r["label"] for r in rep["runs"]) if rep["multi"] else f"run {rep['run_id']}"
    return f"{prefix} {rep['overall']} - {what} ({runs}, {rep['env']})"


# ------------------------------------------------------------------ plain text
def to_text(rep: dict, email: bool = False, link: Optional[str] = None, title: Optional[str] = None) -> str:
    """email=True: short version (summary + status per table and validation + link), no counts, no details."""
    c, n = rep["counts"], len(rep["tables"])
    lines = [title or "ETL parity report"] + ([] if email else [f"Overall result: {rep['overall']}"]) + [""] + \
            ([f"Run ID: {', '.join(r['run_id'] for r in rep['runs'])}"] if email else
             [f"ETL parity run {r['label']} ({r['run_id']})  |  env {r['env']}  |  {r['run_ts']}  |  {r['duration']}"
              for r in rep["runs"]]) + ["",
             "RUN SUMMARY",
             f"  Total tables: {n}" + (f" (across {len(rep['runs'])} runs)" if rep["multi"] else ""),
             f"  Tables passed (every validation passed): {c['PASS']} of {n}"
             + (f"   failed: {c['FAIL']}" if c["FAIL"] else "") + (f"   errors: {c['ERROR']}" if c["ERROR"] else "")]
    for v in rep["by_validation"]:
        lines.append(f"  {v['label'] + ':':<17} {v['passed']} of {v['ran']} tables passed"
                     + (f" ({v['within']} within tolerance)" if v["within"] else "")
                     + (f", {v['failed']} failed" if v["failed"] else "")
                     + (f"   [not run for {v['not_run']}]" if v["not_run"] else ""))
    lines += ["", "SUMMARY TABLE" + ("" if email else " (checks per table per validation)")]
    w = max([len(_tname(rep, t)) for t in rep["tables"]] + [5])
    if email:
        lines.append(f"{'table':<{w}}  {'result':<6}  " + "  ".join(f"{label:<15}" for _, label in VALIDATIONS))
        for t in rep["tables"]:
            lines.append(f"{_tname(rep, t):<{w}}  {t['status']:<6}  " + "  ".join(
                f"{(t['cells'][k]['status'] if k in t['cells'] else 'not run'):<15}" for k, _ in VALIDATIONS))
        lines += ["", f"Full details (checks, row counts, failures): {link}" if link else
                  f"Full details: {rep['results_table']}  WHERE {rep['run_filter']}"]
        return "\n".join(lines)
    for t in rep["tables"]:
        rows, comp = _rows_txt(t)
        lines.append(f"{_tname(rep, t).ljust(w)}  {t['status']:<5}  rows {rows} | compared {comp}")
        for key, label in VALIDATIONS:
            cell = t["cells"].get(key)
            lines.append(f"{'':<{w}}    {label:<16} {(cell['status'] if cell else '-'):<5}  {_checks_txt(cell)}")
    for t in rep["tables"]:
        if not t["issues"]:
            continue
        lines += ["", f"{_tname(rep, t)}  {t['status']}"]
        for v in t["issues"]:
            lines.append(f"  {v['status']:<5} {v['validation']}: {v['details']}")
            for i in v["checks"]:
                lines.append(f"        {i['status']:<5} {i['check']}: {i['detail'][:160]}")
            if v["more"]:
                lines.append(f"        ... {v['more']} more")
    lines += ["", "PASS = no check failed (checks within tolerance count as passed).",
              f"Details: {rep['results_table']} / {rep['mismatch_table']}  WHERE {rep['run_filter']}"]
    return "\n".join(lines)


# ------------------------------------------------------------------ HTML (inline styles: e-mail clients drop <style>)
def _badge(status: str, text: Optional[str] = None) -> str:
    return (f'<span style="display:inline-block;padding:1px 7px;border-radius:9px;font-size:11px;font-weight:600;'
            f'color:{COLORS.get(status, "#57606a")};background:{BG.get(status, "#f6f8fa")}">{text or status}</span>')


def to_html(rep: dict, email: bool = False, link: Optional[str] = None, title: Optional[str] = None) -> str:
    """email=False: full report (notebook output, report file). email=True: short e-mail version - run summary,
    PASS / FAIL per table and validation, and a link to the job run that shows the full report."""
    e = html.escape
    c, n = rep["counts"], len(rep["tables"])
    font = "font-family:Segoe UI,Arial,Helvetica,sans-serif;"
    cell = "padding:6px 10px;border:1px solid #c8ccd1;vertical-align:top;font-size:13px;"
    head = "padding:6px 10px;border:1px solid #c8ccd1;background:#f3f4f6;text-align:left;font-size:12px;"
    h2 = "font-size:16px;font-weight:700;margin:22px 0 8px"
    runs_html = (f'<div style="color:#57606a;font-size:13px;margin:2px 0">Run ID: '
                 f'<b>{e(", ".join(r["run_id"] for r in rep["runs"]))}</b></div>') if email else "".join(
        f'<div style="color:#57606a;font-size:13px;margin:2px 0">{"run <b>" + e(r["label"]) + "</b> &middot; " if rep["multi"] else ""}'
        f'run id <b>{e(r["run_id"])}</b> &middot; env <b>{e(r["env"])}</b> &middot; {e(r["run_ts"])} &middot; '
        f'{e(r["duration"])} &middot; {e(r["config"])}</div>' for r in rep["runs"])

    # 1. run summary: plain table, counts are tables
    across = f' (across {len(rep["runs"])} runs)' if rep["multi"] else ""
    summary = [f'<tr><td style="{cell}">Total tables{across}</td><td style="{cell}text-align:right"><b>{n}</b></td>'
               f'<td style="{cell}"></td></tr>',
               f'<tr><td style="{cell}">Tables passed (every validation passed)</td>'
               f'<td style="{cell}text-align:right"><b>{c["PASS"]}</b> of {n}</td>'
               f'<td style="{cell}">{c["FAIL"]} failed' + (f', {c["ERROR"]} errors' if c["ERROR"] else "") + '</td></tr>']
    for v in rep["by_validation"]:
        note = ", ".join(x for x in (f'{v["within"]} within tolerance' if v["within"] else "",
                                     f'{v["failed"]} failed' if v["failed"] else "",
                                     f'not run for {v["not_run"]}' if v["not_run"] else "") if x)
        summary.append(f'<tr><td style="{cell}">Passed {e(v["label"].lower())}</td>'
                       f'<td style="{cell}text-align:right"><b>{v["passed"]}</b> of {v["ran"]}</td>'
                       f'<td style="{cell}">{e(note)}</td></tr>')

    # 2. summary table: table x validation, aggregated checks; no colours
    cols = "".join(f'<th style="{head}">{e(label)}</th>' for _, label in VALIDATIONS)
    rows = []
    for t in rep["tables"]:
        r, comp = _rows_txt(t)
        tds = []
        for key, _ in VALIDATIONS:
            cl = t["cells"].get(key)
            if not cl:
                tds.append(f'<td style="{cell}">not run</td>')
                continue
            if email:                            # e-mail: the verdict only
                tds.append(f'<td style="{cell}"><b>{cl["status"]}</b></td>')
                continue
            parts = [f'{cl["pass"]} pass'] + [f'{cl[k]} {name}' for k, name in
                                               (("warn", "within tol."), ("fail", "fail"), ("error", "error"),
                                                ("skip", "skipped")) if cl[k]]
            scope = f' <span style="font-size:11px;color:#57606a">({e(cl["scope"])})</span>' if cl["scope"] else ""
            n_ch = f'{cl["checks"]} check' + ("s" if cl["checks"] != 1 else "")
            tds.append(f'<td style="{cell}min-width:120px"><b>{cl["status"]}</b>{scope}<br>'
                       f'<span style="font-size:12px">{n_ch}: {e(", ".join(parts))}</span></td>')
        run_td = f'<td style="{cell}">{e(t["run"])}</td>' if rep["multi"] else ""
        if email:
            rows.append(f'<tr>{run_td}<td style="{cell}"><b>{e(t["table"])}</b></td>'
                        f'<td style="{cell}"><b>{t["status"]}</b></td>{"".join(tds)}</tr>')
            continue
        rows.append(f'<tr>{run_td}<td style="{cell}"><b>{e(t["table"])}</b><br><span style="font-size:11px;color:#57606a">'
                    f'rows {e(r)}<br>compared {e(comp)}</span></td><td style="{cell}"><b>{t["status"]}</b></td>'
                    f'{"".join(tds)}</tr>')

    # 3. details: checks that did not pass (not in the e-mail: the link leads to them)
    details = []
    for t in ([] if email else rep["tables"]):
        if not t["issues"]:
            continue
        items = []
        for v in t["issues"]:
            checks = "".join(f'<li style="margin:2px 0">{_badge(i["status"])} <b>{e(i["check"])}</b>: '
                             f'{e(i["detail"][:220])}</li>' for i in v["checks"])
            more = f'<li>... {v["more"]} more</li>' if v["more"] else ""
            items.append(f'<div style="margin:8px 0 4px"><b>{e(v["validation"])}</b> ({_verdict_word(v)}) '
                         f'<span style="color:#444">{e(v["details"][:220])}</span></div>'
                         f'<ul style="margin:0 0 6px 18px;padding:0;font-size:12px">{checks}{more}</ul>')
        details.append(f'<div style="margin:12px 0;padding:10px 14px;border:1px solid #c8ccd1">'
                       f'<div style="font-size:15px;font-weight:700">{e(_tname(rep, t))} - {t["status"]}</div>'
                       f'{"".join(items)}</div>')
    if email:
        footer = (f'<div style="margin-top:18px;font-size:14px">Full details (checks, row counts, failures): '
                  f'<a href="{e(link)}" style="color:#0969da;font-weight:600">open the job run</a></div>'
                  if link else
                  f'<div style="margin-top:18px;font-size:13px">Full details: <code>{e(rep["results_table"])}</code> '
                  f'WHERE {e(rep["run_filter"])}</div>')
    sql = (f"SELECT table_name, validation, check_name, status, src_value, tgt_value, detail\n"
           f"FROM {rep['results_table']}\nWHERE {rep['run_filter']} AND status NOT IN ('PASS','INFO')\n"
           f"ORDER BY table_name, validation;")
    if not email:
        footer = (f'<div style="margin-top:20px;font-size:12px;color:#57606a">All checks: <code>{e(rep["results_table"])}</code>, '
                  f'example bad rows: <code>{e(rep["mismatch_table"])}</code></div>'
                  f'<pre style="background:#f6f8fa;padding:10px;font-size:12px;overflow:auto">{e(sql)}</pre>')
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>{e(title or "ETL parity report")}</title></head>
<body style="margin:0;padding:20px;background:#ffffff;color:#1f2328;{font}">
<div style="max-width:1200px">
  <div style="font-size:20px;font-weight:700">{e(title or "ETL parity report")}</div>
  {'' if email else f'<div style="font-size:14px;margin:4px 0 6px">Overall result: <b>{rep["overall"]}</b></div>'}
  {runs_html}

  <div style="{h2}">1. Run summary</div>
  <table cellspacing="0" cellpadding="0" style="border-collapse:collapse;min-width:560px">{''.join(summary)}</table>
  {'' if email else '<div style="font-size:11px;color:#57606a;margin-top:4px">A validation passes when none of its checks fails; checks within tolerance count as passed. A table passes when all its validations pass.</div>'}

  <div style="{h2}">2. Summary table{'' if email else ' (checks per table per validation)'}</div>
  <table cellspacing="0" cellpadding="0" style="border-collapse:collapse;width:100%">
    <tr>{f'<th style="{head}">Run</th>' if rep["multi"] else ''}<th style="{head}">{'Table' if email else 'Table (rows src vs tgt)'}</th>
        <th style="{head}">Result</th>{cols}</tr>
    {''.join(rows)}
  </table>

  {f'<div style="{h2}">3. Details (checks that did not pass)</div>' if details else ''}
  {''.join(details)}
  {footer}
</div></body></html>"""


def _tname(rep: dict, t: dict) -> str:
    return f"{t['table']} ({t['run']})" if rep["multi"] else t["table"]


def _verdict_word(v: dict) -> str:
    return "FAIL" if v["status"] in ("FAIL", "ERROR") else "PASS, within tolerance"


# ------------------------------------------------------------------ link to the job run (Databricks)
def job_run_link(spark=None) -> Optional[str]:
    """
    URL of the Databricks page that shows this notebook's output:
      in a workflow  : https://<workspace>/jobs/<job_id>/runs/<task run id>  (this task's run: its notebook output)
      run by hand    : https://<workspace>/?o=<org>#notebook/<notebook id>     (the notebook; output not kept)
    None outside Databricks or when the context cannot be read.
    """
    try:
        import json as _json
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = spark or SparkSession.getActiveSession()
        nb = DBUtils(spark).notebook.entry_point.getDbutils().notebook().getContext()
        info: dict = {}
        for how in ("toJson", "safeToJson"):     # the layout differs between runtimes: merge every level
            try:
                ctx = _json.loads(getattr(nb, how)())
            except Exception:
                continue
            for part in (ctx, ctx.get("extraContext", {}), ctx.get("attributes", {}),
                         ctx.get("tags", {}), ctx.get("attributes", {}).get("tags", {})):
                if isinstance(part, dict):
                    info.update({k: v for k, v in part.items() if not isinstance(v, dict)})
        host = None
        try:
            host = spark.conf.get("spark.databricks.workspaceUrl")
        except Exception:
            pass
        host = (host or info.get("browserHostName") or str(info.get("api_url") or info.get("apiUrl") or "")
                .replace("https://", "")).strip().rstrip("/")
        if not host:
            return None
        org = info.get("orgId")
        q = f"?o={org}" if org else ""
        job = info.get("jobId")
        # the run of THIS task (its page shows this notebook's output); the whole job run only as a fallback
        task_run = info.get("runId") or info.get("taskRunId")
        job_run = info.get("multitaskParentRunId") or info.get("jobRunId")
        run = task_run or job_run
        if job and run:
            return f"https://{host}/jobs/{job}/runs/{run}{q}"
        nid = info.get("notebookId") or info.get("notebook_id")
        return f"https://{host}/{q}#notebook/{nid}" if nid else f"https://{host}/{q}"
    except Exception:
        return None


# ------------------------------------------------------------------ output
def write_report(html_text: str, folder: str, run_id: str) -> str:
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"parity_{run_id}.html")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html_text)
    return path


def should_send(cfg: dict, rep: dict) -> bool:
    on = str(cfg.get("send_on", "always")).lower()
    if on == "never":
        return False
    if on == "failure":
        return rep["overall"] in ("FAIL", "ERROR")
    if on == "issues":                         # any failure, or any check within tolerance
        return rep["overall"] != "PASS" or any(c["warn"] for t in rep["tables"] for c in t["cells"].values())
    return True


def send_email(cfg: dict, subject_line: str, text: str, html_text: Optional[str] = None) -> None:
    """Same approach as the team's EmailSender: SMTP relay, STARTTLS, plain + HTML alternative parts."""
    to = [a.strip() for a in str(cfg["to"]).replace(";", ",").split(",") if a.strip()]
    msg = MIMEMultipart("alternative")
    msg["From"] = cfg.get("sender", "noreply@ftr.databricks.com")
    msg["To"] = ", ".join(to)
    msg["Subject"] = subject_line
    msg.attach(MIMEText(text, "plain", "utf-8"))
    if html_text:
        msg.attach(MIMEText(html_text, "html", "utf-8"))
    server = None
    try:
        server = smtplib.SMTP(cfg.get("smtp_server", "mailrelay.ftr.com"), int(cfg.get("smtp_port", 25)), timeout=60)
        if cfg.get("starttls", True):
            server.starttls()
        server.sendmail(msg["From"], to, msg.as_string())
    finally:
        if server is not None:
            try:
                server.quit()
            except Exception:
                pass


def check_email_cfg(cfg: Optional[dict]):
    if not cfg:
        return
    unknown = set(cfg) - EMAIL_KEYS
    if unknown:
        raise ValueError(f"settings.email: unknown key(s) {sorted(unknown)}; allowed: {sorted(EMAIL_KEYS)}")
    if not cfg.get("to"):
        raise ValueError("settings.email needs 'to' (one address, or several separated by commas)")
    if str(cfg.get("send_on", "always")).lower() not in ("always", "issues", "failure", "never"):
        raise ValueError("settings.email.send_on must be always | issues | failure | never")
