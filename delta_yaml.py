"""
Build today's incremental YAML from a base YAML (keys, excludes, settings) and a watermark query.

    from etl_parity import build_delta_yaml, run_parity
    path = build_delta_yaml(
        base_config="/Workspace/.../configs/icd_full_rerun.yaml",      # keys / exclude_cols / settings come from here
        watermarks="SELECT table_name, incr_column, watermark FROM it_bronze_dev.test_only.load_watermarks",
        out_path="/Workspace/.../configs/icd_delta_generated.yaml")   # written fresh on every run
    delta = run_parity(path, env="databricks")

watermarks: a SQL query, a DataFrame or a list of tuples. Columns, by position:
    1. table name   2. incremental column   3. watermark (timestamp / date / string)
  optional, by name: target_column, target_watermark (when the target table uses another column / value)
Window per table:  <column> BETWEEN '<watermark date> 00:00:00' AND '<watermark>'   (start= changes the start)
Tables of the base YAML without a watermark row are left out; watermark rows without a base table are reported.
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Optional

import yaml


def _fmt(v) -> str:
    """Watermark as text Spark SQL compares with a timestamp column: 2026-10-21 10:24:22.858"""
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ", timespec="milliseconds") if v.microsecond else v.isoformat(sep=" ", timespec="seconds")
    if isinstance(v, dt.date):
        return f"{v.isoformat()} 23:59:59"
    v = str(v).strip()
    return f"{v} 23:59:59" if len(v) == 10 else v              # a date: up to the end of that day


def _day(wm: str) -> str:
    return wm[:10]


def _window(col: str, wm: str, start: Optional[str]) -> str:
    begin = start or f"{_day(wm)} 00:00:00"
    return f"{col} BETWEEN '{begin}' AND '{wm}'"


def _rows(watermarks, spark):
    """[(table, column, watermark, target_column, target_watermark)] from a query / DataFrame / list."""
    if isinstance(watermarks, str):
        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        watermarks = spark.sql(watermarks)
    if hasattr(watermarks, "collect"):                       # a Spark DataFrame
        # timestamps as text INSIDE Spark (session time zone, like the filter that uses them): collecting them as
        # Python datetimes would shift them to the driver's local time zone
        from pyspark.sql import functions as F
        from pyspark.sql import types as T
        timeish = (T.TimestampType, T.DateType) + ((T.TimestampNTZType,) if hasattr(T, "TimestampNTZType") else ())
        watermarks = watermarks.select(*[F.col(f.name).cast("string").alias(f.name) if isinstance(f.dataType, timeish)
                                         else F.col(f.name) for f in watermarks.schema.fields])
        cols = [c.lower() for c in watermarks.columns]
        out = []
        for r in watermarks.collect():
            d = dict(zip(cols, r))
            out.append((r[0], r[1], r[2], d.get("target_column"), d.get("target_watermark")))
        return out
    return [tuple(list(r) + [None, None])[:5] for r in watermarks]


def build_delta_yaml(base_config: str, watermarks, out_path: Optional[str] = None, start: Optional[str] = None,
                     delta_only: bool = True, tables: Optional[list] = None, spark=None) -> str:
    """
    Write the incremental YAML for today's watermarks and return its path.

    base_config  YAML with settings, environments and the tables' keys / exclude_cols / tolerances
    watermarks   SQL query / DataFrame / list of (table, incremental column, watermark[, target_column, target_watermark])
    out_path     where to write it (default: next to base_config, <base>_delta.yaml); overwritten on every run
    start        window start for every table, e.g. '2026-10-20 00:00:00' (default: the watermark's day at 00:00:00)
    delta_only   true = only the incremental checks (+ schema) for these tables
    tables       only these tables (default: every base table that has a watermark)
    """
    with open(base_config) as fh:
        base = yaml.safe_load(fh) or {}
    by_name = {str(t.get("name", "")).upper(): t for t in base.get("tables", [])}
    want = {t.upper() for t in tables} if tables else None

    out_tables, used, unknown = [], set(), []
    for table, col, wm, tcol, twm in _rows(watermarks, spark):
        name = str(table).strip().upper()
        if "." in name:                                       # catalog.schema.table -> table
            name = name.rsplit(".", 1)[1]
        if want and name not in want:
            continue
        if name not in by_name:
            unknown.append(name)
            continue
        if wm is None or col is None:
            print(f"  {name}: no incremental column / watermark in the query row - skipped")
            continue
        spec = dict(by_name[name])                            # keys, exclude_cols, tolerances, target_name, ...
        inc = dict(spec.get("incremental") or {})             # keeps per-table delta tolerances from the base
        src_wm = _fmt(wm)
        tgt_wm = _fmt(twm) if twm is not None else src_wm
        inc["source_delta"] = _window(str(col), src_wm, start)
        inc["target_delta"] = _window(str(tcol or col), tgt_wm, start)
        inc["delta_only"] = delta_only
        spec["incremental"] = inc
        out_tables.append(spec)
        used.add(name)

    skipped = [n for n in by_name if n not in used and (not want or n in want)]
    out = {k: v for k, v in base.items() if k != "tables"}
    out["tables"] = out_tables
    out_path = out_path or os.path.join(os.path.dirname(base_config),
                                        os.path.splitext(os.path.basename(base_config))[0] + "_delta.yaml")
    header = (f"# GENERATED by etl_parity.build_delta_yaml on {dt.datetime.now():%Y-%m-%d %H:%M:%S} - do not edit,\n"
              f"# it is rewritten on every run. Keys / settings: {base_config}\n")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as fh:
        fh.write(header + yaml.safe_dump(out, sort_keys=False, default_flow_style=False, width=200))

    print(f"delta YAML: {out_path}  ({len(out_tables)} table(s))")
    for t in out_tables:
        print(f"  {t['name']}: {t['incremental']['source_delta']}")
    if skipped:
        print(f"  no watermark today (left out): {', '.join(skipped)}")
    if unknown:
        print(f"  watermark rows for tables not in {os.path.basename(base_config)} (no keys known, left out): "
              f"{', '.join(unknown)}")
    if not out_tables:
        raise ValueError("build_delta_yaml: no table has both a base entry and a watermark - nothing to validate")
    return out_path
