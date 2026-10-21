"""
Configuration: the YAML file, environments, ${PARAM} placeholders and the per-table spec.

The YAML says WHAT to compare; nothing in here touches data.
"""
from __future__ import annotations

import os
import string
from dataclasses import dataclass, field, fields
from typing import Optional

import yaml


# --------------------------------------------------------------------------- connections
@dataclass
class Connection:
    """
    One side of the comparison.
      type: oracle  -> url, user, schema, password | password_env | password_secret
      type: table   -> name: a template such as "it_bronze_dev.test_only.{table}_src"
    """
    type: str = "oracle"
    # oracle
    url: Optional[str] = None
    user: Optional[str] = None
    schema: Optional[str] = None
    password: Optional[str] = None
    password_env: Optional[str] = None            # environment variable holding the password
    password_secret: Optional[dict] = None        # Databricks secret: {scope: ..., key: ...}
    fetchsize: int = 10_000
    properties: dict = field(default_factory=dict)  # extra JDBC properties (wallet, TLS, Kerberos, ...)
    # table (Databricks / Spark catalog)
    name: Optional[str] = None

    def __post_init__(self):
        self.type = self.type.lower()
        if self.type == "oracle":
            missing = [f for f in ("url", "user", "schema") if not getattr(self, f)]
            if missing:
                raise ValueError(f"oracle connection needs {missing}")
        elif self.type == "table":
            if not self.name or "{table}" not in self.name:
                raise ValueError('table connection needs name with a {table} placeholder, '
                                 'e.g. "it_bronze_dev.test_only.{table}_src"')
        else:
            raise ValueError(f"connection type must be 'oracle' or 'table', got {self.type!r}")

    def secret(self, spark=None) -> Optional[str]:
        if self.password:
            return self.password
        if self.password_env:
            return os.environ[self.password_env]
        if self.password_secret:
            from pyspark.dbutils import DBUtils          # only available on Databricks
            return DBUtils(spark).secrets.get(self.password_secret["scope"], self.password_secret["key"])
        return None


# --------------------------------------------------------------------------- settings
TS_UNITS = {"year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond", "microsecond"}


@dataclass
class Settings:
    sample_threshold_rows: int = 500_000      # tables with more rows than this are sampled
    sample_pct: float = 10.0                  # sample this % of the rows ...
    max_sample_rows: int = 100_000            # ... but at most this many rows per side
    sample_seed: int = 42                     # same seed = same sample on every run
    read_partitions: int = 16                 # parallel slices for full reads from Oracle
    count_tolerance_pct: float = 0.0          # allowed row-count difference in %
    row_mismatch_tolerance_pct: float = 0.0   # % of matched rows allowed to differ in value
    mismatch_sample_limit: int = 50           # example bad rows kept per table per problem
    parallel_tables: int = 1                  # tables validated at the same time (threads in one Spark session)
    report_path: Optional[str] = None         # folder for the HTML run report (parity_<run_id>.html), e.g. /Workspace/...
    email: Optional[dict] = None              # e-mail the report: {to, send_on, smtp_server, smtp_port, sender, ...}
    ts_truncate: Optional[str] = None         # every table: ignore timestamp differences below this unit
                                              #   (second | minute | ...); a table's ts_truncate overrides it
    # incremental tolerances (a table's incremental block can override each one); within tolerance = WARN, not FAIL
    delta_count_tolerance_pct: float = 0.0    # |source delta rows - target delta rows| / source delta rows
    delta_key_tolerance_pct: float = 0.0      # (missed + extra keys) / source delta keys
    delta_value_tolerance_pct: Optional[float] = None   # differing rows / matched rows (default: row_mismatch_tolerance_pct)
    # column distributions (text, date, timestamp columns)
    distributions: bool = True                # compare value distributions of text / date / timestamp columns
    distribution_max_categories: int = 1000   # text columns with up to this many distinct values: counts per value;
                                              #   more distinct values: counts per text length
    distribution_tolerance_pct: float = 0.0   # a bucket's count may differ by this % of the bucket
    distribution_tolerance_rows: int = 0      # ... or by this many rows (a bucket passes within EITHER limit)
    keyless_method: str = "fingerprint"       # big tables without keys: fingerprint (every row) | sample (v2 behaviour)
    fingerprint_buckets: int = 0              # whole-table fingerprints: number of buckets (0 = auto: ~2,000 rows each,
                                              #   between 1,000 and 100,000 buckets)
    max_detail_rows: int = 1_000_000          # rows (per side) of differing buckets compared row by row; beyond that the
                                              #   remaining differing buckets are only counted
    results_table: Optional[str] = None       # e.g. it_bronze_dev.test_only.parity_results
    mismatch_table: Optional[str] = None      # e.g. it_bronze_dev.test_only.parity_mismatches

    def __post_init__(self):
        if not self.results_table or not self.mismatch_table:
            raise ValueError("settings need results_table and mismatch_table "
                             "(e.g. it_bronze_dev.test_only.parity_results / parity_mismatches)")
        if self.keyless_method not in ("fingerprint", "sample"):
            raise ValueError("keyless_method must be 'fingerprint' or 'sample'")
        if self.ts_truncate is not None and str(self.ts_truncate).lower() not in TS_UNITS:
            raise ValueError(f"settings.ts_truncate must be one of {sorted(TS_UNITS)}, got {self.ts_truncate!r}")
        from .notify import check_email_cfg
        check_email_cfg(self.email)
        if self.parallel_tables < 1:
            raise ValueError("parallel_tables must be 1 or more")
        if not 0 < self.sample_pct <= 100:
            raise ValueError("sample_pct must be between 0 and 100")


# --------------------------------------------------------------------------- tables
@dataclass
class Incremental:
    source_delta: str                         # condition: rows the source (existing) ETL run inserted/updated
    target_delta: str                         # condition: rows the target (new) ETL run inserted/updated
    delta_only: bool = False                  # true = only the incremental checks (+ schema)
    count_tolerance_pct: Optional[float] = None   # delta row counts may differ by this % (default: settings)
    key_tolerance_pct: Optional[float] = None     # missed + extra delta keys up to this % of the source delta keys
    value_tolerance_pct: Optional[float] = None   # delta rows with different values up to this % of matched rows


@dataclass
class TableSpec:
    name: str
    keys: list = field(default_factory=list)   # empty = no grain: whole-row comparison
    exclude_cols: list = field(default_factory=list)
    filter: Optional[str] = None              # same condition on both sides
    num_tolerance: float = 0.0                # numbers may differ by up to this much
    ts_truncate: Optional[str] = None         # "second", "minute", ...; null = settings.ts_truncate; "none" = off
    force_full: bool = False                  # never sample this table
    target_name: Optional[str] = None         # table name on the target side, if different
    row_mismatch_tolerance_pct: Optional[float] = None   # overrides the global setting
    incremental: Optional[Incremental] = None
    distribution_tolerance_pct: Optional[float] = None   # overrides the setting for this table
    distribution_tolerance_rows: Optional[int] = None    # overrides the setting for this table

    def __post_init__(self):
        self.name = self.name.upper()
        self.target_name = (self.target_name or self.name).upper()
        self.keys = [k.upper() for k in (self.keys or [])]
        self.exclude_cols = [c.upper() for c in self.exclude_cols]
        if self.ts_truncate is not None and str(self.ts_truncate).lower() not in TS_UNITS | {"none"}:
            raise ValueError(f"{self.name}: ts_truncate must be one of {sorted(TS_UNITS)} or none, got {self.ts_truncate!r}")
        overlap = set(self.keys) & set(self.exclude_cols)
        if overlap:
            raise ValueError(f"{self.name}: key columns cannot be excluded: {sorted(overlap)}")


# --------------------------------------------------------------------------- loading
def fill_params(text: Optional[str], params: dict, where: str) -> Optional[str]:
    """Replace ${NAME} placeholders (run windows, batch ids) with values from params or env vars."""
    if text is None:
        return None
    try:
        return string.Template(text).substitute(params)
    except KeyError as e:
        raise ValueError(f"{where} uses ${{{e.args[0]}}} but no value was given: pass it in params "
                         f"(or --param {e.args[0]}=...) or set an environment variable") from None


def pick_env(value, env: Optional[str], where: str):
    """A setting is one value for every environment, or {env_name: value, ...}."""
    if isinstance(value, dict):
        if env not in value:
            raise ValueError(f"{where} has no value for environment {env!r} (has: {sorted(value)})")
        return value[env]
    return value


def _check_keys(cls, d: dict, where: str):
    allowed = {f.name for f in fields(cls)}
    unknown = set(d) - allowed
    if unknown:
        raise ValueError(f"{where}: unknown setting(s) {sorted(unknown)}; allowed: {sorted(allowed)}")


def build_spec(tcfg: dict, params: dict, env: Optional[str]) -> TableSpec:
    tcfg = dict(tcfg)
    tcfg.pop("envs", None)
    label = str(tcfg.get("name", "?"))
    if "scd2" in tcfg:
        raise ValueError(f"{label}: scd2 checks are not part of this version yet; remove the scd2 block")
    _check_keys(TableSpec, tcfg, f"table {label}")
    tcfg["filter"] = fill_params(pick_env(tcfg.get("filter"), env, f"{label}.filter"), params, f"{label}.filter")
    if tcfg.get("incremental"):
        inc = dict(tcfg["incremental"])
        _check_keys(Incremental, inc, f"table {label}.incremental")
        for k in ("source_delta", "target_delta"):
            where = f"{label}.incremental.{k}"
            inc[k] = pick_env(inc.get(k), env, where)
            if not inc[k]:
                raise ValueError(f"{label}.incremental needs {k}")
            inc[k] = fill_params(inc[k], params, where)
        tcfg["incremental"] = Incremental(**inc)
    return TableSpec(**tcfg)


@dataclass
class RunConfig:
    env: Optional[str]
    source: Connection
    target: Connection
    settings: Settings
    tables: list          # raw table dicts; turned into TableSpec one by one so one bad entry fails one table


def load_config(path: str, env: Optional[str] = None) -> RunConfig:
    with open(path) as fh:
        cfg = yaml.safe_load(fh) or {}
    settings = dict(cfg.get("settings") or {})
    envs = cfg.get("environments")
    if envs:
        env = env or cfg.get("default_env")
        if not env:
            raise ValueError(f"choose an environment ({', '.join(envs)}) or set default_env in the config")
        if env not in envs:
            raise ValueError(f"unknown environment {env!r}; the config defines: {', '.join(envs)}")
        e = envs[env] or {}
        src, tgt = e.get("source"), e.get("target")
        settings.update(e.get("settings") or {})
    else:
        if env:
            raise ValueError(f"environment {env!r} given, but the config has no 'environments:' block")
        src, tgt = cfg.get("source"), cfg.get("target")
    if not (src and tgt):
        raise ValueError(f"config needs a source and a target connection{f' for environment {env}' if env else ''}")
    _check_keys(Settings, settings, "settings")
    source, target = Connection(**src), Connection(**tgt)
    if source.type != target.type:
        raise ValueError("source and target must be the same type (both oracle or both table)")
    tables = [t for t in (cfg.get("tables") or [])
              if not (env and t.get("envs") and env not in t["envs"])]
    return RunConfig(env, source, target, Settings(**settings), tables)
