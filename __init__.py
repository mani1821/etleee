"""
etl_parity: validate that a migrated ETL produces the same tables as the existing one.

    from etl_parity import run_parity
    run = run_parity("/Workspace/Shared/etl_parity/configs/tables.yaml", env="databricks")
    display(run.summary)
"""
from .keys import KeySettings, discover_keys, to_yaml
from .runner import ParityReport, ParityRun, main, run_parity, send_report

__version__ = "3.7.0"
__all__ = ["run_parity", "send_report", "ParityReport", "main", "ParityRun", "discover_keys", "to_yaml", "KeySettings", "__version__"]
