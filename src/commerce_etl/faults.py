"""Explicit demo faults at the file/DB boundary; disabled unless requested by the DAG run."""

import os
from dataclasses import dataclass
from pathlib import Path

from psycopg2 import OperationalError

from commerce_etl.files import run_directory


@dataclass(frozen=True)
class FaultInjection:
    point: str = "none"
    mode: str = "once"

    def __post_init__(self):
        if self.point not in {"none", "after_file", "after_commit"}:
            raise ValueError("invalid_fault_point")
        if self.mode not in {"once", "always"}:
            raise ValueError("invalid_fault_mode")

    def check(self, root: Path, run_id: str, batch_id: str, point: str):
        if self.point != point:
            return
        directory = run_directory(root, run_id) / "faults"
        directory.mkdir(parents=True, exist_ok=True)
        if self.mode == "once":
            # One fault per run/batch/point, even across task processes and retries.
            try:
                with (directory / f"{batch_id}-{point}").open("x"):
                    pass
            except FileExistsError:
                return
        if point == "after_file":
            raise OperationalError("injected_db_failure_after_file")
        # A hard exit deliberately bypasses except/finally and local success reporting.
        os._exit(75)
