import json
import os
import subprocess
import sys

import pytest

from commerce_etl.database import connect
from commerce_etl.faults import FaultInjection
from commerce_etl.files import run_directory
from commerce_etl.pipeline import PermanentBatchError, discover, finalize, process


def read_report(settings, run_id):
    return json.loads((run_directory(settings.root, run_id) / "report.json").read_text())


@pytest.mark.integration
def test_file_written_but_db_failure_is_retried_without_touching_other_batches(
    database_settings,
    generate,
):
    settings = database_settings
    generate(settings.root, "demo", 2, 10)
    batches = discover(settings, "retry", "demo")
    failed = batches[1]
    for batch in batches:
        if batch != failed:
            process(settings, "retry", batch)
    directory = run_directory(settings.root, "retry")
    previous = {
        b["batch_id"]: (directory / f"{b['batch_id']}.json").read_bytes()
        for b in batches
        if b != failed
    }
    fault = FaultInjection("after_file", "once")
    with pytest.raises(RuntimeError, match="OperationalError"):
        process(settings, "retry", failed, fault)
    assert (settings.root / "normalized" / f"{failed['batch_id']}.jsonl").exists()
    with connect(settings.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM orders")
        assert cur.fetchone()["n"] == 30
        cur.execute("SELECT count(*) AS n FROM batch_rows WHERE batch_id=%s", (failed["batch_id"],))
        assert cur.fetchone()["n"] == 0
    with pytest.raises(RuntimeError):
        finalize(settings, "retry")
    assert read_report(settings, "retry")["status"] == "failed"
    process(settings, "retry", failed, fault)
    assert finalize(settings, "retry")["unique_orders"] == 40
    assert len(list((settings.root / "normalized").glob("*.jsonl"))) == 4
    for bid, body in previous.items():
        assert (directory / f"{bid}.json").read_bytes() == body


@pytest.mark.integration
def test_hard_exit_after_commit_recovers_without_duplicate_orders(database_settings, generate):
    settings = database_settings
    generate(settings.root, "demo", 1, 10)
    batch, sibling = discover(settings, "crash", "demo")
    process(settings, "crash", sibling)
    code = (
        "import json,sys; from commerce_etl.config import Settings; "
        "from commerce_etl.pipeline import process; "
        "from commerce_etl.faults import FaultInjection; "
        "process(Settings.from_env(),'crash',json.loads(sys.argv[1]),"
        "FaultInjection('after_commit',sys.argv[2]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, json.dumps(batch), "once"],
        env=dict(
            os.environ,
            ETL_DATA_ROOT=str(settings.root),
            ORDERS_DATABASE_URL=settings.dsn,
            CUSTOMER_TOKEN_KEY=settings.token_key,
        ),
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 75
    directory = run_directory(settings.root, "crash")
    assert json.loads((directory / f"{batch['batch_id']}.json").read_text())["status"] == "running"
    with connect(settings.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM orders")
        assert cur.fetchone()["n"] == 20
        cur.execute("SELECT status FROM batch_runs WHERE batch_id=%s", (batch["batch_id"],))
        assert cur.fetchone()["status"] == "success"
    with pytest.raises(RuntimeError):
        finalize(settings, "crash")
    assert any(
        e["reason"] == "local_db_report_mismatch" for e in read_report(settings, "crash")["errors"]
    )
    process(settings, "crash", batch, FaultInjection("after_commit", "once"))
    report = finalize(settings, "crash")
    assert report["unique_orders"] == 20
    assert report["counts"]["inserted"] == 10
    assert report["counts"]["duplicate"] == 10
    assert len(list((settings.root / "normalized").glob("*.jsonl"))) == 2


@pytest.mark.integration
def test_persistent_fault_remains_failed_after_two_attempts(database_settings, generate):
    settings = database_settings
    generate(settings.root, "demo", 1, 1)
    batches = discover(settings, "persistent", "demo")
    process(settings, "persistent", batches[0])
    fault = FaultInjection("after_file", "always")
    for _ in range(2):
        with pytest.raises(RuntimeError):
            process(settings, "persistent", batches[1], fault)
    with pytest.raises(RuntimeError):
        finalize(settings, "persistent")
    report = read_report(settings, "persistent")
    assert report["status"] == "failed"
    assert any(e.get("reason") == "batch_incomplete" for e in report["errors"])


@pytest.mark.integration
def test_all_files_failed_cannot_be_reported_as_success(database_settings, generate):
    settings = database_settings
    generate(settings.root, "demo", 1, 1)
    for batch in discover(settings, "all-failed", "demo"):
        with pytest.raises(RuntimeError):
            process(settings, "all-failed", batch, FaultInjection("after_file", "always"))
    with pytest.raises(RuntimeError):
        finalize(settings, "all-failed")
    report = read_report(settings, "all-failed")
    assert report["unique_orders"] == 0
    assert sum(e.get("reason") == "batch_incomplete" for e in report["errors"]) == 2


@pytest.mark.integration
def test_empty_input_leaves_failed_report(database_settings):
    settings = database_settings
    with pytest.raises(ValueError, match="no_input_files"):
        discover(settings, "empty", "missing")
    with pytest.raises(RuntimeError):
        finalize(settings, "empty")
    assert read_report(settings, "empty")["status"] == "failed"


@pytest.mark.integration
def test_unreadable_file_is_permanent_and_keeps_a_safe_failure_record(database_settings):
    settings = database_settings
    path = settings.root / "input/demo/shop_b/bad.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"email":"private@example.com",')
    batch = discover(settings, "parse", "demo")[0]
    with pytest.raises(PermanentBatchError):
        process(settings, "parse", batch)
    local = (run_directory(settings.root, "parse") / f"{batch['batch_id']}.json").read_text()
    assert "private@example.com" not in local
    assert json.loads(local)["retryable"] is False
    assert json.loads(local)["reason"] == "unreadable_file"
    with pytest.raises(RuntimeError):
        finalize(settings, "parse")


def test_fault_markers_are_scoped_to_run_and_batch(settings):
    from psycopg2 import OperationalError

    fault = FaultInjection("after_file", "once")
    with pytest.raises(OperationalError):
        fault.check(settings.root, "run-a", "batch-a", "after_file")
    fault.check(settings.root, "run-a", "batch-a", "after_file")
    with pytest.raises(OperationalError):
        fault.check(settings.root, "run-a", "batch-b", "after_file")
    with pytest.raises(OperationalError):
        fault.check(settings.root, "run-b", "batch-a", "after_file")
