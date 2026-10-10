import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from commerce_etl.database import connect
from commerce_etl.files import run_directory
from commerce_etl.pipeline import discover, finalize, process

pytestmark = pytest.mark.integration


def run_all(settings, run_id, generate=None):
    if generate:
        generate(settings.root, "demo", 2, 10)
    batches = discover(settings, run_id, "demo")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda b: process(settings, run_id, b), batches))
    return finalize(settings, run_id), batches


def test_parallel_ingest_and_rerun(database_settings, generate):
    settings = database_settings
    result, _ = run_all(settings, "first", generate)
    assert result["unique_orders"] == 40
    assert result["counts"]["inserted"] == 40
    result2, _ = run_all(settings, "second")
    assert result2["counts"]["duplicate"] == 40
    assert result2["amount_krw"] == result["amount_krw"]
    with connect(settings.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM orders")
        assert cur.fetchone()["n"] == 40
    for artifact in (settings.root / "normalized").glob("*.jsonl"):
        assert "example.com" not in artifact.read_text()


def test_same_order_in_different_parallel_files(database_settings):
    settings = database_settings
    root = settings.root / "input/demo/shop_b"
    root.mkdir(parents=True)
    common = dict(orderNo="shared", buyerId="C", totalPrice=1, purchasedAt="2026-01-01T00:00:00Z")
    for i in range(4):
        (root / f"{i}.json").write_text(json.dumps([common, dict(common, orderNo=f"unique-{i}")]))
    result, _ = run_all(settings, "parallel")
    assert result["unique_orders"] == 5
    assert result["counts"]["inserted"] == 5
    assert result["counts"]["duplicate"] == 3


def test_conflicting_order_does_not_overwrite(database_settings, generate):
    settings = database_settings
    run_all(settings, "first", generate)
    path = settings.root / "input/demo/shop_b/part-000.json"
    rows = json.loads(path.read_text())
    rows[0]["totalPrice"] = 999999
    path.write_text(json.dumps(rows))
    batches = discover(settings, "conflict", "demo")
    for batch in batches:
        process(settings, "conflict", batch)
    with pytest.raises(RuntimeError, match="run_verification_failed"):
        finalize(settings, "conflict")
    with connect(settings.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT payload FROM orders WHERE source='shop_b' AND order_id='demo-00000000'")
        assert cur.fetchone()["payload"]["amount_krw"] == 1000


def test_finalizer_detects_missing_order(database_settings, generate):
    settings = database_settings
    run_all(settings, "first", generate)
    with connect(settings.dsn) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM orders WHERE source='shop_a' AND order_id='demo-00000000'")
    with pytest.raises(RuntimeError, match="run_verification_failed"):
        finalize(settings, "first")
    report = json.loads((run_directory(settings.root, "first") / "report.json").read_text())
    assert {"reason": "orders_mismatch"} in report["errors"]


def test_finalizer_detects_modified_artifact(database_settings, generate):
    settings = database_settings
    run_all(settings, "first", generate)
    next((settings.root / "normalized").glob("*.jsonl")).write_text("{}\n")
    with pytest.raises(RuntimeError, match="run_verification_failed"):
        finalize(settings, "first")


def test_invalid_row_is_recorded_without_pii(database_settings):
    settings = database_settings
    folder = settings.root / "input/demo/shop_b"
    folder.mkdir(parents=True)
    row = dict(
        orderNo="1",
        buyerId="C",
        totalPrice=-1,
        purchasedAt="2026-01-01T00:00:00Z",
        email="private@example.com",
    )
    (folder / "bad.json").write_text(json.dumps([row]))
    batch = discover(settings, "invalid", "demo")[0]
    result = process(settings, "invalid", batch)
    assert result["counts"]["invalid"] == 1
    with pytest.raises(RuntimeError):
        finalize(settings, "invalid")
    with connect(settings.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT payload,reason FROM batch_rows")
        record = cur.fetchone()
        assert record == {"payload": None, "reason": "invalid_amount"}


def test_upstream_failure_cannot_be_hidden(database_settings, generate):
    settings = database_settings
    run_all(settings, "first", generate)
    with pytest.raises(RuntimeError):
        finalize(settings, "first", [{"task_id": "process_batch", "state": "failed"}])


def test_changed_archived_input_fails_batch(database_settings, generate):
    settings = database_settings
    generate(settings.root, "demo", 1, 1)
    batch = discover(settings, "changed", "demo")[0]
    (settings.root / batch["raw_path"]).write_text("tampered")
    with pytest.raises(RuntimeError, match="batch_failed"):
        process(settings, "changed", batch)
    with pytest.raises(RuntimeError):
        finalize(settings, "changed")
