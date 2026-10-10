"""Discover immutable inputs, process one file transactionally, and verify a whole run."""

import json
import re
import time
from collections import Counter
from datetime import datetime, timezone

from psycopg2.extras import Json, execute_values

from commerce_etl.config import Settings
from commerce_etl.database import connect, save_summary, store_orders
from commerce_etl.files import atomic_write, canonical, digest, run_directory, write_json
from commerce_etl.transform import InvalidOrder, normalize, parse_rows

TRANSFORM_VERSION = "v1"


def now():
    return datetime.now(timezone.utc).isoformat()


def discover(settings: Settings, run_id: str, dataset: str) -> list[dict]:
    if not re.fullmatch(r"[a-z0-9_-]+", dataset):
        raise ValueError("invalid_dataset")
    root = settings.root
    manifest = run_directory(root, run_id) / "manifest.json"
    token_version = digest(settings.token_key.encode())[:16]
    if manifest.exists():
        previous = json.loads(manifest.read_text())
        if previous["dataset"] != dataset or previous["token_version"] != token_version:
            raise ValueError("run_configuration_changed")
        return previous["batches"]
    input_root = root / "input" / dataset
    batches = []
    for source, suffix in (("shop_a", ".csv"), ("shop_b", ".json")):
        for path in sorted((input_root / source).glob("*")):
            if not path.is_file() or path.suffix != suffix:
                raise ValueError("unexpected_input_file")
            data = path.read_bytes()
            sha = digest(data)
            batch_id = digest(canonical([TRANSFORM_VERSION, token_version, source, sha]).encode())
            # Identical files in one run have one task, not racing writers.
            if any(b["batch_id"] == batch_id for b in batches):
                continue
            archived = root / "raw" / source / (sha + suffix)
            atomic_write(archived, data)
            batches.append(
                {
                    "batch_id": batch_id,
                    "source": source,
                    "input_hash": sha,
                    "raw_path": str(archived.relative_to(root)),
                    "bytes": len(data),
                }
            )
    if not batches:
        raise ValueError("no_input_files")
    if len(batches) > 200:
        raise ValueError("too_many_files_max_200")
    write_json(
        manifest,
        {
            "run_id": run_id,
            "dataset": dataset,
            "token_version": token_version,
            "transform_version": TRANSFORM_VERSION,
            "batches": batches,
        },
    )
    return batches


def process(settings: Settings, run_id: str, batch: dict) -> dict:
    batch_id = batch["batch_id"]
    report = run_directory(settings.root, run_id) / f"{batch_id}.json"
    summary = {"batch_id": batch_id, "status": "running", "started_at": now()}
    write_json(report, summary)
    start = time.monotonic()
    try:
        data = (settings.root / batch["raw_path"]).read_bytes()
        if digest(data) != batch["input_hash"]:
            raise ValueError("input_hash_mismatch")
        source_rows = parse_rows(batch["source"], data)
        valid, rejected = [], []
        for number, row in enumerate(source_rows, 1):
            try:
                valid.append((number, normalize(batch["source"], row, settings.token_key)))
            except InvalidOrder as exc:
                rejected.append({"row_number": number, "outcome": "invalid", "reason": str(exc)})
        # The normalized artifact contains only allowlisted fields, including conflict candidates.
        output = settings.root / "normalized" / f"{batch_id}.jsonl"
        body = "".join(canonical(order) + "\n" for _, order in valid).encode()
        with connect(settings.dsn) as conn, conn.cursor() as cur:
            # Same batch cannot write its output concurrently; different files still run in parallel.
            lock_key = int(batch_id[:16], 16)
            if lock_key >= 2**63:
                lock_key -= 2**64
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
            atomic_write(output, body)
            db_start = time.monotonic()
            outcomes = store_orders(cur, valid) + rejected
            cur.execute(
                "DELETE FROM batch_rows WHERE run_id=%s AND batch_id=%s", (run_id, batch_id)
            )
            if outcomes:
                execute_values(
                    cur,
                    "INSERT INTO batch_rows VALUES %s",
                    [
                        (
                            run_id,
                            batch_id,
                            r["row_number"],
                            r["outcome"],
                            Json(r["payload"]) if "payload" in r else None,
                            r.get("reason"),
                        )
                        for r in outcomes
                    ],
                    page_size=1000,
                )
            counts = {key: 0 for key in ("inserted", "duplicate", "conflict", "invalid")}
            counts.update(Counter(r["outcome"] for r in outcomes))
            summary.update(
                status="success",
                rows=len(source_rows),
                counts=counts,
                output_path=str(output.relative_to(settings.root)),
                output_hash=digest(body),
                db_seconds=round(time.monotonic() - db_start, 6),
                finished_at=now(),
                processing_seconds=round(time.monotonic() - start, 6),
            )
            save_summary(cur, run_id, batch_id, "success", summary)
        write_json(report, summary)
        # XCom contains no rows or PII.
        return {"batch_id": batch_id, "rows": len(source_rows), "counts": counts}
    except Exception as exc:
        # A failed retry must not leave a previous success in the run report.
        summary.update(status="failed", error_type=type(exc).__name__, finished_at=now())
        write_json(report, summary)
        try:
            with connect(settings.dsn) as conn, conn.cursor() as cur:
                save_summary(cur, run_id, batch_id, "failed", summary)
        except Exception:
            pass  # The local report survives an unavailable business DB.
        raise RuntimeError(f"batch_failed:{batch_id}:{type(exc).__name__}") from None


def finalize(settings: Settings, run_id: str, task_failures: list[dict] | None = None) -> dict:
    directory = run_directory(settings.root, run_id)
    errors = list(task_failures or [])
    totals = Counter()
    batches = []
    expected = {}
    try:
        manifest = json.loads((directory / "manifest.json").read_text())
        with connect(settings.dsn) as conn, conn.cursor() as cur:
            for batch in manifest["batches"]:
                bid = batch["batch_id"]
                cur.execute(
                    "SELECT status,summary FROM batch_runs WHERE run_id=%s AND batch_id=%s",
                    (run_id, bid),
                )
                state = cur.fetchone()
                if not state or state["status"] != "success":
                    errors.append({"batch_id": bid, "reason": "batch_incomplete"})
                    continue
                summary = state["summary"]
                local = json.loads((directory / f"{bid}.json").read_text())
                if local != summary:
                    errors.append({"batch_id": bid, "reason": "local_db_report_mismatch"})
                cur.execute(
                    """SELECT row_number,outcome,payload FROM batch_rows
                               WHERE run_id=%s AND batch_id=%s ORDER BY row_number""",
                    (run_id, bid),
                )
                rows = cur.fetchall()
                row_counts = Counter(r["outcome"] for r in rows)
                if len(rows) != summary["rows"] or any(
                    row_counts[k] != summary["counts"][k] for k in summary["counts"]
                ):
                    errors.append({"batch_id": bid, "reason": "row_count_mismatch"})
                artifact = (settings.root / summary["output_path"]).read_bytes()
                desired = "".join(
                    canonical(r["payload"]) + "\n" for r in rows if r["payload"] is not None
                ).encode()
                if digest(artifact) != summary["output_hash"] or artifact != desired:
                    errors.append({"batch_id": bid, "reason": "artifact_mismatch"})
                for row in rows:
                    if row["outcome"] in ("inserted", "duplicate"):
                        payload = row["payload"]
                        key = payload["source"], payload["order_id"]
                        if key in expected and expected[key] != payload:
                            errors.append({"reason": "accepted_payload_conflict"})
                        expected[key] = payload
                totals.update(summary["counts"])
                batches.append(summary)
            if expected:
                actual = execute_values(
                    cur,
                    """SELECT o.source,o.order_id,o.payload FROM orders o
                    JOIN (VALUES %s) AS k(source,order_id)
                    ON (o.source,o.order_id)=(k.source,k.order_id)""",
                    sorted(expected),
                    page_size=1000,
                    fetch=True,
                )
                observed = {(r["source"], r["order_id"]): r["payload"] for r in actual}
                if observed != expected:
                    errors.append({"reason": "orders_mismatch"})
        if totals["invalid"] or totals["conflict"]:
            errors.append({"reason": "rejected_orders_require_review"})
    except Exception as exc:
        errors.append({"reason": "verification_unavailable", "error_type": type(exc).__name__})
    report = {
        "run_id": run_id,
        "status": "failed" if errors else "success",
        "counts": dict(totals),
        "unique_orders": len(expected),
        "amount_krw": sum(r["amount_krw"] for r in expected.values()),
        "batches": batches,
        "errors": errors,
        "verified_at": now(),
    }
    write_json(directory / "report.json", report)
    if errors:
        raise RuntimeError(f"run_verification_failed: see {directory / 'report.json'}")
    return {k: v for k, v in report.items() if k != "batches"}
