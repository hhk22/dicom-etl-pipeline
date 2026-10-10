"""Trigger and verify a real scheduled DAG run; this is a smoke check, not a benchmark.

Run from the repository root: python3 scripts/run_demo.py --slots 4 --dataset demo
"""

import argparse
import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def config():
    values = {}
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    values.update(os.environ)
    return values


def overlap(intervals):
    events = []
    for start, end in intervals:
        if start and end:
            events.extend([(datetime.fromisoformat(start), 1), (datetime.fromisoformat(end), -1)])
    active = peak = 0
    for _, change in sorted(events):
        active += change
        peak = max(peak, active)
    return peak


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--slots", type=int, choices=(1, 2, 4), default=4)
    p.add_argument("--dataset", default="demo")
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    cfg = config()
    origin = "http://127.0.0.1:" + cfg.get("AIRFLOW_PORT", "8085")
    credentials = (
        cfg.get("AIRFLOW_ADMIN_USER", "admin") + ":" + cfg.get("AIRFLOW_ADMIN_PASSWORD", "admin")
    )
    auth = base64.b64encode(credentials.encode()).decode()

    def api(path, method="GET", data=None):
        req = urllib.request.Request(
            origin + "/api/v1" + path,
            method=method,
            headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"},
            data=json.dumps(data).encode() if data is not None else None,
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)

    # Avoid changing concurrency under an existing run.
    for state in ("queued", "running"):
        existing = api("/dags/orders_etl/dagRuns?state=" + state)
        if existing["total_entries"]:
            raise SystemExit("orders_etl already has a queued/running run; wait for it to finish")
    api("/pools/orders_pool", "PATCH", {"name": "orders_pool", "slots": args.slots})
    api("/dags/orders_etl", "PATCH", {"is_paused": False})
    run_id = "v1_slots_" + str(args.slots) + "_" + uuid.uuid4().hex[:12]
    api(
        "/dags/orders_etl/dagRuns",
        "POST",
        {"dag_run_id": run_id, "conf": {"dataset": args.dataset}},
    )
    path = "/dags/orders_etl/dagRuns/" + urllib.parse.quote(run_id, safe="")
    deadline = time.monotonic() + args.timeout
    last_state = None
    while True:
        run = api(path)
        if run["state"] != last_state:
            print(f"{run_id}: {run['state']}", flush=True)
            last_state = run["state"]
        if run["state"] in ("success", "failed"):
            break
        if time.monotonic() >= deadline:
            raise SystemExit("Timeout; run is still available in the Airflow UI")
        time.sleep(2)
    tasks = api(path + "/taskInstances?limit=500")["task_instances"]
    mapped = [t for t in tasks if t["task_id"] == "process_batch"]
    command = (
        "import json,sys; from pathlib import Path; "
        "from commerce_etl.files import run_directory; "
        "p=run_directory(Path('/opt/airflow/data'),sys.argv[1])/'report.json'; "
        "print(p.read_text() if p.exists() else '{}')"
    )
    completed = subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-scheduler", "python", "-c", command, run_id],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    report = json.loads(completed.stdout)
    app_peak = overlap([(b["started_at"], b["finished_at"]) for b in report.get("batches", [])])
    result = {
        "kind": "v1_functional_smoke_not_benchmark",
        "slots": args.slots,
        "run_id": run_id,
        "dataset": args.dataset,
        "state": run["state"],
        "start_date": run["start_date"],
        "end_date": run["end_date"],
        "mapped_tasks": len(mapped),
        "peak_task_overlap": overlap([(t["start_date"], t["end_date"]) for t in mapped]),
        "peak_processing_overlap": app_peak,
        "tasks": [
            {
                k: t[k]
                for k in ("task_id", "map_index", "state", "start_date", "end_date", "try_number")
            }
            for t in tasks
        ],
        "verification": report,
    }
    output = args.output or ROOT / ".local" / f"{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Report: {output}")
    print(f"Mapped tasks: {len(mapped)}, peak application overlap: {app_peak}")
    print(f"Verified orders: {report.get('unique_orders')}, counts: {report.get('counts')}")
    if run["state"] != "success" or report.get("status") != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
