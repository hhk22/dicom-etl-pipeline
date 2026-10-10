"""Business data only; the Airflow metadata database is separate."""

from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

from commerce_etl.files import canonical, digest

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
 source TEXT NOT NULL, order_id TEXT NOT NULL,
 payload JSONB NOT NULL, payload_hash TEXT NOT NULL,
 PRIMARY KEY (source, order_id)
);
CREATE TABLE IF NOT EXISTS batch_runs (
 run_id TEXT NOT NULL, batch_id TEXT NOT NULL,
 status TEXT NOT NULL, summary JSONB NOT NULL,
 PRIMARY KEY (run_id, batch_id)
);
CREATE TABLE IF NOT EXISTS batch_rows (
 run_id TEXT NOT NULL, batch_id TEXT NOT NULL, row_number INTEGER NOT NULL,
 outcome TEXT NOT NULL, payload JSONB, reason TEXT,
 PRIMARY KEY (run_id, batch_id, row_number)
);
"""


@contextmanager
def connect(dsn):
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db(dsn):
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA)


def save_summary(cur, run_id, batch_id, status, summary):
    cur.execute(
        """INSERT INTO batch_runs VALUES (%s,%s,%s,%s)
        ON CONFLICT (run_id,batch_id) DO UPDATE
        SET status=EXCLUDED.status, summary=EXCLUDED.summary""",
        (run_id, batch_id, status, Json(summary)),
    )


def store_orders(cur, records: list[tuple[int, dict]]) -> list[dict]:
    """Lock keys in sorted order; insert once, compare the winning value on conflict.

    INSERT .. ON CONFLICT protects concurrent batches too. READ COMMITTED's next
    SELECT sees the row whose insert we waited for. No blind overwrites.
    """
    first = {}
    for _, order in records:
        first.setdefault((order["source"], order["order_id"]), order)
    if not first:
        return []
    values = [
        (source, oid, Json(first[(source, oid)]), digest(canonical(first[(source, oid)]).encode()))
        for source, oid in sorted(first)
    ]
    inserted = execute_values(
        cur,
        """INSERT INTO orders (source, order_id, payload, payload_hash) VALUES %s
        ON CONFLICT DO NOTHING RETURNING source,order_id""",
        values,
        page_size=1000,
        fetch=True,
    )
    new_keys = {(r["source"], r["order_id"]) for r in inserted}
    stored = execute_values(
        cur,
        """SELECT o.source,o.order_id,o.payload FROM orders o
        JOIN (VALUES %s) AS k(source,order_id)
        ON (o.source,o.order_id)=(k.source,k.order_id)""",
        sorted(first),
        page_size=1000,
        fetch=True,
    )
    actual = {(r["source"], r["order_id"]): r["payload"] for r in stored}
    results = []
    for number, order in records:
        key = order["source"], order["order_id"]
        outcome = "conflict" if actual[key] != order else "duplicate"
        if outcome != "conflict" and key in new_keys:
            outcome = "inserted"
            new_keys.remove(key)
        results.append({"row_number": number, "outcome": outcome, "payload": order})
    return results
