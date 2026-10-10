import importlib.util
import os
import uuid
from pathlib import Path

import pytest

from commerce_etl.config import Settings
from commerce_etl.database import connect, init_db


@pytest.fixture
def settings(tmp_path):
    return Settings(tmp_path, "not-connected", "test-key-" * 8)


@pytest.fixture
def database_settings(tmp_path):
    """Each integration test gets its own schema, never the live demo tables."""
    dsn = os.environ.get("TEST_ORDERS_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_ORDERS_DATABASE_URL is not configured")
    schema = "test_" + uuid.uuid4().hex
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
    isolated = dsn + ("&" if "?" in dsn else "?") + f"options=-csearch_path%3D{schema}"
    init_db(isolated)
    try:
        yield Settings(tmp_path, isolated, "test-key-" * 8)
    finally:
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.fixture
def generate():
    path = Path(__file__).resolve().parents[1] / "scripts/generate_orders.py"
    spec = importlib.util.spec_from_file_location("generate_orders", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate
