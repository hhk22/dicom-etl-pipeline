#!/usr/bin/env bash
set -euo pipefail
airflow db migrate
if ! airflow users list --output json | python -c 'import json,os,sys; users=json.load(sys.stdin); sys.exit(not any(u["username"] == os.environ["AIRFLOW_ADMIN_USER"] for u in users))'; then
  airflow users create --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname Demo --lastname Admin --role Admin --email demo@example.com
fi
# Keep a user's chosen pool size when restarting Compose.
if ! airflow pools get orders_pool >/dev/null 2>&1; then
  airflow pools set orders_pool 4 'Order file ETL concurrency (1, 2 or 4)'
fi
python -c 'from commerce_etl.config import Settings; from commerce_etl.database import init_db; init_db(Settings.from_env().dsn)'
