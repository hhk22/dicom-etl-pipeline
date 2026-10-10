"""One immutable input file per mapped task; the final leaf fails on incomplete runs."""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="orders_etl",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    params={"dataset": Param("demo", type="string", pattern="^[a-z0-9_-]+$")},
    tags=["commerce", "v1", "parallel-etl"],
    default_args={"owner": "portfolio", "retries": 1, "retry_delay": timedelta(seconds=15)},
    doc_md="File-level mapping. Change orders_pool slots to 1/2/4. See README.md.",
)
def orders_etl():
    @task(show_return_value_in_logs=False)
    def discover_batches():
        from commerce_etl.config import Settings
        from commerce_etl.pipeline import discover

        ctx = get_current_context()
        return discover(Settings.from_env(), ctx["run_id"], ctx["params"]["dataset"])

    @task(
        pool="orders_pool", pool_slots=1, max_active_tis_per_dag=4, show_return_value_in_logs=False
    )
    def process_batch(batch: dict):
        from commerce_etl.config import Settings
        from commerce_etl.pipeline import process

        return process(Settings.from_env(), get_current_context()["run_id"], batch)

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0, show_return_value_in_logs=False)
    def finalize_run():
        from commerce_etl.config import Settings
        from commerce_etl.pipeline import finalize

        ctx = get_current_context()
        failures = [
            {"task_id": ti.task_id, "map_index": ti.map_index, "state": ti.state}
            for ti in ctx["dag_run"].get_task_instances()
            if ti.task_id != "finalize_run" and ti.state != "success"
        ]
        return finalize(Settings.from_env(), ctx["run_id"], failures)

    batches = discover_batches()
    mapped = process_batch.expand(batch=batches)
    finish = finalize_run()
    batches >> finish
    mapped >> finish


orders_etl()
