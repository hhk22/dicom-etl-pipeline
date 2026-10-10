from pathlib import Path

import pytest

pytestmark = pytest.mark.airflow


def test_dag_graph_and_failure_leaf():
    pytest.importorskip("airflow")
    from airflow.models import DagBag

    bag = DagBag(str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert bag.import_errors == {}
    dag = bag.get_dag("orders_etl")
    assert set(dag.task_ids) == {"discover_batches", "process_batch", "finalize_run"}
    assert dag.get_task("process_batch").pool == "orders_pool"
    assert dag.get_task("process_batch").upstream_task_ids == {"discover_batches"}
    assert dag.get_task("finalize_run").upstream_task_ids == {"discover_batches", "process_batch"}
    assert dag.get_task("finalize_run").trigger_rule == "all_done"
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 4
