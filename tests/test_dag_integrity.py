import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = "/opt/airflow/dags"


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder=DAGS_FOLDER)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_weather_pipeline_order(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    assert [task.task_id for task in dag.topological_sort()] == [
        "ingest_open_meteo",
        "register_bronze_catalog",
        "dbt_build",
        "validate_gold",
    ]


def test_weather_pipeline_runs_one_at_a_time(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_containers_run_on_platform_network(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    for task_id in ("ingest_open_meteo", "register_bronze_catalog", "dbt_build"):
        task = dag.get_task(task_id)
        assert task.network_mode == "bigdata"
        assert task.auto_remove == "force"


def test_every_step_processes_the_previous_day(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    assert "macros.ds_add(ds, -1)" in " ".join(dag.get_task("ingest_open_meteo").command)
    assert "macros.ds_add(ds, -1)" in " ".join(dag.get_task("dbt_build").command)
    assert "macros.ds_add(ds, -1)" in dag.get_task("validate_gold").sql
