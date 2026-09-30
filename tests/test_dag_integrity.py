import importlib

import pytest
from airflow.dag_processing.dagbag import DagBag
from bigdata_pipeline import config

DAGS_FOLDER = "/opt/airflow/dags"
TARGET_DATE = "{{ macros.ds_add(ds, -1) }}"


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder=DAGS_FOLDER)


@pytest.fixture
def multi_source_dag(monkeypatch):
    """A DAG montada com duas fontes configuradas."""
    monkeypatch.setenv("BIGDATA_INGESTION_SOURCES", "open_meteo, outra_fonte")
    monkeypatch.setenv("BIGDATA_DBT_SOURCES", "open_meteo")
    importlib.reload(config)
    try:
        bag = DagBag(dag_folder=DAGS_FOLDER)
        assert bag.import_errors == {}
        yield bag.get_dag("weather_pipeline")
    finally:
        monkeypatch.undo()
        importlib.reload(config)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_weather_pipeline_order(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    assert [task.task_id for task in dag.topological_sort()] == [
        "ingest_open_meteo",
        "register_open_meteo",
        "dbt_build",
        "validate_gold",
    ]


def test_weather_pipeline_runs_one_at_a_time(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_containers_run_on_platform_network(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    for task_id in ("ingest_open_meteo", "register_open_meteo", "dbt_build"):
        task = dag.get_task(task_id)
        assert task.network_mode == "bigdata"
        assert task.auto_remove == "force"


def test_every_step_processes_the_previous_day(dag_bag):
    dag = dag_bag.get_dag("weather_pipeline")

    assert "macros.ds_add(ds, -1)" in " ".join(dag.get_task("ingest_open_meteo").command)
    assert "macros.ds_add(ds, -1)" in " ".join(dag.get_task("dbt_build").command)
    assert "macros.ds_add(ds, -1)" in dag.get_task("validate_gold").sql


def test_each_source_has_its_own_chain(multi_source_dag):
    dag = multi_source_dag

    for source in ("open_meteo", "outra_fonte"):
        ingest = dag.get_task(f"ingest_{source}")
        register = dag.get_task(f"register_{source}")
        assert ingest.command == ["run", source, "--date", TARGET_DATE]
        assert register.command == ["register-local", source]
        assert ingest.downstream_task_ids == {f"register_{source}"}
        assert register.upstream_task_ids == {f"ingest_{source}"}


def test_dbt_waits_only_for_the_sources_it_reads(multi_source_dag):
    dag = multi_source_dag

    assert dag.get_task("dbt_build").upstream_task_ids == {"register_open_meteo"}
    assert dag.get_task("register_outra_fonte").downstream_task_ids == set()


@pytest.mark.parametrize("value", ["", " , ", "open_meteo,open_meteo"])
def test_invalid_source_list_is_rejected(value):
    with pytest.raises(ValueError):
        config.parse_sources("BIGDATA_INGESTION_SOURCES", value)


def test_dbt_source_without_ingestion_is_rejected():
    with pytest.raises(ValueError, match="sem_ingestao"):
        config.dbt_sources(("open_meteo",), "open_meteo,sem_ingestao")
