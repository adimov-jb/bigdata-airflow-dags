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
def dag_bag_with_env(monkeypatch):
    """Monta as DAGs com variáveis de ambiente alteradas; restaura a config no final."""

    def build(set_env: dict[str, str] | None = None, unset_env: tuple[str, ...] = ()) -> DagBag:
        for name, value in (set_env or {}).items():
            monkeypatch.setenv(name, value)
        for name in unset_env:
            monkeypatch.delenv(name, raising=False)
        importlib.reload(config)
        bag = DagBag(dag_folder=DAGS_FOLDER)
        assert bag.import_errors == {}
        return bag

    yield build
    monkeypatch.undo()
    importlib.reload(config)


@pytest.fixture
def multi_source_dag(dag_bag_with_env):
    """A DAG com uma fonte a mais, que nenhum domínio do dbt lê."""
    bag = dag_bag_with_env(
        {"BIGDATA_INGESTION_SOURCES": "open_meteo, open_meteo_locations, outra_fonte"}
    )
    return bag.get_dag("bigdata_daily")


@pytest.fixture
def multi_domain_dag(monkeypatch):
    """A DAG com dois domínios do dbt, cada um lendo fontes diferentes."""
    monkeypatch.setattr(
        config,
        "DBT_DOMAINS",
        {"open_meteo": ("open_meteo", "open_meteo_locations"), "outro": ("open_meteo_locations",)},
    )
    bag = DagBag(dag_folder=DAGS_FOLDER)
    assert bag.import_errors == {}
    return bag.get_dag("bigdata_daily")


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_bigdata_daily_dependencies(dag_bag):
    dag = dag_bag.get_dag("bigdata_daily")

    for source in ("open_meteo", "open_meteo_locations"):
        assert dag.get_task(f"ingest_{source}").downstream_task_ids == {f"register_{source}"}
    assert dag.get_task("dbt_build_open_meteo").upstream_task_ids == {
        "register_open_meteo",
        "register_open_meteo_locations",
    }
    assert dag.get_task("dbt_build_open_meteo").downstream_task_ids == set()
    assert "validate_gold" not in dag.task_ids


def test_bigdata_daily_runs_one_at_a_time(dag_bag):
    dag = dag_bag.get_dag("bigdata_daily")

    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_containers_run_on_platform_network(dag_bag):
    dag = dag_bag.get_dag("bigdata_daily")

    for task_id in ("ingest_open_meteo", "register_open_meteo", "dbt_build_open_meteo"):
        task = dag.get_task(task_id)
        assert task.network_mode == "bigdata"
        assert task.auto_remove == "force"


def test_every_step_processes_the_previous_day(dag_bag):
    dag = dag_bag.get_dag("bigdata_daily")

    assert "macros.ds_add(ds, -1)" in " ".join(dag.get_task("ingest_open_meteo").command)
    assert "macros.ds_add(ds, -1)" in " ".join(dag.get_task("dbt_build_open_meteo").command)


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

    assert "register_outra_fonte" not in dag.get_task("dbt_build_open_meteo").upstream_task_ids
    assert dag.get_task("register_outra_fonte").downstream_task_ids == set()


def test_dbt_build_selects_only_its_domain(dag_bag):
    task = dag_bag.get_dag("bigdata_daily").get_task("dbt_build_open_meteo")

    assert task.command[:3] == ["build", "--select", "@source:open_meteo"]


def test_each_dbt_domain_is_independent(multi_domain_dag):
    dag = multi_domain_dag

    assert dag.get_task("dbt_build_outro").upstream_task_ids == {"register_open_meteo_locations"}
    assert dag.get_task("dbt_build_outro").command[2] == "@source:outro"
    # Nenhum build de domínio depende de outro.
    assert not dag.get_task("dbt_build_open_meteo").upstream_task_ids & {"dbt_build_outro"}
    assert not dag.get_task("dbt_build_outro").upstream_task_ids & {"dbt_build_open_meteo"}


@pytest.mark.parametrize("value", ["", " , ", "open_meteo,open_meteo"])
def test_invalid_source_list_is_rejected(value):
    with pytest.raises(ValueError):
        config.parse_sources("BIGDATA_INGESTION_SOURCES", value)


def test_dbt_domain_without_ingestion_is_rejected():
    with pytest.raises(ValueError, match="sem_ingestao"):
        config.check_dbt_domains({"open_meteo": ("open_meteo", "sem_ingestao")}, ("open_meteo",))


def test_containers_receive_the_platform_contract(dag_bag):
    dag = dag_bag.get_dag("bigdata_daily")
    platform = config.load_platform_env("/opt/airflow/tests/fixtures/platform.env")

    assert dag.get_task("ingest_open_meteo").environment == platform
    dbt_env = dag.get_task("dbt_build_open_meteo").environment
    assert dbt_env["SILVER_BUCKET"] == "bigdata-local-silver"
    assert dbt_env["DBT_TARGET"] == "local"


def test_missing_platform_file_explains_how_to_fix(tmp_path):
    with pytest.raises(FileNotFoundError, match="terraform apply"):
        config.load_platform_env(str(tmp_path / "local.env"))


def test_malformed_platform_line_is_rejected(tmp_path):
    file = tmp_path / "local.env"
    file.write_text("# comentário\nBRONZE_BUCKET=x\nsem-igual\n", encoding="utf-8")

    with pytest.raises(ValueError, match="local.env:3"):
        config.load_platform_env(str(file))


ALERTED_DAGS = ("bigdata_daily", "bronze_freshness")


def all_tasks(bag):
    return [task for dag_id in ALERTED_DAGS for task in bag.get_dag(dag_id).tasks]


def test_local_environment_sends_no_email(dag_bag):
    assert config.ALERT_EMAILS == ()
    for task in all_tasks(dag_bag):
        assert not task.on_failure_callback, task.task_id


def test_every_task_emails_on_failure_outside_local(dag_bag_with_env):
    bag = dag_bag_with_env({"BIGDATA_ALERT_EMAILS": "andredimov@hotmail.com"})

    for task in all_tasks(bag):
        [notifier] = task.on_failure_callback
        assert type(notifier).__name__ == "SmtpNotifier", task.task_id
        assert notifier.to == ["andredimov@hotmail.com"]
        assert task.retries == 2


def test_alert_email_default_is_andre(dag_bag_with_env):
    dag_bag_with_env(unset_env=("BIGDATA_ALERT_EMAILS",))

    assert config.ALERT_EMAILS == ("andredimov@hotmail.com",)


def test_invalid_alert_email_is_rejected():
    with pytest.raises(ValueError, match="sem-arroba"):
        config.parse_emails("andredimov@hotmail.com, sem-arroba")


def test_freshness_runs_daily_outside_the_pipeline(dag_bag):
    dag = dag_bag.get_dag("bronze_freshness")
    task = dag.get_task("dbt_source_freshness")

    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert task.command == ["source", "freshness"]
    assert task.network_mode == "bigdata"
    assert task.environment["DBT_TARGET"] == "local"
