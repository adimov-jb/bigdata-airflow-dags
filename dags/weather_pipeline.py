"""
### Pipeline diário do tempo (Open-Meteo)

`ingest_open_meteo` → `register_bronze_catalog` → `dbt_build` → `validate_gold`

- **Dia processado:** o run de D processa **D-1** (UTC), já completo na API.
- **Idempotente:** a ingestão sobrescreve a partição do dia e o dbt faz merge, então
  reexecutar um run ou fazer backfill não duplica dados.
- **Backfill:** pela UI (*Trigger* → *Backfill*) ou `airflow backfill create`.
  `max_active_runs=1` porque commits Iceberg simultâneos na mesma tabela conflitam.
"""

from datetime import timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from bigdata_pipeline import config

TARGET_DATE = "{{ macros.ds_add(ds, -1) }}"


def container_task(task_id: str, image: str, command: list[str], environment: dict) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment,
        docker_url=config.DOCKER_URL,
        network_mode=config.DOCKER_NETWORK,
        auto_remove="force",
        # Sem bind de /tmp do host: não funciona com o socket via proxy/Docker Desktop.
        mount_tmp_dir=False,
    )


with DAG(
    dag_id="weather_pipeline",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bigdata", "open-meteo"],
    doc_md=__doc__,
) as dag:
    ingest = container_task(
        "ingest_open_meteo",
        config.INGESTION_IMAGE,
        ["run", "--date", TARGET_DATE],
        config.INGESTION_ENV,
    )

    # Local: Hive Metastore via Trino. Na AWS este passo vira o Glue Crawler.
    register_catalog = container_task(
        "register_bronze_catalog",
        config.INGESTION_IMAGE,
        ["register-local"],
        config.INGESTION_ENV,
    )

    dbt_build = container_task(
        "dbt_build",
        config.DBT_IMAGE,
        ["build", "--vars", '{"start_date": "' + TARGET_DATE + '"}'],
        config.DBT_ENV,
    )

    # Falha se o dia não chegou à gold ou se alguma cidade não tem as 24 horas.
    validate_gold = SQLCheckOperator(
        task_id="validate_gold",
        conn_id="trino_default",
        sql=f"""
            SELECT count(*) > 0 AND count_if(hours_observed = 24) = count(*)
            FROM iceberg.gold.fct_weather_daily
            WHERE observed_date = DATE '{TARGET_DATE}'
        """,
    )

    ingest >> register_catalog >> dbt_build >> validate_gold
