"""
### Pipeline diário do tempo (Open-Meteo)

`ingest_<fonte>` → `register_<fonte>` (uma cadeia por fonte) → `dbt_build` → `validate_gold`

- **Fontes independentes:** cada fonte de `BIGDATA_INGESTION_SOURCES` tem sua própria cadeia
  `ingest_<fonte>` → `register_<fonte>`, em paralelo, com retry e log próprios.
- **dbt só espera o que usa:** `dbt_build` depende apenas das fontes de `BIGDATA_DBT_SOURCES`.
  A falha de outra fonte marca o run como falho, mas não bloqueia a gold.

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
    registered = {}
    for source in config.INGESTION_SOURCES:
        ingest = container_task(
            f"ingest_{source}",
            config.INGESTION_IMAGE,
            ["run", source, "--date", TARGET_DATE],
            config.INGESTION_ENV,
        )
        # Local: Hive Metastore via Trino. Na AWS este passo vira o Glue Crawler.
        registered[source] = container_task(
            f"register_{source}",
            config.INGESTION_IMAGE,
            ["register-local", source],
            config.INGESTION_ENV,
        )
        ingest >> registered[source]

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

    [registered[source] for source in config.DBT_SOURCES] >> dbt_build >> validate_gold
