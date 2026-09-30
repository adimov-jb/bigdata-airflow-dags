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
- **Alerta:** a task que falhar depois das novas tentativas envia e-mail para
  `BIGDATA_ALERT_EMAILS` (desligado no ambiente local).
"""

import pendulum
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.sdk import DAG
from bigdata_pipeline import config
from bigdata_pipeline.tasks import container_task, default_args

TARGET_DATE = "{{ macros.ds_add(ds, -1) }}"


with DAG(
    dag_id="weather_pipeline",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
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
