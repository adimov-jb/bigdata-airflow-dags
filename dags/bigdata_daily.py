"""
### Pipeline diário da plataforma

Orquestra todas as fontes de ingestão e os domínios do dbt (hoje: Open-Meteo).

`ingest_<fonte>` → `register_<fonte>` (uma cadeia por fonte) → `dbt_build_<domínio>`

- **Fontes independentes:** cada fonte de `BIGDATA_INGESTION_SOURCES` tem sua própria cadeia
  `ingest_<fonte>` → `register_<fonte>`, em paralelo, com retry e log próprios.
- **Um dbt build por domínio:** cada domínio de `DBT_DOMAINS` (config.py) tem sua task
  `dbt_build_<domínio>`, que roda `dbt build --selector <domínio>` e espera só as fontes
  desse domínio. A falha de outra fonte ou de outro domínio marca o run como falho, mas não
  bloqueia essa gold.
- **Validação da gold:** fica nos testes do dbt (`assert_gold_days_complete`), com a mesma
  janela do build: o dia precisa estar na gold, com as 24 horas de cada cidade.

- **Dia processado:** o run de D processa **D-1** (UTC), já completo na API.
- **Idempotente:** a ingestão sobrescreve a partição do dia e o dbt faz merge, então
  reexecutar um run ou fazer backfill não duplica dados.
- **Backfill:** pela UI (*Trigger* → *Backfill*) ou `airflow backfill create`.
  `max_active_runs=1` porque commits Iceberg simultâneos na mesma tabela conflitam.
- **Alerta:** a task que falhar depois das novas tentativas envia e-mail para
  `BIGDATA_ALERT_EMAILS` (desligado no ambiente local).
"""

import pendulum
from airflow.sdk import DAG
from bigdata_pipeline import config
from bigdata_pipeline.tasks import container_task, default_args

TARGET_DATE = "{{ macros.ds_add(ds, -1) }}"


with DAG(
    dag_id="bigdata_daily",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["bigdata", "pipeline"],
    doc_md=__doc__,
) as dag:
    registered = {}
    for source in config.INGESTION_SOURCES:
        ingest = container_task(
            f"ingest_{source}",
            config.INGESTION_IMAGE,
            ["run", source, "--date", TARGET_DATE],
            config.INGESTION_ENV,
            private_environment=config.SOURCE_SECRETS.get(source),
        )
        # Local: Hive Metastore via Trino. Na AWS este passo vira o Glue Crawler.
        registered[source] = container_task(
            f"register_{source}",
            config.INGESTION_IMAGE,
            ["register-local", source],
            config.INGESTION_ENV,
        )
        ingest >> registered[source]

    for domain, sources in config.DBT_DOMAINS.items():
        dbt_build = container_task(
            f"dbt_build_{domain}",
            config.DBT_IMAGE,
            [
                "build",
                "--selector",
                domain,
                "--vars",
                '{"start_date": "' + TARGET_DATE + '"}',
            ],
            config.DBT_ENV,
        )
        [registered[source] for source in sources] >> dbt_build
