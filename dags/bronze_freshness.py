"""
### Freshness da bronze

Roda `dbt source freshness` todo dia às 12:00 UTC, fora do `weather_pipeline`, para
detectar quando o pipeline para de trazer dados: DAG pausada, falhas seguidas ou
scheduler parado por um tempo.

- Os limites ficam nas sources do dbt (`loaded_at_field: ingested_at`).
- Se algum limite de erro for ultrapassado, a task falha e dispara o mesmo alerta por
  e-mail das outras DAGs (desligado no ambiente local). Avisos só aparecem no log.
"""

import pendulum
from airflow.sdk import DAG
from bigdata_pipeline import config
from bigdata_pipeline.tasks import container_task, default_args

with DAG(
    dag_id="bronze_freshness",
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["bigdata", "monitoramento"],
    doc_md=__doc__,
) as dag:
    container_task(
        "dbt_source_freshness",
        config.DBT_IMAGE,
        ["source", "freshness"],
        config.DBT_ENV,
    )
