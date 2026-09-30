"""Configuração do pipeline local.

Os valores vêm do ambiente do Airflow (.env.local); os padrões são os do ambiente local.
As variáveis repassadas aos containers espelham os .env.local dos repositórios de ingestão e dbt.
"""

import os

DOCKER_URL = os.getenv("BIGDATA_DOCKER_URL", "tcp://docker-proxy:2375")
DOCKER_NETWORK = os.getenv("BIGDATA_DOCKER_NETWORK", "bigdata")
INGESTION_IMAGE = os.getenv("BIGDATA_INGESTION_IMAGE", "bigdata-ingestion:local")
DBT_IMAGE = os.getenv("BIGDATA_DBT_IMAGE", "bigdata-dbt:local")

_TRINO = {"TRINO_HOST": "trino", "TRINO_PORT": "8080"}

INGESTION_ENV = {
    # Credenciais fictícias do LocalStack.
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ENDPOINT_URL": "http://localstack:4566",
    "BRONZE_BUCKET": "bigdata-local-bronze",
    **_TRINO,
}

DBT_ENV = {
    "DBT_TARGET": "local",
    "SILVER_BUCKET": "bigdata-local-silver",
    "GOLD_BUCKET": "bigdata-local-gold",
    # Log legível na UI do Airflow (sem códigos de cor) e sem telemetria do dbt.
    "DBT_USE_COLORS": "false",
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    **_TRINO,
}
