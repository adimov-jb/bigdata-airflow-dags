"""Configuração do pipeline local.

Os valores vêm do ambiente do Airflow (.env.local); os padrões são os do ambiente local.
Buckets, LocalStack e Trino vêm do contrato da plataforma (platform/local.env, gerado pelo
terraform apply do repositório Terraform) e são repassados aos containers das tasks.
"""

import os
from pathlib import Path

DOCKER_URL = os.getenv("BIGDATA_DOCKER_URL", "tcp://docker-proxy:2375")
DOCKER_NETWORK = os.getenv("BIGDATA_DOCKER_NETWORK", "bigdata")
INGESTION_IMAGE = os.getenv("BIGDATA_INGESTION_IMAGE", "bigdata-ingestion:local")
DBT_IMAGE = os.getenv("BIGDATA_DBT_IMAGE", "bigdata-dbt:local")
PLATFORM_ENV_FILE = os.getenv("BIGDATA_PLATFORM_ENV_FILE", "/opt/airflow/platform/local.env")


def parse_sources(variable: str, value: str) -> tuple[str, ...]:
    sources = tuple(name.strip() for name in value.split(",") if name.strip())
    if not sources:
        raise ValueError(f"{variable} não pode ser vazio")
    if len(set(sources)) != len(sources):
        raise ValueError(f"{variable} tem fontes repetidas: {value}")
    return sources


def dbt_sources(ingestion_sources: tuple[str, ...], value: str) -> tuple[str, ...]:
    sources = parse_sources("BIGDATA_DBT_SOURCES", value)
    unknown = [name for name in sources if name not in ingestion_sources]
    if unknown:
        raise ValueError(f"BIGDATA_DBT_SOURCES sem ingestão configurada: {unknown}")
    return sources


def load_platform_env(path: str) -> dict[str, str]:
    """Lê o arquivo KEY=VALUE gerado pelo Terraform. Falha com instrução se não existir."""
    file = Path(path)
    if not file.is_file():
        raise FileNotFoundError(
            f"{path} não encontrado. Rode `terraform apply` no repositório Terraform, que gera "
            "platform/local.env, e confira o volume da pasta platform no docker-compose.yml."
        )
    env = {}
    for number, line in enumerate(file.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key.strip():
            raise ValueError(f"{path}:{number}: linha inválida, esperado KEY=VALUE")
        env[key.strip()] = value.strip()
    return env


# Fontes da imagem de ingestão (`ingestion list`). Cada uma vira a cadeia independente
# ingest_<fonte> → register_<fonte>.
INGESTION_SOURCES = parse_sources(
    "BIGDATA_INGESTION_SOURCES",
    os.getenv("BIGDATA_INGESTION_SOURCES", "open_meteo,open_meteo_locations"),
)
# Fontes de ingestão lidas pelo dbt; só elas bloqueiam o dbt_build.
DBT_SOURCES = dbt_sources(
    INGESTION_SOURCES, os.getenv("BIGDATA_DBT_SOURCES", "open_meteo,open_meteo_locations")
)

PLATFORM_ENV = load_platform_env(PLATFORM_ENV_FILE)

INGESTION_ENV = dict(PLATFORM_ENV)

DBT_ENV = {
    **PLATFORM_ENV,
    "DBT_TARGET": "local",
    # Log legível na UI do Airflow (sem códigos de cor) e sem telemetria do dbt.
    "DBT_USE_COLORS": "false",
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
}
