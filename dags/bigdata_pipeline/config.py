"""Configuração do pipeline local.

Os valores vêm do ambiente do Airflow (.env.local); os padrões são os do ambiente local.
Buckets, LocalStack e Trino vêm do contrato da plataforma (platform/local.env, gerado pelo
terraform apply do repositório Terraform) e são repassados aos containers das tasks.
"""

import os
import re
from pathlib import Path

DOCKER_URL = os.getenv("BIGDATA_DOCKER_URL", "tcp://docker-proxy:2375")
DOCKER_NETWORK = os.getenv("BIGDATA_DOCKER_NETWORK", "bigdata")
INGESTION_IMAGE = os.getenv("BIGDATA_INGESTION_IMAGE", "bigdata-ingestion:local")
DBT_IMAGE = os.getenv("BIGDATA_DBT_IMAGE", "bigdata-dbt:local")
PLATFORM_ENV_FILE = os.getenv("BIGDATA_PLATFORM_ENV_FILE", "/opt/airflow/platform/local.env")
# Chaves de API locais (platform/secrets.env no repositório Terraform, fora do git).
SECRETS_ENV_FILE = os.getenv("BIGDATA_SECRETS_ENV_FILE", "/opt/airflow/platform/secrets.env")


def parse_sources(variable: str, value: str) -> tuple[str, ...]:
    sources = tuple(name.strip() for name in value.split(",") if name.strip())
    if not sources:
        raise ValueError(f"{variable} não pode ser vazio")
    if len(set(sources)) != len(sources):
        raise ValueError(f"{variable} tem fontes repetidas: {value}")
    return sources


def check_dbt_domains(
    domains: dict[str, tuple[str, ...]], ingestion_sources: tuple[str, ...]
) -> dict[str, tuple[str, ...]]:
    for domain, sources in domains.items():
        unknown = [name for name in sources if name not in ingestion_sources]
        if not sources or unknown:
            raise ValueError(f"DBT_DOMAINS[{domain!r}] com fontes sem ingestão: {unknown or 'nenhuma'}")
    return domains


def load_platform_env(path: str) -> dict[str, str]:
    """Lê o arquivo KEY=VALUE gerado pelo Terraform. Falha com instrução se não existir."""
    file = Path(path)
    if not file.is_file():
        raise FileNotFoundError(
            f"{path} não encontrado. Rode `terraform apply` no repositório Terraform, que gera "
            "platform/local.env, e confira o volume da pasta platform no docker-compose.yml."
        )
    return parse_env_file(file)


def parse_env_file(file: Path) -> dict[str, str]:
    path = str(file)
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


def load_source_secrets(path: str, wanted: dict[str, tuple[str, ...]]) -> dict[str, dict]:
    """Chaves de cada fonte, lidas do arquivo de segredos. Arquivo ou chave ausente não
    impede a DAG de carregar: a task da fonte falha com a instrução de onde preenchê-la."""
    file = Path(path)
    secrets = parse_env_file(file) if file.is_file() else {}
    return {
        source: {name: secrets[name] for name in names if secrets.get(name)}
        for source, names in wanted.items()
    }


DEFAULT_SOURCES = (
    "open_meteo,open_meteo_locations,"
    "rest_countries,world_bank_indicators,world_bank_countries"
)

# Fontes da imagem de ingestão (`ingestion list`). Cada uma vira a cadeia independente
# ingest_<fonte> → register_<fonte>.
INGESTION_SOURCES = parse_sources(
    "BIGDATA_INGESTION_SOURCES", os.getenv("BIGDATA_INGESTION_SOURCES", DEFAULT_SOURCES)
)
# Domínios do dbt (seletores em selectors.yml no projeto dbt) → fontes de ingestão que cada
# um lê. Cada domínio vira uma task dbt_build_<domínio> (`dbt build --selector <domínio>`),
# que espera só as suas fontes: a falha de um domínio não trava a gold de outro.
DBT_DOMAINS = check_dbt_domains(
    {
        "open_meteo": ("open_meteo", "open_meteo_locations"),
        "countries": ("rest_countries", "world_bank_indicators", "world_bank_countries"),
    },
    INGESTION_SOURCES,
)

# Chaves de API por fonte: cada task de ingestão recebe só as da sua fonte, como variável
# privada do container (não aparece na UI nem nos logs).
SOURCE_SECRET_NAMES: dict[str, tuple[str, ...]] = {"rest_countries": ("REST_COUNTRIES_API_KEY",)}
SOURCE_SECRETS = load_source_secrets(SECRETS_ENV_FILE, SOURCE_SECRET_NAMES)

def parse_emails(value: str) -> tuple[str, ...]:
    emails = tuple(email.strip() for email in value.split(",") if email.strip())
    invalid = [email for email in emails if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email)]
    if invalid:
        raise ValueError(f"BIGDATA_ALERT_EMAILS com e-mail inválido: {invalid}")
    return emails


# Destinatários do alerta de falha. Vazio desliga o e-mail: o .env.local faz isso, porque
# o ambiente local não envia e-mail. Fora dele, o envio usa a conexão smtp_default.
ALERT_EMAILS = parse_emails(os.getenv("BIGDATA_ALERT_EMAILS", "andredimov@hotmail.com"))

PLATFORM_ENV = load_platform_env(PLATFORM_ENV_FILE)

INGESTION_ENV = dict(PLATFORM_ENV)

DBT_ENV = {
    **PLATFORM_ENV,
    "DBT_TARGET": "local",
    # Log legível na UI do Airflow (sem códigos de cor) e sem telemetria do dbt.
    "DBT_USE_COLORS": "false",
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
}
