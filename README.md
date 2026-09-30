# Airflow — orquestração do pipeline

Airflow 3.3 (LocalExecutor) em Docker, com a DAG `weather_pipeline`, que liga os outros repositórios:

```
ingest_open_meteo ──> register_bronze_catalog ──> dbt_build ──> validate_gold
 (bigdata-ingestion)    (bigdata-ingestion)      (bigdata-dbt)     (SQL no Trino)
```

| Task | O que faz | Equivalente na AWS |
|---|---|---|
| `ingest_open_meteo` | `ingestion run --date D-1`: API → Parquet na bronze | `EcsRunTaskOperator` |
| `register_bronze_catalog` | `ingestion register-local`: tabela e partições no Hive Metastore | `GlueCrawlerOperator` |
| `dbt_build` | `dbt build --vars '{"start_date": "D-1"}'`: silver e gold (Iceberg) com testes | `EcsRunTaskOperator` |
| `validate_gold` | Falha se o dia não chegou à gold ou se falta alguma hora em alguma cidade | `AthenaOperator` / SQL check |

- **Agenda:** todo dia às 03:00 UTC. O run de D processa **D-1**, que a essa hora já está completo na API.
- **Idempotente:** reexecutar um run ou fazer backfill não duplica dados (a partição é sobrescrita e o dbt faz merge).
- **Um run por vez** (`max_active_runs=1`), porque commits Iceberg simultâneos na mesma tabela conflitam.
- **Tentativas:** 2 novas tentativas por task, com 2 minutos de intervalo.
- **Containers:** as tasks de ingestão e dbt executam as **imagens dos outros repositórios** com `DockerOperator`. O Airflow não instala nem o dbt nem o código de ingestão.

## Pré-requisitos

1. Plataforma no ar: `docker compose up -d` e `terraform apply` no repositório `bigdata-terraform`.
2. Imagens construídas:
   - `docker compose build` em `ingestion-python`, que gera `bigdata-ingestion:local`.
   - `docker compose build` em `dbt-modeling`, que gera `bigdata-dbt:local`.

   Depois de mudar código nesses repositórios, **reconstrua a imagem**: o Airflow usa o que está na imagem.

## Subir

```bash
docker compose up -d --build
```

- UI: **http://localhost:8080**, usuário `airflow`, senha `airflow`.
- A DAG nasce **pausada**. Ative-a na UI ou com:

```bash
docker compose exec airflow-scheduler airflow dags unpause weather_pipeline
```

Ao ativar, o Airflow cria o run do dia mais recente (`catchup=False`).

## Operação

```bash
# Processar um dia específico: a data lógica D processa o dia D-1
docker compose exec airflow-scheduler airflow dags trigger weather_pipeline --logical-date 2026-09-25T03:00:00+00:00

# Backfill (também dá pela UI: Trigger → Backfill). Cria um run para cada 03:00 UTC
# dentro do intervalo, e cada run processa o dia anterior. Exemplo: processa os dias 01 a 09/09.
# Datas sem horário valem 00:00, então use 23:00 no fim para incluir o run das 03:00 do último dia.
docker compose exec airflow-scheduler airflow backfill create --dag-id weather_pipeline --from-date 2026-09-02 --to-date 2026-09-10T23:00:00

# Reexecutar uma task e as seguintes: na UI, abra o run → task → "Clear" (com "Downstream")
```

Os logs de cada task ficam na UI e em `logs/`. Os containers das tasks são removidos no fim da execução, mesmo quando falham, porque a saída deles já foi para o log.

## Testes

```bash
docker compose run --rm --build tests
```

Verificam que a DAG importa sem erros, a ordem das tasks, `max_active_runs`/`catchup`, a rede dos containers e que todas as etapas usam D-1.

## Como o Airflow executa containers

As tasks precisam criar containers no Docker do host. Em vez de montar o `docker.sock` inteiro no Airflow, o serviço `docker-proxy` (tecnativa/docker-socket-proxy) libera só as rotas de containers e imagens. O `DockerOperator` fala com ele em `tcp://docker-proxy:2375`, e os containers das tasks entram na rede `bigdata` para acessar LocalStack e Trino.

## Estrutura

```
dags/
  weather_pipeline.py        a DAG
  bigdata_pipeline/config.py imagens, rede e variáveis repassadas aos containers
  .airflowignore             evita que o Airflow procure DAGs em bigdata_pipeline/
tests/                       testes de integridade da DAG
docker-compose.yml           Postgres, docker-proxy, api-server, scheduler e dag-processor
.env.local                   configuração local (valores de desenvolvimento, sem segredos reais)
```

## Na AWS

As tasks de container viram `EcsRunTaskOperator` (as mesmas imagens, publicadas no ECR), o registro vira `GlueCrawlerOperator` e a validação roda no Athena. Isso depende dos módulos `network`, `ecr` e `ecs` do Terraform, que ainda serão escritos quando houver conta AWS.
