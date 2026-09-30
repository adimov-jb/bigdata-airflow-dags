# Airflow — orquestração do pipeline

Airflow 3.3 (LocalExecutor) em Docker, com a DAG `weather_pipeline`, que liga os outros repositórios:

```
ingest_open_meteo ──> register_open_meteo ──> dbt_build ──> validate_gold
ingest_<fonte>    ──> register_<fonte>          (bigdata-dbt)   (SQL no Trino)
      ...                  ...
(bigdata-ingestion)   (bigdata-ingestion)
```

Cada fonte tem uma cadeia própria. O `dbt_build` só depende das fontes que o dbt lê.

| Task | O que faz | Equivalente na AWS |
|---|---|---|
| `ingest_<fonte>` | `ingestion run <fonte> --date D-1`: fonte → Parquet na bronze (uma task por fonte) | `EcsRunTaskOperator` |
| `register_<fonte>` | `ingestion register-local <fonte>`: tabela e partições no Hive Metastore | `GlueCrawlerOperator` |
| `dbt_build` | `dbt build --vars '{"start_date": "D-1"}'`: silver e gold (Iceberg) com testes | `EcsRunTaskOperator` |
| `validate_gold` | Falha se o dia não chegou à gold ou se falta alguma hora em alguma cidade | `AthenaOperator` / SQL check |

- **Agenda:** todo dia às 03:00 UTC. O run de D processa **D-1**, que a essa hora já está completo na API.
- **Idempotente:** reexecutar um run ou fazer backfill não duplica dados (a partição é sobrescrita e o dbt faz merge).
- **Um run por vez** (`max_active_runs=1`), porque commits Iceberg simultâneos na mesma tabela conflitam.
- **Tentativas:** 2 novas tentativas por task, com 2 minutos de intervalo.
- **Fontes independentes:** cada fonte de `BIGDATA_INGESTION_SOURCES` (`.env.local`) roda sua cadeia `ingest_<fonte>` → `register_<fonte>` em paralelo, com retry e log próprios. Uma fonte não espera nenhuma outra.
- **dbt só espera o que lê:** o `dbt_build` depende apenas das fontes de `BIGDATA_DBT_SOURCES`. Se uma fonte fora dessa lista falhar, o run fica marcado como falho, mas a gold é atualizada normalmente.
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

### Adicionar uma fonte

1. Crie a fonte no repositório `ingestion-python` e reconstrua a imagem `bigdata-ingestion:local`.
2. Inclua o nome dela em `BIGDATA_INGESTION_SOURCES`, no `.env.local`. Os nomes são os de `ingestion list`, separados por vírgula.
3. Se o dbt passar a ler essa fonte, inclua o nome também em `BIGDATA_DBT_SOURCES`. A DAG não carrega se essa lista tiver uma fonte que não está em `BIGDATA_INGESTION_SOURCES`.
4. Recrie os containers do Airflow com `docker compose up -d`. As tasks `ingest_<fonte>` e `register_<fonte>` aparecem na DAG.

Os logs de cada task ficam na UI e em `logs/`. Os containers das tasks são removidos no fim da execução, mesmo quando falham, porque a saída deles já foi para o log.

## Testes

```bash
docker compose run --rm --build tests
```

Verificam que a DAG importa sem erros, a ordem das tasks, a cadeia de cada fonte, que o dbt só espera as fontes que lê, `max_active_runs`/`catchup`, a rede dos containers e que todas as etapas usam D-1.

## CI

O workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml) roda em todo PR e em todo push para a `main`, com o mesmo comando de testes acima. A `main` é protegida: só recebe mudanças por PR, e o check `testes` precisa passar antes do merge.

## Como o Airflow executa containers

As tasks precisam criar containers no Docker do host. Em vez de montar o `docker.sock` inteiro no Airflow, o serviço `docker-proxy` (tecnativa/docker-socket-proxy) libera só as rotas de containers e imagens. O `DockerOperator` fala com ele em `tcp://docker-proxy:2375`, e os containers das tasks entram na rede `bigdata` para acessar LocalStack e Trino.

## Estrutura

```
dags/
  weather_pipeline.py        a DAG
  bigdata_pipeline/config.py imagens, rede, fontes de ingestão e variáveis repassadas aos containers
  .airflowignore             evita que o Airflow procure DAGs em bigdata_pipeline/
tests/                       testes de integridade da DAG
docker-compose.yml           Postgres, docker-proxy, api-server, scheduler e dag-processor
.env.local                   configuração local (valores de desenvolvimento, sem segredos reais)
```

## Na AWS

As tasks de container viram `EcsRunTaskOperator` (as mesmas imagens, publicadas no ECR), o registro vira `GlueCrawlerOperator` e a validação roda no Athena. Isso depende dos módulos `network`, `ecr` e `ecs` do Terraform, que ainda serão escritos quando houver conta AWS.
