# Airflow — orquestração do pipeline

Airflow 3.3 (LocalExecutor) em Docker, com duas DAGs: `weather_pipeline`, que liga os outros repositórios, e `bronze_freshness`, que monitora se os dados continuam chegando (veja [Alertas e monitoramento](#alertas-e-monitoramento)).

```
ingest_open_meteo           ──> register_open_meteo           ──┐
ingest_open_meteo_locations ──> register_open_meteo_locations ──┴──> dbt_build_open_meteo
ingest_<fonte>              ──> register_<fonte>              ────> dbt_build_<domínio>
         (bigdata-ingestion)            (bigdata-ingestion)             (bigdata-dbt)
```

Cada fonte tem uma cadeia própria. Cada domínio do dbt tem seu próprio build, que espera só as fontes que ele lê.

| Task | O que faz | Equivalente na AWS |
|---|---|---|
| `ingest_<fonte>` | `ingestion run <fonte> --date D-1`: fonte → Parquet na bronze (uma task por fonte) | `EcsRunTaskOperator` |
| `register_<fonte>` | `ingestion register-local <fonte>`: tabela e partições no Hive Metastore | `GlueCrawlerOperator` |
| `dbt_build_<domínio>` | `dbt build --select @source:<domínio> --vars '{"start_date": "D-1"}'`: silver e gold (Iceberg) do domínio, com testes. Um dos testes (`assert_gold_days_complete`) falha se o dia não chegou à gold ou se falta alguma hora em alguma cidade | `EcsRunTaskOperator` |

- **Agenda:** todo dia às 03:00 UTC. O run de D processa **D-1**, que a essa hora já está completo na API.
- **Idempotente:** reexecutar um run ou fazer backfill não duplica dados (a partição é sobrescrita e o dbt faz merge).
- **Um run por vez** (`max_active_runs=1`), porque commits Iceberg simultâneos na mesma tabela conflitam.
- **Tentativas:** 2 novas tentativas por task, com 2 minutos de intervalo. Se todas falharem, sai um alerta por e-mail (desligado no ambiente local).
- **Fontes independentes:** cada fonte de `BIGDATA_INGESTION_SOURCES` (`.env.local`) roda sua cadeia `ingest_<fonte>` → `register_<fonte>` em paralelo, com retry e log próprios. Uma fonte não espera nenhuma outra.
- **Um dbt build por domínio:** `DBT_DOMAINS`, em `dags/bigdata_pipeline/config.py`, liga cada domínio do dbt (o nome da source no projeto dbt) às fontes de ingestão que ele lê. Cada domínio tem sua task `dbt_build_<domínio>`, que espera só essas fontes. Se outra fonte ou outro domínio falhar, o run fica marcado como falho, mas a gold desse domínio é atualizada normalmente.
- **Validação da gold no dbt:** a checagem de dia completo é um teste do dbt, e não uma task do Airflow. Por isso ela roda igual no Trino e no Athena.
- **Containers:** as tasks de ingestão e dbt executam as **imagens dos outros repositórios** com `DockerOperator`. O Airflow não instala nem o dbt nem o código de ingestão.

## Pré-requisitos

1. Plataforma no ar: `docker compose up -d` e `terraform apply` no repositório `bigdata-terraform`. O `apply` gera `platform/local.env`, que o Airflow monta em `/opt/airflow/platform` e repassa aos containers das tasks (buckets, LocalStack e Trino). Sem esse arquivo, a DAG não carrega, e a UI mostra o erro com a instrução para corrigir. Se o repositório do Terraform não estiver em `../Terraform`, defina `BIGDATA_PLATFORM_DIR`.
2. Imagens construídas:
   - `docker compose build` em `ingestion-python`, que gera `bigdata-ingestion:local`.
   - `docker compose build` em `dbt-modeling`, que gera `bigdata-dbt:local`.

   Depois de mudar código nesses repositórios, **reconstrua a imagem**: o Airflow usa o que está na imagem.

## Subir

```bash
docker compose up -d --build
```

- UI: **http://localhost:8080**, usuário `airflow`, senha `airflow`.
- As DAGs nascem **pausadas**. Ative-as na UI ou com:

```bash
docker compose exec airflow-scheduler airflow dags unpause weather_pipeline
docker compose exec airflow-scheduler airflow dags unpause bronze_freshness
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
3. Se o dbt passar a ler essa fonte, inclua-a no domínio correspondente em `DBT_DOMAINS` (`dags/bigdata_pipeline/config.py`). Um domínio novo no dbt (uma source nova) vira uma entrada nova ali. A DAG não carrega se um domínio citar uma fonte que não está em `BIGDATA_INGESTION_SOURCES`.
4. Recrie os containers do Airflow com `docker compose up -d`. As tasks `ingest_<fonte>`, `register_<fonte>` e, se houver domínio novo, `dbt_build_<domínio>` aparecem na DAG.

Os logs de cada task ficam na UI e em `logs/`. Os containers das tasks são removidos no fim da execução, mesmo quando falham, porque a saída deles já foi para o log.

## Testes

```bash
docker compose run --rm --build tests
```

Verificam que as DAGs importam sem erros, a cadeia de cada fonte, que cada domínio do dbt tem seu build e só espera as fontes que lê, `max_active_runs`/`catchup`, a rede dos containers, que todas as etapas usam D-1, o contrato da plataforma e os alertas: nenhum e-mail no ambiente local, e e-mail em todas as tasks fora dele.

## CI

O workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml) roda em todo PR e em todo push para a `main`, com o mesmo comando de testes acima.

## Alertas e monitoramento

**E-mail de falha.** Toda task das duas DAGs envia e-mail quando falha de vez, depois das novas tentativas. O assunto informa DAG, task e data, e o corpo traz o run, o erro e o link para o log. O envio usa o `SmtpNotifier` do provider SMTP (`dags/bigdata_pipeline/alerts.py`).

| | Ambiente local | Fora do local (AWS) |
|---|---|---|
| `BIGDATA_ALERT_EMAILS` | vazio no `.env.local`: nenhum e-mail é enviado | não definir: o padrão é `andredimov@hotmail.com`. Vários destinatários são separados por vírgula |
| Conexão `smtp_default` | não usada | obrigatória, com host, porta, usuário, senha e `from_email`, por exemplo `AIRFLOW_CONN_SMTP_DEFAULT='smtp://usuario:senha@smtp.exemplo.com:587?from_email=airflow%40exemplo.com'`. Guarde-a no Secrets Manager |

Sem a conexão SMTP, a falha da task continua aparecendo na UI, mas o e-mail não sai, e o erro do envio fica no log da task.

**Freshness da bronze.** A DAG `bronze_freshness` roda `dbt source freshness` todo dia às 12:00 UTC. O aviso sai com 1 dia sem dados novos, e o erro, que dispara o e-mail, com 2 dias. Os limites ficam nas sources do repositório dbt. A DAG fica separada do `weather_pipeline` para pegar justamente os casos em que ele deixou de rodar: DAG pausada ou falhas seguidas.

## Como o Airflow executa containers

As tasks precisam criar containers no Docker do host. Em vez de montar o `docker.sock` inteiro no Airflow, o serviço `docker-proxy` (tecnativa/docker-socket-proxy) libera só as rotas de containers e imagens. O `DockerOperator` fala com ele em `tcp://docker-proxy:2375`, e os containers das tasks entram na rede `bigdata` para acessar LocalStack e Trino.

## Estrutura

```
dags/
  weather_pipeline.py        pipeline diário: ingestão → catálogo → dbt build por domínio (com os testes da gold)
  bronze_freshness.py        freshness diária da bronze (dbt source freshness)
  bigdata_pipeline/config.py imagens, rede, fontes, contrato da plataforma e destinatários dos alertas
  bigdata_pipeline/tasks.py  argumentos padrão das DAGs (tentativas e alerta) e tasks de container
  bigdata_pipeline/alerts.py e-mail de falha (SmtpNotifier)
  .airflowignore             evita que o Airflow procure DAGs em bigdata_pipeline/
tests/                       testes de integridade da DAG (fixtures/platform.env imita o contrato da plataforma)
docker-compose.yml           Postgres, docker-proxy, api-server, scheduler e dag-processor
.env.local                   configuração local (valores de desenvolvimento, sem segredos reais)
```

## Na AWS

As tasks de container viram `EcsRunTaskOperator` (as mesmas imagens, publicadas no ECR), e o registro vira `GlueCrawlerOperator`. A validação da gold já é um teste do dbt, então roda no Athena sem mudança no Airflow. Isso depende dos módulos `network`, `ecr` e `ecs` do Terraform, que ainda serão escritos quando houver conta AWS.
