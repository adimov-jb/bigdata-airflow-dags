ARG AIRFLOW_VERSION=3.3.2

FROM apache/airflow:${AIRFLOW_VERSION}-python3.12 AS runtime
ARG AIRFLOW_VERSION
# Constraints oficiais do Airflow: fixam os providers e todas as dependências nas versões
# testadas com esta versão do Airflow. Para atualizar, mude AIRFLOW_VERSION.
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
        --constraint "${CONSTRAINTS_URL}" \
    && pip check

# Imagem de testes: DagBag + testes de estrutura da DAG.
FROM runtime AS test
RUN pip install --no-cache-dir pytest==9.1.1
