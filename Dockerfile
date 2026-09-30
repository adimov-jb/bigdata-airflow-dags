ARG AIRFLOW_VERSION=3.3.2

FROM apache/airflow:${AIRFLOW_VERSION}-python3.12 AS runtime
ARG AIRFLOW_VERSION
COPY requirements.txt /requirements.txt
# Fixar apache-airflow evita que um provider atualize o core sem querer.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Imagem de testes: DagBag + testes de estrutura da DAG.
FROM runtime AS test
RUN pip install --no-cache-dir pytest
