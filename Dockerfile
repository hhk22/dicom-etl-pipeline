FROM apache/airflow:2.11.0-python3.12
USER root
RUN mkdir -p /opt/airflow/data && chown airflow:root /opt/airflow/data
USER airflow
COPY --chown=airflow:root pyproject.toml /opt/commerce/pyproject.toml
COPY --chown=airflow:root src /opt/commerce/src
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10" "pytest==8.3.5" \
    && pip install --no-cache-dir --no-deps /opt/commerce
COPY --chown=airflow:root scripts /opt/airflow/scripts
COPY --chown=airflow:root tests /opt/airflow/tests
