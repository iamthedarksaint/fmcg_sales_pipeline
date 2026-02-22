FROM apache/airflow:3.1.7

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt