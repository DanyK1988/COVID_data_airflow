FROM apache/airflow:2.9.1-python3.11

USER root
# Ставим только критические системные либы
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libkrb5-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Install project Python dependencies with Airflow constraints.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt" \
    -r /tmp/requirements.txt