FROM apache/airflow:2.9.1-python3.11

USER root
# Ставим только критические системные либы
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libkrb5-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Устанавливаем провайдеры. Используем constraints для 2.9.1 и Python 3.11
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-hdfs" \
    "apache-airflow-providers-apache-spark" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"