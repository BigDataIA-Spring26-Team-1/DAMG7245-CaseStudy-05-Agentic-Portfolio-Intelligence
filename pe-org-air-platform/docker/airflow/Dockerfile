FROM apache/airflow:2.10.2-python3.12

USER root
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir poetry==1.8.3

WORKDIR /opt/airflow/project
COPY pyproject.toml poetry.lock /opt/airflow/project/
RUN poetry config virtualenvs.create false && poetry install --no-root --no-interaction --no-ansi
RUN pip install --no-cache-dir --force-reinstall "numpy==1.26.4" "pandas==2.1.4"
