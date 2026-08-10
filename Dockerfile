FROM bitnami/spark:4.2

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["spark-submit",
     "--jars",
     "jars/mysql-connector-j-9.7.0.jar,jars/postgresql-42.7.7.jar",
     "src/incremental_executor.py"]