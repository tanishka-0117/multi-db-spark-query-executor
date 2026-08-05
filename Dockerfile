FROM bitnami/spark:4.2

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["spark-submit", "src/executor.py"]