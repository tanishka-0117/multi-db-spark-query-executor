from pyspark.sql import SparkSession

class JDBCReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_table(self, config, table_name):
        return self.spark.read.jdbc(
            url=config["url"],
            table=table_name,
            properties={
                "user": config["user"],
                "password": config["password"],
                "driver": config["driver"]
            }
        )