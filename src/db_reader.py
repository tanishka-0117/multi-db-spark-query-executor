from pyspark.sql import SparkSession


class JDBCReader:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_table(self, config, table_name):

        reader = (
            self.spark.read
            .format("jdbc")
            .option("url", config["url"])
            .option("dbtable", table_name)
            .option("user", config["user"])
            .option("password", config["password"])
            .option("driver", config["driver"])
        )

        if "partitionColumn" in config:
            reader = (
                reader
                .option("partitionColumn", config["partitionColumn"])
                .option("lowerBound", config["lowerBound"])
                .option("upperBound", config["upperBound"])
                .option("numPartitions", config["numPartitions"])
            )

        return reader.load()

    def read_query(self, config, query):

        reader = (
            self.spark.read
            .format("jdbc")
            .option("url", config["url"])
            .option("dbtable", f"({query}) AS temp")
            .option("user", config["user"])
            .option("password", config["password"])
            .option("driver", config["driver"])
        )

        return reader.load()