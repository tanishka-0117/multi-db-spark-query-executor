from pyspark.sql import SparkSession
from config_loader import get_database_config
from jdbc_reader import read_query

spark = (
    SparkSession.builder
    .appName("JDBCReaderTest")
    .master("local[*]")
    .getOrCreate()
)

config = get_database_config("mysql_company")

df = read_query(
    spark,
    config,
    "SELECT * FROM employee"
)

df.show()

spark.stop()