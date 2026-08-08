from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquet") \
    .master("local[*]") \
    .getOrCreate()


df = spark.read.parquet(
    "output/mysql_company/query_result"
)

df.show()

spark.stop()