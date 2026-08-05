from pyspark.sql import SparkSession
import os
import shutil

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

spark = (
    SparkSession.builder
    .appName("MultiDB-Parquet-Executor")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
    .getOrCreate()
)

df = spark.read.option("header", True).csv(
    r"C:\Users\asus\Documents\spark-projects\multi-db-executor\data\employees.csv"
)

print("=== Employee Data ===")
df.show()

output_path = r"C:\Users\asus\Documents\spark-projects\multi-db-executor\output\employees_parquet"

if os.path.exists(output_path):
    shutil.rmtree(output_path)
csv_output = r"C:\Users\asus\Documents\spark-projects\multi-db-executor\output\employees_csv"

if os.path.exists(csv_output):
    shutil.rmtree(csv_output)

df.write.mode("overwrite").option("header", True).csv(csv_output)

print(f"CSV written to: {csv_output}")
print(f"Parquet written to: {output_path}")

spark.stop()
