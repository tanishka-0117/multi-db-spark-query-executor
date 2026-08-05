import json
import sys
import logging
from pyspark.sql import SparkSession
from db_reader import JDBCReader

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------- Validate arguments ----------------
if len(sys.argv) != 3:
    print("Usage: spark-submit src/executor.py <db_name> <table_name>")
    sys.exit(1)

db_name = sys.argv[1]
table_name = sys.argv[2]

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("MultiDBExecutor")
    .master("local[*]")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    )
    .config(
        "spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter"
    )
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config(
        "spark.sql.warehouse.dir",
        "file:///C:/spark-projects/multi-db-executor/spark-warehouse"
    )
    .getOrCreate()
)

try:
    # ---------------- Load DB Config ----------------
    with open("config/databases.json", "r") as f:
        dbs = json.load(f)

    if db_name not in dbs:
        raise ValueError(
            f"Database '{db_name}' not found in config/databases.json"
        )

    logger.info(
        f"Reading table '{table_name}' from database '{db_name}'"
    )

    # ---------------- Read Data ----------------
    reader = JDBCReader(spark)
    df = reader.read_table(dbs[db_name], table_name)

    # ---------------- Parallelism Info ----------------
    logger.info(f"Spark partitions: {df.rdd.getNumPartitions()}")

    count = df.count()
    logger.info(f"Row count: {count}")

    df.show(truncate=False)

    # ---------------- Write Parquet ----------------
    output_path = (
        f"file:///C:/spark-projects/multi-db-executor/output/"
        f"{db_name}/{table_name}"
    )

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info(f"Parquet written successfully to: {output_path}")

except Exception as e:
    logger.exception(f"Job failed: {e}")
    sys.exit(1)

finally:
    spark.stop()
    logger.info("Spark session stopped")