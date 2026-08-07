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

# ---------------- Validate Arguments ----------------
if len(sys.argv) != 3:
    print("Usage: spark-submit src/executor.py <db_name> <table_name>")
    sys.exit(1)

db_name = sys.argv[1]
table_name = sys.argv[2]

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName(f"MultiDBExecutor-{db_name}")
    .master("local[*]")
    .config(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
        "2"
    )
    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    )
    .config(
        "spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter"
    )
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    .config(
        "spark.sql.warehouse.dir",
        "file:///C:/spark-projects/multi-db-executor/spark-warehouse"
    )
    .getOrCreate()
)

try:
    # ---------------- Load Database Config ----------------
    with open("config/databases.json", "r") as f:
        databases = json.load(f)

    if db_name not in databases:
        raise ValueError(
            f"Database '{db_name}' not found in config/databases.json"
        )

    db_config = databases[db_name]

    logger.info(f"Database Config: {db_config}")
    logger.info(f"Reading table '{table_name}' from '{db_name}'")

    # ---------------- Read Table ----------------
    reader = JDBCReader(spark)
    df = reader.read_table(db_config, table_name)

    logger.info(f"Spark Partitions: {df.rdd.getNumPartitions()}")

    row_count = df.count()
    logger.info(f"Row Count: {row_count}")

    df.show(truncate=False)

    # ---------------- Output Path ----------------
    output_path = (
        f"file:///C:/spark-projects/multi-db-executor/output/"
        f"{db_name}/{table_name}"
    )

    logger.info(f"Writing Parquet to: {output_path}")

    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )

    # Partition only if the column exists
    if "department" in df.columns:
        writer = writer.partitionBy("department")
        logger.info("Partitioning by department")

    writer.parquet(output_path)

    logger.info("Parquet written successfully.")

except Exception as e:
    logger.exception(f"Job Failed: {e}")
    sys.exit(1)

finally:
    spark.stop()
    logger.info("Spark Session Stopped")