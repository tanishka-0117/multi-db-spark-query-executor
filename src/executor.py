import sys
import time
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from config_loader import get_database_config
from db_reader import JDBCReader


# =========================================================
# Logging Configuration
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# Validate Command Line Arguments
# =========================================================
if len(sys.argv) != 4:
    print("Usage:")
    print("spark-submit executor.py <db_name> table <table_name>")
    print("spark-submit executor.py <db_name> query <sql_query>")
    sys.exit(1)

db_name = sys.argv[1]
job_type = sys.argv[2]
value = sys.argv[3]


# =========================================================
# Project Root
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# =========================================================
# Spark Session
# =========================================================
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
        str(PROJECT_ROOT / "spark-warehouse")
    )
    .getOrCreate()
)

start_time = time.time()

try:

    # =====================================================
    # Load Database Configuration
    # =====================================================
    db_config = get_database_config(db_name)

    reader = JDBCReader(spark)

    # =====================================================
    # TABLE EXECUTION
    # =====================================================
    if job_type == "table":

        logger.info(f"Reading table: {value}")

        df = reader.read_table(
            db_config,
            value
        )

        output_name = value

    # =====================================================
    # QUERY EXECUTION
    # =====================================================
    elif job_type == "query":

        logger.info(f"Executing query: {value}")

        df = reader.read_query(
            db_config,
            value
        )

        output_name = "query_result"

    else:
        raise ValueError("job_type must be either table or query")

    # =====================================================
    # DataFrame Information
    # =====================================================
    partitions = df.rdd.getNumPartitions()
    row_count = df.count()

    logger.info(f"Partitions: {partitions}")
    logger.info(f"Rows fetched: {row_count}")

    df.show(truncate=False)

    # =====================================================
    # Output Path
    # =====================================================
    output_path = (
        PROJECT_ROOT / "output" / db_name / output_name
    )

    # =====================================================
    # Write Parquet
    # =====================================================
    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )

    # Partition if department column exists
    if "department" in df.columns:
        writer = writer.partitionBy("department")

    writer.parquet(str(output_path))

    logger.info(f"Output written to: {output_path}")

    # =====================================================
    # Execution Metrics
    # =====================================================
    duration = round(time.time() - start_time, 2)

    logger.info("Execution completed successfully")
    logger.info(f"Database      : {db_name}")
    logger.info(f"Job Type      : {job_type}")
    logger.info(f"Output Path   : {output_path}")
    logger.info(f"Duration (s)  : {duration}")

except Exception as e:

    logger.exception("Execution failed")
    sys.exit(1)

finally:

    spark.stop()
    logger.info("Spark Session Stopped")