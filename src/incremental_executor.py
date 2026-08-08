import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max

from db_reader import JDBCReader
from metadata_manager import MetadataManager
from logger import logger


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "databases.json"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("IncrementalETL")
        .master("local[*]")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def run_incremental_job():
    spark = create_spark_session()

    try:
        databases = load_config()
        db_config = databases["mysql_company"]

        job_name = "mysql_company_employee"
        table_name = "employee"
        watermark_column = "id"

        metadata = MetadataManager()

        try:
            last_value = metadata.get_watermark(job_name)

            logger.info(f"Last watermark: {last_value}")

            query = (
                f"SELECT * FROM {table_name} "
                f"WHERE {watermark_column} > {last_value}"
            )

            logger.info(f"Incremental query: {query}")

            reader = JDBCReader(spark)
            df = reader.read_query(db_config, query)

            row_count = df.count()

            logger.info(f"New rows fetched: {row_count}")

            if row_count == 0:
                metadata.update_watermark(
                    job_name,
                    last_value,
                    0,
                    "NO_DATA"
                )

                logger.info("No new data found.")
                return

            output_dir = (
                PROJECT_ROOT
                / "output"
                / "mysql_company"
                / "incremental_employee"
            )

            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir.as_uri()

            (
                df.write
                .mode("append")
                .parquet(output_path)
            )

            max_id = (
                df.agg(spark_max(watermark_column))
                  .collect()[0][0]
            )

            metadata.update_watermark(
                job_name,
                int(max_id),
                row_count,
                "SUCCESS"
            )

            logger.info(f"Loaded {row_count} rows.")
            logger.info(f"Updated watermark to: {max_id}")

        except Exception as e:
            logger.exception("Incremental job failed")

            metadata.update_watermark(
                job_name,
                last_value,
                0,
                "FAILED"
            )

            raise

        finally:
            metadata.close()

    finally:
        spark.stop()


if __name__ == "__main__":
    run_incremental_job()