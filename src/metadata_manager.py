from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

META_CONFIG = {
    "host": os.getenv("META_HOST"),
    "user": os.getenv("META_USER"),
    "password": os.getenv("META_PASSWORD"),
    "database": os.getenv("META_DB")
}

class MetadataManager:

    def __init__(self):
        self.conn = mysql.connector.connect(**META_CONFIG)

    def get_watermark(self, job_name):
        cursor = self.conn.cursor()

        cursor.execute(
            """
            SELECT last_loaded_value
            FROM etl_job_metadata
            WHERE job_name = %s
            """,
            (job_name,)
        )

        row = cursor.fetchone()
        cursor.close()

        if row:
            return row[0]

        return 0

    def update_watermark(self, job_name, new_value, rows_loaded, status):
        cursor = self.conn.cursor()

        cursor.execute(
            """
            UPDATE etl_job_metadata
            SET last_loaded_value = %s,
                last_run_time = NOW(),
                status = %s,
                rows_loaded = %s
            WHERE job_name = %s
            """,
            (new_value, status, rows_loaded, job_name)
        )

        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()