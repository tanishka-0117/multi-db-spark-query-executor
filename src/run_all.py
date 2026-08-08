import json
import subprocess
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================================================
# Project Paths
# =========================================================
PROJECT_DIR = Path(r"C:\spark-projects\multi-db-executor")

SPARK_SUBMIT = (
    r"C:\Users\asus\Downloads\spark-4.2.0-bin-hadoop3"
    r"\spark-4.2.0-bin-hadoop3\bin\spark-submit.cmd"
)

JARS = (
    str(PROJECT_DIR / "jars" / "mysql-connector-j-9.7.0.jar")
    + "," +
    str(PROJECT_DIR / "jars" / "postgresql-42.7.7.jar")
)

EXECUTOR = str(PROJECT_DIR / "src" / "executor.py")
JOBS_FILE = PROJECT_DIR / "config" / "jobs.json"

# =========================================================
# Logs Folder
# =========================================================
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def run_job(db, job_type, value):
    """
    Execute one Spark ETL job.
    """

    command = [
        SPARK_SUBMIT,
        "--jars",
        JARS,
        EXECUTOR,
        db,
        job_type,
        value
    ]

    safe_name = "query" if job_type == "query" else value
    log_file = LOG_DIR / f"{db}_{safe_name}.log"

    start_time = time.time()

    try:
        with open(log_file, "w", encoding="utf-8") as log:

            result = subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(PROJECT_DIR)
            )

        duration = round(time.time() - start_time, 2)

        return {
            "database": db,
            "job_type": job_type,
            "name": value,
            "status": "SUCCESS" if result.returncode == 0 else "FAILED",
            "duration_seconds": duration,
            "log_file": str(log_file)
        }

    except Exception as e:

        duration = round(time.time() - start_time, 2)

        return {
            "database": db,
            "job_type": job_type,
            "name": value,
            "status": "ERROR",
            "error": str(e),
            "duration_seconds": duration,
            "log_file": str(log_file)
        }


def main():

    # -----------------------------------------------------
    # Load jobs configuration
    # -----------------------------------------------------
    with open(JOBS_FILE, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    results = []

    # -----------------------------------------------------
    # Parallel Spark execution
    # -----------------------------------------------------
    with ThreadPoolExecutor(max_workers=2) as executor:

        future_to_job = {}

        for job in jobs:

            if "table" in job:

                future = executor.submit(
                    run_job,
                    job["db"],
                    "table",
                    job["table"]
                )

            elif "query" in job:

                future = executor.submit(
                    run_job,
                    job["db"],
                    "query",
                    job["query"]
                )

            else:
                print(f"Skipping invalid job: {job}")
                continue

            future_to_job[future] = job

        # -------------------------------------------------
        # Collect results
        # -------------------------------------------------
        for future in as_completed(future_to_job):

            result = future.result()

            print(
                f"Completed: {result['database']} "
                f"({result['job_type']}) - "
                f"{result['status']}"
            )

            results.append(result)

    # -----------------------------------------------------
    # Save execution report
    # -----------------------------------------------------
    report_path = LOG_DIR / "execution_report.json"

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)

    # -----------------------------------------------------
    # Print summary
    # -----------------------------------------------------
    print("\nExecution Summary")
    print("-" * 60)

    for result in results:

        print(
            f"{result['database']} | "
            f"{result['job_type']} | "
            f"{result['status']} | "
            f"{result['duration_seconds']} sec"
        )

    print("-" * 60)
    print(f"Report saved to: {report_path}")


if __name__ == "__main__":
    main()