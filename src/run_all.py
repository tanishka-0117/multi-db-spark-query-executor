import json
import subprocess
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

SPARK_SUBMIT = r"C:\Users\asus\Downloads\spark-4.2.0-bin-hadoop3\spark-4.2.0-bin-hadoop3\bin\spark-submit.cmd"
JARS = "jars/mysql-connector-j-9.7.0.jar,jars/postgresql-42.7.7.jar"
EXECUTOR = "src/executor.py"
JOBS_FILE = "config/jobs.json"

# Create logs folder
Path("logs").mkdir(exist_ok=True)

def run_job(db, table):
    command = [
        SPARK_SUBMIT,
        "--jars",
        JARS,
        EXECUTOR,
        db,
        table
    ]

    log_file = f"logs/{db}_{table}.log"
    start = time.time()

    with open(log_file, "w") as log:
        result = subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True
        )

    duration = round(time.time() - start, 2)

    return {
        "database": db,
        "table": table,
        "status": "SUCCESS" if result.returncode == 0 else "FAILED",
        "duration_seconds": duration,
        "log_file": log_file
    }

def main():
    with open(JOBS_FILE, "r") as f:
        jobs = json.load(f)

    results = []

    # Run jobs in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_job = {
            executor.submit(run_job, job["db"], job["table"]): job
            for job in jobs
        }

        for future in as_completed(future_to_job):
            result = future.result()
            print(f"Completed: {result['database']}.{result['table']} - {result['status']}")
            results.append(result)

    report_path = "logs/execution_report.json"

    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)

    print("\nExecution Summary")
    print("-" * 60)

    for r in results:
        print(
            f"{r['database']}.{r['table']} | "
            f"{r['status']} | "
            f"{r['duration_seconds']} sec"
        )

    print("-" * 60)
    print(f"Report saved to: {report_path}")

if __name__ == "__main__":
    main()