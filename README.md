# Multi-DB Spark Incremental ETL Pipeline

A production-style incremental ETL pipeline built with **Apache Spark (PySpark)** that ingests data from **MySQL and PostgreSQL** using JDBC, performs watermark-based incremental loading, stores data in **Parquet format with Snappy compression**, and maintains execution metadata for reliable batch processing.

## Architecture

Source DB (MySQL / PostgreSQL) → JDBC Reader → Spark Incremental ETL → Parquet (Snappy) → Metadata Store (MySQL)

## Features

* Incremental data loading using watermarks
* Metadata-driven ETL execution
* JDBC ingestion from MySQL and PostgreSQL
* Parquet output with Snappy compression
* Structured logging for each job run
* Modular and reusable codebase
* Config-driven database and job management
* Fault-tolerant batch processing pattern

## Tech Stack

* Python 3.11
* Apache Spark 4.2.0
* PySpark
* MySQL
* PostgreSQL
* JDBC
* Parquet
* Git & GitHub

## Project Structure

```text
multi-db-executor/
├── config/
│   ├── databases.json
│   └── jobs.json
├── jars/
│   ├── mysql-connector-j-9.7.0.jar
│   └── postgresql-42.7.7.jar
├── src/
│   ├── incremental_executor.py
│   ├── metadata_manager.py
│   ├── jdbc_reader.py
│   ├── logger.py
│   └── ...
├── state/
├── output/
├── logs/
├── requirements.txt
└── README.md
```

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/tanishka-0117/multi-db-spark-query-executor.git
cd multi-db-spark-query-executor
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file:

```ini
META_HOST=localhost
META_USER=root
META_PASSWORD=your_password
META_DB=etl_metadata
```

### 4. Start MySQL and PostgreSQL

Ensure both database servers are running and accessible.

## Run Incremental ETL

```bash
spark-submit --jars jars/mysql-connector-j-9.7.0.jar,jars/postgresql-42.7.7.jar src/incremental_executor.py
```

## Sample Output

```text
Last watermark: 4
Incremental query: SELECT * FROM employee WHERE id > 4
New rows fetched: 0
No new data found.
```

## Metadata Table

The pipeline maintains execution state in MySQL:

| job_name               | last_loaded_value | status  | rows_loaded |
| ---------------------- | ----------------- | ------- | ----------- |
| mysql_company_employee | 4                 | SUCCESS | 1           |

## Interview Demo

1. Insert a new employee row in MySQL with a higher `id`.
2. Run the Spark job.
3. Observe:

   * `New rows fetched: 1`
   * `Updated watermark to: new_id`
4. Run the job again and observe:

   * `New rows fetched: 0`

This demonstrates true incremental ETL behavior.

## Production Improvements

* Environment variable based secret management
* Git ignored logs and outputs
* Modular code organization
* Metadata-driven processing
* Compressed columnar storage (Parquet + Snappy)

