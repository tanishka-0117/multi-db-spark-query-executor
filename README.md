[![CI](https://github.com/tanishka-0117/multi-db-spark-query-executor/actions/workflows/ci.yml/badge.svg)](https://github.com/tanishka-0117/multi-db-spark-query-executor/actions/workflows/ci.yml)

# Multi-DB Spark Incremental ETL Pipeline

A production-style incremental ETL pipeline built with **Apache Spark (PySpark)** that ingests data from **MySQL and PostgreSQL** using JDBC, performs watermark-based incremental loading, stores data in **Parquet format with Snappy compression**, and maintains execution metadata for reliable batch processing.

---

## Architecture

```text
MySQL / PostgreSQL
        │
        ▼
   JDBC Reader
        │
        ▼
 Apache Spark ETL
        │
        ▼
Incremental Filter (watermark)
        │
        ▼
 Parquet + Snappy
        │
        ▼
Metadata Store (MySQL)
```

---

## Features

* Incremental data loading using watermarks
* Metadata-driven ETL execution
* JDBC ingestion from MySQL and PostgreSQL
* Parquet output with Snappy compression
* Structured logging for each job run
* Modular and reusable codebase
* Config-driven database and job management
* Fault-tolerant batch processing pattern
* Environment-variable based secret management
* Docker support
* GitHub Actions CI pipeline

---

## Tech Stack

* Python 3.11
* Apache Spark 4.2.0
* PySpark
* MySQL
* PostgreSQL
* JDBC
* Parquet
* Docker
* Git & GitHub Actions

---

## Project Structure

```text
multi-db-spark-query-executor/
├── .github/
│   └── workflows/
│       └── ci.yml
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
│   ├── executor.py
│   └── ...
├── output/          # generated
├── logs/            # generated
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/tanishka-0117/multi-db-spark-query-executor.git
cd multi-db-spark-query-executor
```

### 2. Create a virtual environment

```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```ini
META_HOST=localhost
META_USER=root
META_PASSWORD=your_password
META_DB=etl_metadata
```

### 5. Start databases

Ensure **MySQL** and **PostgreSQL** are running and accessible.

---

## Metadata Table

Create the metadata database and table in MySQL:

```sql
CREATE DATABASE etl_metadata;

USE etl_metadata;

CREATE TABLE etl_job_metadata (
    job_name VARCHAR(100) PRIMARY KEY,
    watermark_column VARCHAR(100),
    last_loaded_value BIGINT,
    last_run_time DATETIME,
    status VARCHAR(20),
    rows_loaded INT
);
```

---

## Run Incremental ETL

```bash
spark-submit --jars jars/mysql-connector-j-9.7.0.jar,jars/postgresql-42.7.7.jar src/incremental_executor.py
```

---

## Sample Output

```text
Last watermark: 4
Incremental query: SELECT * FROM employee WHERE id > 4
New rows fetched: 0
No new data found.
```

When new records are inserted:

```text
Last watermark: 4
Incremental query: SELECT * FROM employee WHERE id > 4
New rows fetched: 1
Loaded 1 rows.
Updated watermark to: 5
```

---

## Output Format

Incrementally loaded data is stored as compressed Parquet files:

```text
output/
└── mysql_company/
    └── incremental_employee/
        ├── part-00000-....
        └── _SUCCESS
```

Compression used: **Snappy**

---

## Docker

### Build image

```bash
docker build -t spark-etl .
```

### Run container

```bash
docker run --env-file .env spark-etl
```

---

## CI/CD

GitHub Actions automatically validates the project on every push and pull request by:

* Installing Python and Java
* Installing Spark dependencies
* Validating PySpark imports
* Running automated checks

Workflow file:

```text
.github/workflows/ci.yml
```

---

## Production-Oriented Improvements

* Secrets managed through environment variables
* Logs and outputs excluded from Git
* Metadata-driven watermark tracking
* Compressed columnar storage (Parquet + Snappy)
* Modular Spark ETL components
* CI pipeline for automated validation
* Containerized execution with Docker

---

## Performance Characteristics

* Incremental extraction reduces database load
* Parquet improves read performance for analytics workloads
* Snappy compression reduces storage usage
* Metadata tracking prevents duplicate processing
* Spark enables scalable distributed execution

---

## Future Enhancements

* Airflow orchestration
* S3 / ADLS data lake support
* Partitioned Parquet writes
* Schema evolution handling
* Data quality validation
* Monitoring with Prometheus/Grafana
* Kubernetes deployment
* Delta Lake integration
