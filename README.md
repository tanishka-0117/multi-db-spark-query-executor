\# Multi-DB Spark Query Executor



Production-style ETL pipeline using \*\*Apache Spark (PySpark)\*\* to read data from MySQL and PostgreSQL via JDBC and write analytics-ready \*\*Parquet\*\* files.



\## Architecture



```text

MySQL / PostgreSQL

&#x20;       |

&#x20;       v

&#x20;     JDBC

&#x20;       |

&#x20;       v

&#x20; Spark DataFrame

&#x20;       |

&#x20;       v

&#x20;    Parquet

```



\## Features



\* Multi-database support

\* Dynamic table selection

\* Partitioned JDBC reads

\* Structured logging

\* Parquet output

\* GitHub-ready project structure



\## Run



\### PostgreSQL



```bash

spark-submit --jars jars/mysql-connector-j-9.7.0.jar,jars/postgresql-42.7.7.jar src/executor.py postgres\_hr employee

```



\### MySQL



```bash

spark-submit --jars jars/mysql-connector-j-9.7.0.jar,jars/postgresql-42.7.7.jar src/executor.py mysql\_company employee

```



\## Output



Parquet files are generated under:



```text

output/<database\_name>/<table\_name>/

```



\## Tech Stack



Apache Spark, PySpark, JDBC, MySQL, PostgreSQL, Parquet, Git, GitHub



