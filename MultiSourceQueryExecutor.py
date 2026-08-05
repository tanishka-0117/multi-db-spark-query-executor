import json
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MultiSourceQueryExecutor")
    .master("local[*]")
    .getOrCreate()
)

with open(r"C:\spark-projects\multi-db-executor\sources.json") as f:
    sources = json.load(f)

for table_name, cfg in sources.items():
    if cfg["type"] == "csv":
        df = spark.read.option("header", cfg.get("header", True)).csv(cfg["path"])
        df.createOrReplaceTempView(table_name)
        print(f"Registered table: {table_name}")

query = """
SELECT department,
       COUNT(*) AS employee_count,
       ROUND(AVG(CAST(salary AS DOUBLE)), 2) AS avg_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC
"""

print("\n=== Query Result ===")
spark.sql(query).show()

spark.stop()
