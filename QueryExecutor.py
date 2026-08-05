from pyspark.sql import SparkSession

spark = (
SparkSession.builder
.appName("MultiDB-Query-Executor")
.master("local[*]")
.getOrCreate()
)

# Load source data

employees = spark.read.option("header", True).csv(
r"C:\Users\asus\Documents\spark-projects\multi-db-executor\data\employees.csv"
)

# Register as SQL table

employees.createOrReplaceTempView("employees")

print("=== All Employees ===")
spark.sql("SELECT * FROM employees").show()

print("=== Engineering Employees ===")
spark.sql("""
SELECT name, salary
FROM employees
WHERE department = 'Engineering'
""").show()

print("=== Average Salary by Department ===")
spark.sql("""
SELECT department,
ROUND(AVG(CAST(salary AS DOUBLE)), 2) AS avg_salary
FROM employees
GROUP BY department
""").show()

spark.stop()
