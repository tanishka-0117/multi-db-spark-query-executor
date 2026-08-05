from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestJDBC") \
    .getOrCreate()

jdbc_url = "jdbc:mysql://localhost:3306/student"

properties = {
    "user": "root",
    "password": "shubh@0305",   # put the same password you used in mysql -u root -p
    "driver": "com.mysql.cj.jdbc.Driver"
}

try:
    df = spark.read.jdbc(
        url=jdbc_url,
        table="student",   # existing table
        properties=properties
    )

    print("Connection successful!")
    df.show()

except Exception as e:
    print("Database connection failed:")
    print(e)

finally:
    spark.stop()