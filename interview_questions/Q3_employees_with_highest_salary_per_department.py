# Problem Statement:

# Find Employee(s) with the Highest Salary in Each Department

# You are given a PySpark DataFrame named employees with the following columns:

# Objective:

# Write a PySpark transformation to return Employee(s) who have the Highest Salary in each department.

from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local").appName("employees_with_highest_salary_per_department").getOrCreate()

data = [(1,"John","IT",70000),
        (2,"Alice","HR",55000),(3,"Bob","IT",85000),(4,"Mike","HR",60000),(5,"Carol","IT",85000),(6,"Steve","HR",50000)]

schema = StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("emp_name",StringType(),True),
    StructField("department",StringType(),True),
    StructField("salary",IntegerType(),True)
])

df = spark.createDataFrame(data=data,schema=schema)

windowSpec = Window.partitionBy("department").orderBy(df["salary"].desc())

df_ranked = df.withColumn("salary_rnk",dense_rank().over(windowSpec))

df_filtered = df_ranked.filter(df_ranked.salary_rnk == 1)

df_filtered.show()

