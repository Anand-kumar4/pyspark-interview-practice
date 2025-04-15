# Problem Statement:

# Find 2nd Highest Salary in Each Department

# You are given a PySpark DataFrame named employees with the following schema:

# Objective:

# Write a PySpark transformation to find the 2nd highest salary in each department.

from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank
from pyspark.sql.types import StructField,StructType,StringType,IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local").appName("second_highest_salary_in_each_department").getOrCreate()

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

df_filtered = df_ranked.filter(df_ranked.salary_rnk == 2)

df_filtered.select("department", "salary").show()
