# Problem Statement:

# Employee Department Rank

# You are given a PySpark DataFrame named employees with the following schema:

# Objective:

# Write a PySpark transformation to fetch the Top 2 highest paid employees in each department.

from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank
from pyspark.sql.types import StructField, StructType, StringType,IntegerType,DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

spark = SparkSession.builder.master("local").appName("Top_2_highest_paid_employees_per_department").getOrCreate()

# columns = ["emp_id","emp_name","department","salary"]
data = [(1,"John","IT",70000),
        (2,"Alice","HR",55000),(3,"Bob","IT",85000),(4,"Mike","HR",60000),(5,"Carol","IT",85000),(6,"Steve","HR",50000)]

schema = StructType([ \
    StructField("emp_id",IntegerType(),True), \
    StructField("emp_name",StringType(),True), \
    StructField("department",StringType(),True), \
    StructField("salary",IntegerType(),True) \
])

df = spark.createDataFrame(data=data,schema=schema)
df.show()

windowSpec = Window.partitionBy("department").orderBy(df["salary"].desc())

df_ranked = df.withColumn("salary_rnk",dense_rank().over(windowSpec))

df_filtered = df_ranked.filter(df_ranked.salary_rnk <= 2)

df_filtered.show()



# “I created a Window Specification partitioned by department and ordered by salary in descending order.
# Then, I used dense_rank() to rank employees within their departments based on salary.
# Finally, I filtered only those rows where salary_rank is less than or equal to 2 — which gives me the Top 2 highest paid employees per department.”
