# Problem Statement:

# Calculate Running Total of Sales Per Day (Date-wise Cumulative)

# You are given a PySpark DataFrame named sales with daily transactions.

# Write a PySpark transformation to calculate the running total of sales ordered by sale_date.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

spark = SparkSession.builder.master("local").appName("Q8_running_total_sales_per_day").getOrCreate()

schema = StructType([
    StructField("sale_date",StringType(),True),
    StructField("sale_amount",IntegerType(),True)
])

data = [("2024-04-10",250),("2024-04-11",750)]

df_sales = spark.createDataFrame(data=data,schema=schema)

windowSpec = Window.orderBy(df_sales["sale_date"].asc())

df_running_total = df_sales.withColumn("running_total",sum("sale_amount").over(windowSpec))
df_running_total.show()