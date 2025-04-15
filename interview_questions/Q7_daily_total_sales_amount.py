# Problem Statement:

# Calculate Daily Total Sales Amount from Transactions Data

# You are given a PySpark DataFrame named sales containing daily transactions.

# Objective:

# Write a PySpark query to calculate total sales amount for each sale_date.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import sum

spark = SparkSession.builder.master("local").appName("Q7_daily_total_sales_amount").getOrCreate()

schema = StructType([
    StructField("txn_id",IntegerType(),True),
    StructField("product_id",IntegerType(),True),
    StructField("sale_date",StringType(),True),
    StructField("sale_amount",IntegerType(),True)
])

data = [(1,101,"2024-04-10",100),(2,102,"2024-04-10",150),(3,103,"2024-04-11",200),(4,104,"2024-04-11",300),(5,102,"2024-04-11",250)]

df_sales = spark.createDataFrame(data=data, schema=schema)

df_sales_amount = df_sales.groupBy("sale_date").agg(sum("sale_amount").alias("total_sales_amount"))

df_sales_amount.orderBy("sale_date").select("sale_date","total_sales_amount").show()

