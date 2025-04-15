# Problem Statement:

# Find Products Sold in Every Month of the Year

# You are given a PySpark DataFrame sales with the following schema:

# Objective:

# Write a PySpark query to find all product_ids which were sold in every month (Jan to Dec) of a given year.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import month,countDistinct


spark = SparkSession.builder.master("local").appName("Q9_products_sold_in_all_months").getOrCreate()

schema = StructType([
    StructField("product_id",IntegerType(), True),
    StructField("sale_date",StringType(),True),
    StructField("sale_amount",IntegerType(),True)
])

data = [(101,"2024-01-10",100),(102,"2024-02-15",200),(104,"2024-01-20",150),(104,"2024-02-25",120)]

df_sales = spark.createDataFrame(data=data,schema=schema)

df_sales_new = df_sales.withColumn("month",month(df_sales['sale_date']))

df_sales_count = df_sales_new.groupBy("product_id").agg(countDistinct("*").alias("unique_sales_count"))

df_filtered = df_sales_count.filter(df_sales_count.unique_sales_count == 12)

df_filtered.select("product_id").show()

# Dynamic
from pyspark.sql.functions import countDistinct, lit

total_months = df_sales_new.select("month").distinct().count()

df_result = df_sales_count.filter(df_sales_count.unique_sales_count == total_months)

df_result.show()
