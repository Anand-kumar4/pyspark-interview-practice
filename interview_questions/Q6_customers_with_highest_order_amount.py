# Problem Statement:

# Find Customers with the Highest Total Order Amount

# You are given two PySpark DataFrames:

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import sum, dense_rank
from pyspark.sql.window import Window



spark = SparkSession.builder.master("local").appName("Q5_get_customers_with_multiple_orders.py").getOrCreate()

orders = [(101,1,500),(102,3,800),(103,1,300),(104,3,400),(105,3,200)]
orders_schema = StructType([
    StructField("order_id",IntegerType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("order_amount",IntegerType(),True)
])

customers = [(1,"John"),(2,"Alice"),(3,"Bob"),(4,"Mike")]
customers_schmea = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("customer_name",StringType(),True)
])

df_orders = spark.createDataFrame(data=orders,schema=orders_schema)
df_customers = spark.createDataFrame(data=customers,schema=customers_schmea)

df_orders.show()
df_customers.show()

df_order_sum = df_orders.groupBy("customer_id").agg(sum("order_amount").alias("total_orders_amount"))

df_order_sum.show()

windowSpec = Window.orderBy(df_order_sum["total_orders_amount"].desc())

df_ranked = df_order_sum.withColumn("orders_rnk",dense_rank().over(windowSpec))

df_filtered = df_ranked.filter(df_ranked.orders_rnk == 1)

df_result = df_filtered.join(df_customers,"customer_id","inner")

df_result.select("customer_id","customer_name","total_orders_amount").show()