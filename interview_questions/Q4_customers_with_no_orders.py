# Problem Statement:

# Find Customers Who Never Placed Any Order

# You are given two PySpark DataFrames:


# Write a PySpark query to find all customers who have never placed any order.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.master("local").appName("Q4_customers_with_no_orders.py").getOrCreate()

data1 = [(1,"John"),(2,"Alice"),(3,"Bob"),(4,"Mike")]
data2 = [(101,1,500),(102,3,800)]

schema1 = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("customer_name",StringType(),True)
])

schema2 = StructType([
    StructField("order_id",IntegerType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("order_amount",IntegerType(),True)
])

df_cust = spark.createDataFrame(data=data1,schema=schema1)
df_orders = spark.createDataFrame(data=data2,schema=schema2)

df_cust.show()
df_orders.show()

df_joined = df_cust.join(df_orders,"customer_id","left_anti")

df_joined.show()