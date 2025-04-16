# Write PySpark code to find products that were NOT sold in the last 3 months from today’s date.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import to_date,date_format,current_date,add_months,max,col


spark = SparkSession.builder.master("local").appName("Q9_products_sold_in_all_months").getOrCreate()

schema = StructType([
    StructField("product_id",IntegerType(), True),
    StructField("sale_date",StringType(),True),
    StructField("sale_amount",IntegerType(),True)
])

data = [
    (101, "2024-01-10", 100),
    (101, "2024-03-12", 180),
    (102, "2024-02-15", 200),
    (102, "2024-04-10", 150),
    (103, "2023-12-01", 90),
    (103, "2024-01-05", 110),
    (104, "2023-11-10", 130),
    (105, "2024-02-20", 170),
    (105, "2024-04-12", 210),
    (106, "2024-01-25", 140),
    (106, "2024-02-01", 160)
]


df_sales = spark.createDataFrame(data=data,schema=schema)

df_sales = df_sales.withColumn("sale_date", to_date("sale_date"))


# Calculate 3-month cutoff from today
cutoff_date = add_months(current_date(), -3)

# Step 1: Find the latest sale per product
df_latest_sales = df_sales.groupBy("product_id").agg(max("sale_date").alias("last_sold"))

# Step 2: Filter products not sold in 3 months
df_stale_products = df_latest_sales.filter(col("last_sold") < cutoff_date)

# Show the result
df_stale_products.select("product_id", "last_sold").show()


