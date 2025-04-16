from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import to_date, date_format, avg

spark = SparkSession.builder.master("local").appName("Q12_average_sales_per_product_per_month").getOrCreate()

data = [(101,"2024-01-10",200),(101,"2024-01-15",100),(101,"2024-02-05",300),(102,"2024-01-20",150)]

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("sale_amount", IntegerType(), True)
])

df_sales = spark.createDataFrame(data=data,schema=schema)

# Convert sale_date to proper month format
df_sales = df_sales.withColumn("sale_month", date_format(to_date("sale_date"), "yyyy-MM"))

# Calculate average sale amount per product per month
df_result = df_sales.groupBy("product_id", "sale_month") \
                    .agg(avg("sale_amount").alias("avg_sale_amount"))

df_result.show()
