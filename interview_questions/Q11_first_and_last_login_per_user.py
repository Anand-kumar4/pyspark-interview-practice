# Q11: First and Last Login Per User

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark session
spark = SparkSession.builder.master("local").appName("Q11_first_and_last_login_per_user").getOrCreate()

# Define schema and data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("login_time", StringType(), True)
])

data = [
    (1, "2024-04-10 09:00:00"),
    (1, "2024-04-11 12:00:00"),
    (2, "2024-04-10 08:30:00"),
    (2, "2024-04-12 09:45:00")
]

# Create DataFrame
df_logins = spark.createDataFrame(data=data, schema=schema)

# Convert login_time to timestamp format
df_logins = df_logins.withColumn("login_time", to_timestamp("login_time"))

# Compute first and last login time using groupBy
df_result = df_logins.groupBy("user_id") \
    .agg(min("login_time").alias("first_login_time"),
         max("login_time").alias("last_login_time"))

# Show result
df_result.show()

# Stop Spark session
spark.stop()
