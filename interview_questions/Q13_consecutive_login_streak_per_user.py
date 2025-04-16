from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, to_timestamp, col, row_number, datediff, to_date, date_sub
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.master("local").appName("Q13_consecutive_login_streak_per_user").getOrCreate()

# Define schema and data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("login_time", StringType(), True)
])

data = [
    (1, "2024-04-01"),
    (1, "2024-04-02"),
    (1, "2024-04-04"),
    (1, "2024-04-05"),
    (1, "2024-04-06"),
]

# Create DataFrame
df_logins = spark.createDataFrame(data=data, schema=schema)

# Convert login_time to date format
df_logins = df_logins.withColumn("login_date", to_date("login_time"))

# Assign row numbers partitioned by user and ordered by login_date
windowSpec = Window.partitionBy("user_id").orderBy("login_date")
df_with_rn = df_logins.withColumn("rn", row_number().over(windowSpec))

# Create group identifier for consecutive streaks
df_with_diff = df_with_rn.withColumn("grp", date_sub("login_date", col("rn")))

# Group by user and grp to find streaks
df_grouped = df_with_diff.groupBy("user_id", "grp").count()

# Find max streak per user
df_result = df_grouped.groupBy("user_id").agg({"count": "max"}).withColumnRenamed("max(count)", "longest_streak")

df_result.show()