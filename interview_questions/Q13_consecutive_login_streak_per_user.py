from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, to_timestamp, col, row_number, datediff, to_date, date_sub
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.master("local").appName("Q13_consecutive_login_streak_per_user").getOrCreate()

# Define schema and data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("login_time", StringType(), True)  # login_time as string initially
])

# Sample login data per user
data = [
    (1, "2024-04-01"),
    (1, "2024-04-02"),
    (1, "2024-04-04"),
    (1, "2024-04-05"),
    (1, "2024-04-06"),
]

# Create initial DataFrame
df_logins = spark.createDataFrame(data=data, schema=schema)

# Convert login_time string to actual date format
df_logins = df_logins.withColumn("login_date", to_date("login_time"))

# Assign row numbers ordered by login_date for each user
# This simulates a virtual sequence of login dates
windowSpec = Window.partitionBy("user_id").orderBy("login_date")
df_with_rn = df_logins.withColumn("rn", row_number().over(windowSpec))

# Create a group key by subtracting row number from login_date
# This helps identify streaks: consecutive logins will have same (login_date - rn) value
df_with_diff = df_with_rn.withColumn("grp", date_sub("login_date", col("rn")))

# Count the number of logins in each streak group
df_grouped = df_with_diff.groupBy("user_id", "grp").count()

# From the grouped streaks, find the max (longest) count per user
df_result = df_grouped.groupBy("user_id").agg({"count": "max"}).withColumnRenamed("max(count)", "longest_streak")

# Display result
df_result.show()