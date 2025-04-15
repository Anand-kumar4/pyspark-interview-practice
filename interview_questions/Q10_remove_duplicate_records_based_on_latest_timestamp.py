# Problem Statement:

# Remove Duplicate Records by Keeping the Most Recent One

# You are given a PySpark DataFrame logins that tracks user login history.

# Objective:

# For each unique email, keep only the latest login (i.e. highest login_time) and discard older ones.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,TimestampType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


spark = SparkSession.builder.master("local").appName("Q10_remove_duplicate_records_based_on_latest_timestamp").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(),True),
    StructField("login_time",StringType(),True),
    StructField("email",StringType(),True)
])

data = [
    (1, '2024-04-10 09:00:00', "john@example.com"),
    (2, '2024-04-10 10:00:00', "alice@example.com"),
    (3, '2024-04-10 11:00:00', "john@example.com"),
    (4, '2024-04-10 08:30:00', "bob@example.com")
]

df_login = spark.createDataFrame(data=data,schema=schema)

df_login = df_login.withColumn("login_time", to_timestamp("login_time"))

windowSpec = Window.partitionBy("email").orderBy(df_login["login_time"].desc())

df_ranked = df_login.withColumn("rn", row_number().over(windowSpec))

df_latest = df_ranked.filter("rn=1").drop("rn")

df_latest.show()