# ✅ Problem Statement:

# You are given a PySpark DataFrame logins with user login history:

# 🎯 Objective:

# For each user_id, find their first login time and last login time.


from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.master("local").appName("Q11_first_and_last_login_per_user").getOrCreate()

schema = StructType([
    StructField("user_id",IntegerType(),True),
    StructField("login_time",StringType(),True)
])

data = [(1,"2024-04-10 09:00:00"),(1,"2024-04-11 12:00:00"),(2,"2024-04-10 08:30:00"),(2,"2024-04-12 09:45:00")]

df_logins = spark.createDataFrame(data=data,schema=schema)

windowSpec = Window.partitionBy("user_id").orderBy(df_logins["login_time"].desc())

df_result = df_logins.withColumn("first_login_time", min("login_time").over(windowSpec)).withColumn("last_login_time", max("login_time").over(windowSpec))


df_result.show()


df_alternate_sol = df_logins.groupBy("user_id") \
    .agg(min("login_time").alias("first_login_time"),
         max("login_time").alias("last_login_time"))

df_alternate_sol.show()