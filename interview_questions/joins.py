from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Joins Practice").getOrCreate()

users = spark.read.option("header", True).csv("data/users.csv")
orders = spark.read.option("header", True).csv("data/orders.csv")

inner_join_df = users.join(orders, users.user_id == orders.user_id, "inner")
inner_join_df.show()

left_join_df = users.join(orders, users.user_id == orders.user_id, "left")
left_join_df.show()

left_anti_join_df = users.join(orders, users.user_id == orders.user_id, "left_anti")
left_anti_join_df.show()

left_semi_join_df = users.join(orders, users.user_id == orders.user_id, "left_semi")
left_semi_join_df.show()
