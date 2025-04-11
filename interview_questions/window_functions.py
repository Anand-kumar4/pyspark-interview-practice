# From the orders dataset, for each user, rank their orders based on order amount in descending order (highest amount first). 
# Show only the highest amount order per user.

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

spark = SparkSession.builder.master("local").appName("Window Function").getOrCreate()

# Reading orders data
orders = spark.read.option("header", True).csv("data/orders.csv")

# Window Specification: Partition by user_id and order by amount (desc)
window_spec = Window.partitionBy("user_id").orderBy(orders["amount"].cast("int").desc())

# Apply row_number, rank, dense_rank
ranked_orders_df = orders.withColumn("row_num", row_number().over(window_spec)) \
                         .withColumn("rank", rank().over(window_spec)) \
                         .withColumn("dense_rank", dense_rank().over(window_spec))

# Show full ranking details
ranked_orders_df.show()

# Show only highest order per user
top_orders = ranked_orders_df.filter(ranked_orders_df.row_num == 1)
top_orders.show()