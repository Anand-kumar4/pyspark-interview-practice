# PySpark Syntax Bank 📘

This file is your personal quick-reference cheat sheet for all important PySpark transformations, actions, and patterns seen in real-world interview problems.

---

## 🔁 Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

windowSpec = Window.partitionBy("column").orderBy("column")

df.withColumn("rn", row_number().over(windowSpec))
```

---

## 📆 Date & Timestamp Handling

```python
from pyspark.sql.functions import to_date, date_format, date_sub, datediff

df = df.withColumn("date_col", to_date("timestamp_col"))
df = df.withColumn("month", date_format("date_col", "yyyy-MM"))
df = df.withColumn("grp", date_sub("date_col", col("rn")))
```

---

## 📊 Aggregations

```python
from pyspark.sql.functions import avg, sum, min, max, countDistinct

df.groupBy("product_id").agg(avg("sale_amount").alias("avg_sale"))
```

---

## 🧩 Joins

```python
df1.join(df2, "key_column", "inner")
df1.join(df2, df1.id == df2.user_id, "left_anti")
```

---

## 🧼 Deduplication

```python
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("email").orderBy("login_time.desc()")
df.withColumn("rn", row_number().over(windowSpec)).filter("rn == 1")
```

---

## 🧠 Grouping Consecutive Events

```python
# Using login_date - row_number to group consecutive days
df.withColumn("grp", date_sub("login_date", col("rn")))
```

---

## 🛠️ Real-World Interview Patterns

### ⏳ Products Not Sold in Last N Months

```python
from pyspark.sql.functions import current_date, add_months, max, col

cutoff_date = add_months(current_date(), -3)

# Get the last sale date per product
df_latest = df.groupBy("product_id").agg(max("sale_date").alias("last_sold"))

# Filter products not sold in the last 3 months
df_result = df_latest.filter(col("last_sold") < cutoff_date)
```

Continue adding new patterns here as you solve more questions!