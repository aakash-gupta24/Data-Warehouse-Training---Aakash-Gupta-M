# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("June12Assignment2").getOrCreate()
spark

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row
web_data = [
Row(UserID=1, Page="Home", Timestamp="2024-04-10 10:00:00", Duration=35,
Device="Mobile", Country="India"),
Row(UserID=2, Page="Products", Timestamp="2024-04-10 10:02:00", Duration=120,
Device="Desktop", Country="USA"),
Row(UserID=3, Page="Cart", Timestamp="2024-04-10 10:05:00", Duration=45,
Device="Tablet", Country="UK"),
Row(UserID=1, Page="Checkout", Timestamp="2024-04-10 10:08:00", Duration=60,
Device="Mobile", Country="India"),
Row(UserID=4, Page="Home", Timestamp="2024-04-10 10:10:00", Duration=15,
Device="Mobile", Country="Canada"),
Row(UserID=2, Page="Contact", Timestamp="2024-04-10 10:15:00", Duration=25,
Device="Desktop", Country="USA"),
Row(UserID=5, Page="Products", Timestamp="2024-04-10 10:20:00", Duration=90,
Device="Desktop", Country="India"),
]
df_web = spark.createDataFrame(web_data)
df_web.show(truncate=False)

# COMMAND ----------

# Data Exploration & Preparation
# 1. Display the schema of web_traffic_data .
df_web.printSchema()
# 2. Convert the Timestamp column to a proper timestamp type.
df_web = df_web.withColumn("Timestamp", df_web["Timestamp"].cast("timestamp"))
df_web.show(truncate=False)
# 3. Add a new column SessionMinute by extracting the minute from the Timestamp .
from pyspark.sql import functions as F
df_web = df_web.withColumn("SessionMinute", F.minute(df_web["Timestamp"]))
df_web.show(truncate=False)

# COMMAND ----------

# Filtering and Conditions
# 4. Filter users who used a "Mobile" device and visited the "Checkout" page.
df_web.filter((df_web["Device"] == "Mobile") & (df_web["Page"] == "Checkout")).show(truncate=False)
# 5. Show all entries with a Duration greater than 60 seconds.
df_web.filter(df_web["Duration"] > 60).show(truncate=False)
# 6. Find all users from India who visited the "Products" page.
df_web.filter((df_web["Country"] == "India") & (df_web["Page"] == "Products")).show(truncate=False)

# COMMAND ----------

# Aggregation and Grouping
# 7. Get the average duration per device type.
df_web.groupBy("Device").agg(F.avg("Duration").alias("AvgDuration")).show(truncate=False)
# 8. Count the number of sessions per country.
df_web.groupBy("Page").count().orderBy(F.desc("count")).show(truncate=False)
# 9. Find the most visited page overall.
df_web.groupBy("Page").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# Window Functions
# 10. Rank each user’s pages by timestamp (oldest to newest).
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("UserID").orderBy(F.asc("Timestamp"))
df_web = df_web.withColumn("Rank", F.rank().over(windowSpec))
df_web.show(truncate=False)
# 11. Find the total duration of all sessions per user using groupBy .
df_web.groupBy("UserID").agg(F.sum("Duration").alias("TotalDuration")).show(truncate=False)

# COMMAND ----------

# Spark SQL Tasks
# 12. Create a temporary view called traffic_view .
df_web.createOrReplaceTempView("traffic_view")
# 13. Write a SQL query to get the top 2 longest sessions by duration.
spark.sql("select Page, Duration from traffic_view order by Duration desc limit 2").show(truncate=False)
# 14. Get the number of unique users per page using SQL.
spark.sql("select Page, count(distinct UserID) as UniqueUsers from traffic_view group by Page").show(truncate=False)

# COMMAND ----------

# Export & Save
# 15. Save the final DataFrame to CSV.
df_web.write.mode("overwrite").csv("dbfs:/FileStore/tables/traffic_data.csv", header=True)
# 16. Save partitioned by Country in Parquet format.
df_web.write.mode("overwrite").partitionBy("Country").parquet("dbfs:/FileStore/tables/traffic_data_partitioned")

# COMMAND ----------

spark.stop()