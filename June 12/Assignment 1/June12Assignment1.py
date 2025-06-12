# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("June12Assignment1").getOrCreate()
spark

# COMMAND ----------

from pyspark.sql import Row 
data = [ 
Row(OrderID=101, Customer="Ali", Items=[{"Product":"Laptop", "Qty":1}, 
{"Product":"Mouse", "Qty":2}], Region="Asia", Amount=1200.0), 
Row(OrderID=102, Customer="Zara", Items=[{"Product":"Tablet", "Qty":1}], 
Region="Europe", Amount=650.0), 
Row(OrderID=103, Customer="Mohan", Items=[{"Product":"Phone", "Qty":2}, 
{"Product":"Charger", "Qty":1}], Region="Asia", Amount=890.0), 
Row(OrderID=104, Customer="Sara", Items=[{"Product":"Desk", "Qty":1}], 
Region="US", Amount=450.0) 
] 
df_sales = spark.createDataFrame(data) 
df_sales.show(truncate=False)

# COMMAND ----------

# Working with JSON & Nested Fields
# 1. Flatten the Items array using explode() to create one row per product.
from pyspark.sql.functions import explode, col, sum, count, countDistinct
df_sales = df_sales.withColumn("Items", explode(col("Items")))
df_sales.show(truncate=False)
# 2. Count total quantity sold per product.
df_sales.groupBy("Items.Product").agg(sum("Items.Qty").alias("TotalQuantity")).show(truncate=False)
# 3. Count number of orders per region.
df_sales.groupBy("Region").agg(countDistinct("OrderID").alias("TotalOrders")).show(truncate=False)

# COMMAND ----------

# Using when and otherwise
# 4. Create a new column HighValueOrder :
# "Yes" if Amount > 1000
# "No" otherwise
from pyspark.sql.functions import when
df_sales = df_sales.withColumn("HighValueOrder", when(col("Amount") > 1000, "Yes").otherwise("No"))
df_sales.show(truncate=False)
# 5. Add a column ShippingZone :
# Asia → "Zone A", Europe → "Zone B", US → "Zone C"
df_sales = df_sales.withColumn("ShippingZone", when(col("Region") == "Asia", "Zone A").when(col("Region") == "Europe", "Zone B").when(col("Region") == "US", "Zone C").otherwise("Zone D"))
df_sales.show(truncate=False)

# COMMAND ----------

# Temporary & Permanent Views
# 6. Register df_sales as a temporary view named sales_view .
df_sales.createOrReplaceTempView("sales_view")
# 7. Write a SQL query to:
# Count orders by Region
# Find average amount per region
spark.sql("SELECT Region, COUNT(OrderID) AS TotalOrders, AVG(Amount) AS AvgAmount FROM sales_view GROUP BY Region").show(truncate=False)
# 8. Create a permanent view using saveAsTable() .
df_sales.write.mode("overwrite").saveAsTable("sales_permanent_view")

# COMMAND ----------

# SQL Queries via Spark
spark.sql("SELECT Region, COUNT(*) as OrderCount FROM sales_view GROUP BY Region").show()

# 9. Use SQL to filter all orders with more than 1 item.
spark.sql("SELECT Customer FROM sales_view WHERE size(Items) > 1").show()
# 10. Use SQL to extract customer names where Amount > 800.
spark.sql("SELECT Customer FROM sales_view WHERE Amount > 800").show()

# COMMAND ----------

# Saving as Parquet and Reading Again
# 11. Save the exploded product-level DataFrame as a partitioned Parquet file by
# Region .
df_sales.write.mode("overwrite").partitionBy("Region").parquet("dbfs:/FileStore/shared_uploads/parquet/")
# 12. Read the parquet back and perform a group-by on Product .
spark.read.parquet("dbfs:/FileStore/shared_uploads/parquet/").groupBy("Items.Product").agg(sum("Items.Qty").alias("TotalQuantity")).show(truncate=False)

# COMMAND ----------

spark.stop()