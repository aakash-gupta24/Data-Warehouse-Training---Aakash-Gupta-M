# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("June19Assignment3").getOrCreate()
spark

# COMMAND ----------

# Tasks:
# 1. Load the data using PySpark.
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","---------AccessKeyyy----------")

inventory_supply_df=spark.read.csv("wasbs://june19assignment3@hestore.blob.core.windows.net/inventory_supply.csv",header=True,inferSchema=True)
inventory_supply_df.show()
inventory_supply_df.printSchema()
# 2. Create a new column NeedsReorder = StockQty < ReorderLevel .
inventory_supply_df=inventory_supply_df.withColumn("NeedsReorder",inventory_supply_df["StockQty"]<inventory_supply_df["ReorderLevel"])
# 3. Create a view of all items that need restocking.
inventory_supply_df.createOrReplaceTempView("inventory_supply")
# 4. Highlight warehouses with more than 2 such items.
from pyspark.sql.functions import *

inventory_supply_df.groupBy("Warehouse").agg(sum(col("NeedsReorder").cast("int")).alias("ItemsNeedingRestock")).filter(col("ItemsNeedingRestock") > 2).show()


# COMMAND ----------

# Tasks:
# 1. Group items by Supplier and compute average price.
inventory_supply_df.groupBy("Supplier").avg("UnitPrice").show()
# 2. Find which suppliers offer items below average price in their category.
inventory_supply_df.groupBy("Supplier").agg({"UnitPrice":"avg"}).withColumnRenamed("avg(UnitPrice)","avg_UnitPrice").join(inventory_supply_df,"Supplier").filter("UnitPrice<avg_UnitPrice").groupBy("Supplier").count().show()
# 3. Tag suppliers with Good Deal if >50% of their items are below market average.
from pyspark.sql.functions import *
inventory_supply_df.groupBy("Supplier").agg({"UnitPrice":"avg"}).withColumnRenamed("avg(UnitPrice)","avg_UnitPrice").join(inventory_supply_df,"Supplier").filter("UnitPrice<avg_UnitPrice").groupBy("Supplier").count().withColumnRenamed("count","below_avg").join(inventory_supply_df.groupBy("Supplier").count().withColumnRenamed("count","total"),on="Supplier").withColumn("GoodDeal",col("below_avg")/col("total")>0.5).select("Supplier","GoodDeal").show()

# COMMAND ----------

# Tasks:
# 1. Calculate TotalStockValue = StockQty * UnitPrice .
inventory_supply_df=inventory_supply_df.withColumn("TotalStockValue",inventory_supply_df["StockQty"]*inventory_supply_df["UnitPrice"])
inventory_supply_df.show()
# 2. Identify top 3 highest-value items.
inventory_supply_df.orderBy(col("TotalStockValue").desc()).limit(3).show()
# 3. Export the result as a Parquet file partitioned by Warehouse .
inventory_supply_df.write.partitionBy("Warehouse").mode("overwrite").parquet("wasbs://june19assignment3@hestore.blob.core.windows.net/inventory_supply_parquet")

# COMMAND ----------

# Tasks:
# 1. Count items stored per warehouse.
inventory_supply_df.groupBy("Warehouse").count().show()
# 2. Average stock per category in each warehouse.
inventory_supply_df.groupBy("Warehouse","Category").agg({"StockQty":"avg"}).withColumnRenamed("avg(StockQty)","avg_stock").show()
# 3. Determine underutilized warehouses ( total stock < 100 ).
inventory_supply_df.groupBy("Warehouse").agg({"StockQty":"sum"}).withColumnRenamed("sum(StockQty)","total_stock").filter("total_stock<100").show()

# COMMAND ----------

# Task:
# 1. Save as Delta table retail_inventory .
inventory_supply_df.write.format("delta").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/inventory_supply_delta")
# 2. Update stock of 'Laptop' to 20.
inventory_supply_df.filter("Category='Laptop'").write.format("delta").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/inventory_supply_delta")
# 3. Delete any item with StockQty = 0 .
inventory_supply_df.filter("StockQty=0").write.format("delta").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/inventory_supply_delta")
# 4. Run DESCRIBE HISTORY and query VERSION AS OF previous state.
inventory_supply_df.filter("StockQty=0").write.format("delta").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/inventory_supply_delta")

# COMMAND ----------

restock_logs=spark.read.csv("wasbs://june19assignment3@hestore.blob.core.windows.net/restock_logs.csv",header=True,inferSchema=True)
restock_logs.show()
restock_logs.printSchema()
# Tasks:
# 1. Join with inventory table to update StockQty.
inventory_restock_df=inventory_supply_df.join(restock_logs,on="ItemID",how="left")
inventory_restock_df.show()
# 2. Calculate new stock and flag RestockedRecently = true for updated items.
inventory_restock_df=inventory_restock_df.withColumn("StockQty",inventory_restock_df["StockQty"]+inventory_restock_df["QuantityAdded"]).withColumn("RestockedRecently",lit(True)).withColumn("RestockedRecently",when(col("RestockedRecently").isNull(),lit(False)).otherwise(col("RestockedRecently"))).withColumn("RestockedRecently",when(col("RestockedRecently")==True,lit(True)).otherwise(lit(False)))
inventory_restock_df.show()
# 3. Use MERGE INTO to update in Delta.
inventory_restock_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/inventory_supply_delta")

# COMMAND ----------

# Tasks:
# 1. Create SQL view inventory_summary with:
# ItemName, Category, StockQty, NeedsReorder, TotalStockValue
inventory_supply_df.createOrReplaceTempView("inventory_supply")
# Save as a table so it becomes permanent in catalog
inventory_supply_df.write.mode("overwrite").saveAsTable("inventory_supply")
spark.sql("create or replace temporary view inventory_summary as select ItemName,Category,StockQty,NeedsReorder,TotalStockValue from inventory_supply")
# 2. Create view supplier_leaderboard sorted by average price
spark.sql("create or replace temporary view supplier_leaderboard as select Supplier,avg(UnitPrice) as avg_price from inventory_supply group by Supplier order by avg_price")

# COMMAND ----------


# Tasks:
# 1. Use when / otherwise to categorize items:
# "Overstocked" (>2x ReorderLevel)
# "LowStock"
from pyspark.sql.functions import col, when

inventory_supply_df = inventory_supply_df.withColumn("StockStatus",when(col("StockQty") > 2 * col("ReorderLevel"), "Overstocked").when(col("StockQty") < col("ReorderLevel"), "LowStock").otherwise("Normal"))

inventory_supply_df.select("ItemID", "ItemName", "StockQty", "ReorderLevel", "StockStatus").show()

# 2. Use .filter() and .where() for the same and compare.
#Filter overstocked items using .filter()
inventory_supply_df.filter(col("StockStatus") == "Overstocked").show()
#using where
inventory_supply_df.where(col("StockStatus") == "Overstocked").show()



# COMMAND ----------

# Tasks:
from pyspark.sql.functions import to_date, date_format
# 1. Extract RestockMonth from LastRestocked .
inventory_supply_df = inventory_supply_df.withColumn("LastRestocked", to_date("LastRestocked"))

inventory_supply_df = inventory_supply_df.withColumn("RestockMonth", date_format("LastRestocked", "MMMM"))
inventory_supply_df.show()
# 2. Create feature: StockAge = CURRENT_DATE - LastRestocked
from pyspark.sql.functions import current_date, datediff

inventory_supply_df = inventory_supply_df.withColumn("StockAge", datediff(current_date(), col("LastRestocked")))
inventory_supply_df.show()
# 3. Bucket StockAge into: New, Moderate, Stale
from pyspark.sql.functions import when

inventory_supply_df = inventory_supply_df.withColumn("StockAgeBucket",when(col("StockAge") <= 30, "New").when((col("StockAge") > 30) & (col("StockAge") <= 90), "Moderate").otherwise("Stale"))
inventory_supply_df.show()

# COMMAND ----------


# Tasks:
# 1. Write full DataFrame to:
# CSV for analysts
inventory_supply_df.write.format("csv").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/export/inventory")
# JSON for integration
inventory_supply_df.write.format("json").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/export/inventory")
# Delta for pipelines
inventory_supply_df.write.format("delta").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/export/inventory/delta/")
# 2. Save with meaningful file and partition names like
# /export/inventory/stale_items/
inventory_supply_df.filter(col("StockAgeBucket") == "Stale").write.format("delta").mode("overwrite").save("wasbs://june19assignment3@hestore.blob.core.windows.net/export/inventory/stale_items")