# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("June17Assignment2").getOrCreate()
spark

# COMMAND ----------

# Basics
# 1. Load retail_data.csv into a PySpark DataFrame and display schema.
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","------------AccessKeyyy---------------------")

retail_data_df=spark.read.csv("wasbs://june17assignment2@hestore.blob.core.windows.net/retail_data.csv",header=True,inferSchema=True)
retail_data_df.printSchema()
# 2. Infer schema as False — then manually cast columns.
#TransactionID	Customer	City	Product	Category	Quantity	UnitPrice	TotalPrice	TransactionDate	PaymentMode
retail_data_df=spark.read.csv("wasbs://june17assignment2@hestore.blob.core.windows.net/retail_data.csv",header=True,inferSchema=False)
retail_data_df=retail_data_df.withColumn("TransactionID",retail_data_df.TransactionID.cast("int"))
retail_data_df=retail_data_df.withColumn("Customer",retail_data_df.Customer.cast("String"))
retail_data_df=retail_data_df.withColumn("City",retail_data_df.City.cast("String"))
retail_data_df=retail_data_df.withColumn("Product",retail_data_df.Product.cast("String"))
retail_data_df=retail_data_df.withColumn("Category",retail_data_df.Category.cast("String"))
retail_data_df=retail_data_df.withColumn("Quantity",retail_data_df.Quantity.cast("int"))
retail_data_df=retail_data_df.withColumn("UnitPrice",retail_data_df.UnitPrice.cast("double"))
retail_data_df=retail_data_df.withColumn("TotalPrice",retail_data_df.TotalPrice.cast("double"))
retail_data_df=retail_data_df.withColumn("TransactionDate",retail_data_df.TransactionDate.cast("date"))
retail_data_df=retail_data_df.withColumn("PaymentMode",retail_data_df.PaymentMode.cast("String"))
retail_data_df.printSchema()

# COMMAND ----------

# Data Exploration & Filtering
# 3. Filter transactions where TotalPrice > 40000 .
retail_data_df.filter(retail_data_df.TotalPrice>40000).show()
# 4. Get unique cities from the dataset.
retail_data_df.select("City").distinct().show()
# 5. Find all transactions from "Delhi" using .filter() and .where() .
retail_data_df.filter(retail_data_df.City=="Delhi").show()
retail_data_df.where(retail_data_df.City=="Delhi").show()

# COMMAND ----------

# Data Manipulation
# 6. Add a column DiscountedPrice = TotalPrice - 10%.
retail_data_df=retail_data_df.withColumn("DiscountedPrice",retail_data_df.TotalPrice*0.9)
retail_data_df.show()
# 7. Rename TransactionDate to TxnDate .
retail_data_df=retail_data_df.withColumnRenamed("TransactionDate","TxnDate")
# 8. Drop the column UnitPrice .
retail_data_df=retail_data_df.drop("UnitPrice")
retail_data_df.show()

# COMMAND ----------

# Aggregations
# 9. Get total sales by city.
retail_data_df.groupBy("City").sum("TotalPrice").show()
# 10. Get average unit price by category.
retail_data_df.groupBy("Category").avg("TotalPrice").show()
# 11. Count of transactions grouped by PaymentMode.
retail_data_df.groupBy("PaymentMode").count().show()

# COMMAND ----------

# Window Functions
# 12. Use a window partitioned by City to rank transactions by TotalPrice .
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
windowSpec=Window.partitionBy("City").orderBy("TotalPrice")
retail_data_df=retail_data_df.withColumn("rank",rank().over(windowSpec))
retail_data_df.show()
# 13. Use lag function to get previous transaction amount per city.
from pyspark.sql.functions import lag
retail_data_df=retail_data_df.withColumn("prev_txn",lag("TotalPrice").over(windowSpec))
retail_data_df.show()

# COMMAND ----------

city_region_df=spark.read.csv("wasbs://june17assignment2@hestore.blob.core.windows.net/city_region.csv",header=True,inferSchema=True)
city_region_df.show()
# 15. Join with main DataFrame and group total sales by Region.
retail_data_df=retail_data_df.join(city_region_df,retail_data_df.City==city_region_df.City,"inner")
retail_data_df.groupBy("Region").sum("TotalPrice").show()

# COMMAND ----------

# Nulls and Data Cleaning
# 16. Introduce some nulls and replace them with default values.
#insert some null values in retail_data_df
from pyspark.sql.functions import when, col

# Insert nulls into 'City' where TransactionID is divisible by 5
from pyspark.sql.functions import when, col

# Step 1: Read fresh from CSV (optional if you've already done this)
retail_data_df = spark.read.csv(
    "wasbs://june17assignment2@hestore.blob.core.windows.net/retail_data.csv",
    header=True,
    inferSchema=False
)

# Step 2: Cast columns correctly
retail_data_df = retail_data_df.select(
    col("TransactionID").cast("int"),
    col("Customer").cast("string"),
    col("City").cast("string"),
    col("Product").cast("string"),
    col("Category").cast("string"),
    col("Quantity").cast("int"),
    col("UnitPrice").cast("double"),
    col("TotalPrice").cast("double"),
    col("TransactionDate").cast("date"),
    col("PaymentMode").cast("string")
)

# Step 3: Insert nulls into City and TotalPrice
retail_data_df = retail_data_df.withColumn(
    "City",
    when(col("TransactionID") % 5 == 0, None).otherwise(col("City"))
)

retail_data_df = retail_data_df.withColumn(
    "TotalPrice",
    when(col("TransactionID") % 7 == 0, None).otherwise(col("TotalPrice"))
)


# Insert nulls into 'TotalPrice' where TransactionID is divisible by 7
retail_data_df = retail_data_df.withColumn(
    "TotalPrice",
    when(col("TransactionID") % 7 == 0, None).otherwise(col("TotalPrice"))
)

#replace
retail_data_df = retail_data_df.fillna({
    "City": "Unknown",
    "TotalPrice": 0.0
})
retail_data_df.show()
# 17. Drop rows where Quantity is null.
retail_data_df=retail_data_df.fillna(0,subset=["Quantity"])
# 18. Fill null PaymentMode with "Unknown".
retail_data_df=retail_data_df.fillna("Unknown",subset=["PaymentMode"])
retail_data_df.show()

# COMMAND ----------

# 19. Write a UDF to label orders:
def label_order(amount):
    if amount > 50000: return "High"
    elif amount >= 30000: return "Medium"
    else: return "Low"
# Apply this to classify TotalPrice .
from pyspark.sql.functions import *
from pyspark.sql.types import *
label_order_udf = udf(label_order, StringType())
retail_data_df=retail_data_df.withColumn("OrderLabel",label_order_udf(retail_data_df.TotalPrice))
retail_data_df.show()


# COMMAND ----------

# Date & Time
# 20. Extract year, month, and day from TxnDate .
from pyspark.sql.functions import year, month, dayofmonth
retail_data_df=retail_data_df.withColumn("year",year(retail_data_df.TransactionDate))
retail_data_df=retail_data_df.withColumn("month",month(retail_data_df.TransactionDate))
retail_data_df=retail_data_df.withColumn("day",dayofmonth(retail_data_df.TransactionDate))
retail_data_df.show()
# 21. Filter transactions that happened in February.
retail_data_df.filter(retail_data_df.month==2).show()

# COMMAND ----------

# Union & Duplicate Handling
# 22. Duplicate the DataFrame using union() and remove duplicates.
retail_data_df=retail_data_df.union(retail_data_df)
retail_data_df=retail_data_df.dropDuplicates()
retail_data_df.show()