# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("June17Assignment1").getOrCreate()
spark

# COMMAND ----------

# Data Ingestion & Schema Handling
# 1. Load the CSV using inferred schema.
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","------------AccessKeyyy---------------------")

"wasbs://june16assignment1@hestore.blob.core.windows.net/course_enrollments.csv"
customer_df=spark.read.csv("wasbs://june17assignment1@hestore.blob.core.windows.net/customers.csv",header=True,inferSchema=True)
orders_df=spark.read.csv("wasbs://june17assignment1@hestore.blob.core.windows.net/orders.csv",header=True,inferSchema=True)
products_df=spark.read.csv("wasbs://june17assignment1@hestore.blob.core.windows.net/products.csv",header=True,inferSchema=True)
customer_df.show()
orders_df.show()
products_df.show()


# COMMAND ----------

# 2. Load the same file with schema explicitly defined.
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType

customer_schema=StructType([StructField("CustomerID",IntegerType(),True),
                            StructField("CustomerName",StringType(),True),
                            StructField("Region",StringType(),True),
                            StructField("SignupDate",DateType(),True)])
orders_schema=StructType([StructField("OrderID",IntegerType(),True),
                           StructField("CustomerID",IntegerType(),True),
                           StructField("ProductID",IntegerType(),True),
                           StructField("Quantity",IntegerType(),True),
                           StructField("Price",FloatType(),True),
                           StructField("OrderDate",DateType(),True),
                           StructField("Status",StringType(),True)])
products_schema=StructType([StructField("ProductID",IntegerType(),True),
                            StructField("ProductName",StringType(),True),
                            StructField("Category",StringType(),True),
                            StructField("Stock",IntegerType(),True),
                            StructField("ReorderLevel",IntegerType(),True)])
customer_df_temp=spark.read.csv("wasbs://june17assignment1@hestore.blob.core.windows.net/customers.csv",header=True,schema=customer_schema)
orders_df_temp=spark.read.csv("wasbs://june17assignment1@hestore.blob.core.windows.net/orders.csv",header=True,schema=orders_schema)
products_df_temp=spark.read.csv("wasbs://june17assignment1@hestore.blob.core.windows.net/products.csv",header=True,schema=products_schema)
customer_df_temp.show()
orders_df_temp.show()
products_df_temp.show()

# COMMAND ----------

# PySpark + Delta
# 1. Ingest all 3 CSVs as Delta Tables.
customer_df.write.mode('overwrite').format("delta").saveAsTable("customer")
orders_df.write.mode('overwrite').format("delta").saveAsTable("orders")
products_df.write.mode('overwrite').format("delta").saveAsTable("products")
# 2. Write SQL to get the total revenue per Product.
spark.sql("select ProductName,sum(Quantity*Price) as Revenue from orders join products on orders.ProductID=products.ProductID group by ProductName").show()
# 3. Join Orders + Customers to find revenue by Region.
spark.sql("select Region,sum(Quantity*Price) as Revenue from orders join customer on orders.CustomerID=customer.CustomerID group by Region").show()
# 4. Update the Status of Pending orders to 'Cancelled'.
spark.sql("update orders set Status='Cancelled' where Status='Pending'").show()
# 5. Merge a new return record into Orders.
import datetime
new_return_df = spark.createDataFrame(
    [(9999, 1, 1, 1, 0.0,  datetime.date(2025, 6, 17), 'Returned')],
    schema=orders_schema
)


new_return_df.createOrReplaceTempView("new_return")

spark.sql("""
MERGE INTO orders AS target
USING new_return AS source
ON target.OrderID = source.OrderID
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
""")

# COMMAND ----------

# DLT Pipeline
# 6. Create raw → cleaned → aggregated tables:
# Clean: Remove rows with NULLs
spark.sql("create table if not exists customer_cleaned as select * from customer where CustomerID is not null and CustomerName is not null and Region is not null and SignupDate is not null")
# Aggregated: Total revenue per Category
spark.sql("create table if not exists products_aggregated as select Category,sum(Quantity*Price) as Revenue from orders join products on orders.ProductID=products.ProductID group by Category")


# COMMAND ----------

# Time Travel
# 7. View data before the Status update.
spark.sql("select * from orders version as of 0").show()
# 8. Restore to an older version of the orders table.
spark.sql("RESTORE TABLE orders TO VERSION AS OF 0")
spark.sql("select * from orders").show()

# COMMAND ----------

# Vacuum + Retention
# 9. Run VACUUM after changing default retention.
spark.sql("ALTER TABLE orders SET TBLPROPERTIES (delta.logRetentionDuration = '10 hours')")

# COMMAND ----------

# Expectations
# 10. Quantity > 0 , Price > 0 , OrderDate is not null
spark.sql("select * from orders where Quantity <= 0 or Price <= 0 or OrderDate is null").show()

# COMMAND ----------

# Bonus
# 11. Use when-otherwise to create a new column: OrderType = "Return" if Status ==
# 'Returned'
spark.sql("select *, CASE WHEN Status='Returned' THEN 'Return' ELSE 'Normal' END as OrderType from orders").show()