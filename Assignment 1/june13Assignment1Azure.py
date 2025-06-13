# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('June13Assignment1').getOrCreate()
spark

# COMMAND ----------

# 1. Ingest the CSV files into two PySpark DataFrames
spark.conf.set(

  "fs.azure.account.key.hestore.blob.core.windows.net",

  "t9GPzv3EUQuPiAc+xYOGz8ugxHJYyeq+mZwiYW3CowXMCr4j0H0sofY2yXGapzCyksI7PYl/rUDj+ASt2AFRBQ=="

)
 
customers_df= spark.read.option("header", True).option("inferSchema", True).csv(

  "wasbs://june13assignment1@hestore.blob.core.windows.net/customers.csv"

)

orders_df= spark.read.option("header", True).option("inferSchema", True).csv(

  "wasbs://june13assignment1@hestore.blob.core.windows.net/orders.csv"

)
 
customers_df.show()
orders_df.show()
 
# 2. Infer schema and print the schema for both
customers_df.printSchema()
orders_df.printSchema()
# 3. Add a column TotalAmount = Quantity * Price to orders
orders_df=orders_df.withColumn('TotalAmount',orders_df['Quantity']*orders_df['Price'])
orders_df.show()
# 4. Join both DataFrames on CustomerID
customer_orders_df=customers_df.join(orders_df,on='CustomerID',how='inner')
customer_orders_df.show()

# COMMAND ----------

# 5. Filter orders where TotalAmount > 20000
customer_orders_df.filter(customer_orders_df['TotalAmount']>20000).show()
# 6. Show customers who placed more than 1 order
from pyspark.sql.functions import col
customer_orders_df.groupBy('CustomerID').count().withColumnRenamed('count', 'order_count').filter(col('order_count') > 1).show()
# 7. Group orders by City and get average order value
customer_orders_df.groupBy('City').avg('TotalAmount').show()
# 8. Sort orders by OrderDate in descending order
customer_orders_df.orderBy(customer_orders_df['OrderDate'].desc()).show()
# 9. Write the final result as a Parquet file partitioned by City
customer_orders_df.write.partitionBy('City').parquet('/wasbs://june13assignment1@hestore.blob.core.windows.net/output/')
# 10. Create a temporary view and run Spark SQL:
customer_orders_df.createOrReplaceTempView('customer_orders')
# Total sales by customer
spark.sql('select CustomerID,sum(TotalAmount) as TotalSales from customer_orders group by CustomerID').show()
# Count of products per city
spark.sql('select City, count(Product) as ProductCount from customer_orders group by City').show()
# Top 2 cities by revenue
spark.sql('select City,sum(TotalAmount) as TotalRevenue from customer_orders group by City order by TotalRevenue desc limit 2').show()