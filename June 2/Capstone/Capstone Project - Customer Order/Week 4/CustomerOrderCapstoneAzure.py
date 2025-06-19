# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("CustomerOrderCapstone").getOrCreate()
spark

# COMMAND ----------

#  1. Load the CSVs into PySpark
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","t9GPzv3EUQuPiAc+xYOGz8ugxHJYyeq+mZwiYW3CowXMCr4j0H0sofY2yXGapzCyksI7PYl/rUDj+ASt2AFRBQ==")

# Load orders
orders_df=spark.read.csv("wasbs://customerordercapstone@hestore.blob.core.windows.net/orders.csv",header=True,inferSchema=True)

# Load delivery status
delivery_status_df=spark.read.csv("wasbs://customerordercapstone@hestore.blob.core.windows.net/delivery_status.csv",header=True,inferSchema=True)

# Load customers
customers_df=spark.read.csv("wasbs://customerordercapstone@hestore.blob.core.windows.net/customers.csv",header=True,inferSchema=True)

orders_df.printSchema()
delivery_status_df.printSchema()
customers_df.printSchema()
orders_df.show()
delivery_status_df.show()
customers_df.show()


# COMMAND ----------

# 2. Clean and Format Data
from pyspark.sql.functions import to_date

# Format dates
orders_df_clean = orders_df.withColumn("order_date", to_date("order_date", "dd-MM-yyyy")).withColumn("delivery_date", to_date("delivery_date", "dd-MM-yyyy"))

delivery_status_df_clean = delivery_status_df.withColumn("status_date", to_date("status_date", "dd-MM-yyyy"))

# Optional: drop nulls or bad rows
orders_df_clean = orders_df_clean.dropna(subset=["order_id", "customer_id"])
delivery_status_df_clean = delivery_status_df_clean.dropna(subset=["order_id", "current_status"])
customers_df_clean = customers_df.dropna(subset=["customer_id"])

# 1. Cleaning and Formatting orders_df
# Drop rows with null order_id or customer_id
orders_df_clean = orders_df.dropna(subset=["order_id", "customer_id"])

# Convert order_date to proper DateType (assuming dd-MM-yyyy format)
from pyspark.sql.functions import to_date
orders_df_clean = orders_df_clean.withColumn("order_date", to_date("order_date", "dd-MM-yyyy"))

# Convert delivery_date to proper DateType
orders_df_clean = orders_df_clean.withColumn("delivery_date", to_date("delivery_date", "dd-MM-yyyy"))

# Remove rows with negative or zero amount
orders_df_clean = orders_df_clean.filter(orders_df_clean["amount"] > 0)

# Optional: Drop duplicates
orders_df_clean = orders_df_clean.dropDuplicates(["order_id"])

# Show cleaned orders
orders_df_clean.show()




# 2. Cleaning and Formatting delivery_status_df
# Drop rows with null order_id or current_status
delivery_status_df_clean = delivery_status_df.dropna(subset=["order_id", "current_status"])

# Format status_date to DateType
delivery_status_df_clean = delivery_status_df_clean.withColumn("status_date", to_date("status_date", "dd-MM-yyyy"))

# Optional: Remove duplicate entries, keeping the latest status
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_spec = Window.partitionBy("order_id").orderBy(desc("status_date"))
delivery_status_df_clean = delivery_status_df_clean.withColumn("row_num", row_number().over(window_spec))
delivery_status_df_clean = delivery_status_df_clean.filter("row_num = 1").drop("row_num")

# Show cleaned delivery status
delivery_status_df_clean.show()




# 3. Cleaning and Formatting customers_df
# Drop rows with null customer_id or email
customers_df_clean = customers_df.dropna(subset=["customer_id", "email"])

# Remove duplicate customers (keep first occurrence)
customers_df_clean = customers_df_clean.dropDuplicates(["customer_id"])

# Trim whitespaces from name and email
from pyspark.sql.functions import trim

customers_df_clean = customers_df_clean.withColumn("name", trim(customers_df_clean["name"]))
customers_df_clean = customers_df_clean.withColumn("email", trim(customers_df_clean["email"]))

# Optional: lowercase the email for consistency
from pyspark.sql.functions import lower

customers_df_clean = customers_df_clean.withColumn("email", lower(customers_df_clean["email"]))

# Show cleaned customers
customers_df_clean.show()


# COMMAND ----------

#  3. Join Orders with Latest Delivery Status
# Join orders with delivery status
orders_with_status_df = orders_df_clean.join(delivery_status_df_clean, on="order_id", how="left")
orders_with_status_df.show()

# COMMAND ----------

#  4. Save Result as Delta or CSV
# Save as Delta
orders_with_status_df.write.format("delta").mode("overwrite").save("/tmp/orders_with_status_delta")

# Save as CSV
orders_with_status_df.write.option("header", True).mode("overwrite").csv("/tmp/orders_with_status_csv")

# COMMAND ----------

# 5. Register as Temp Views
orders_with_status_df.createOrReplaceTempView("orders_status")
customers_df_clean.createOrReplaceTempView("customers")

# COMMAND ----------

# 6. SQL Query: Top 5 Delayed Customers
spark.sql("""select c.customer_id, c.name, c.email, count(*) as delayed_orders from orders_status o join customers c on o.customer_id = c.customer_id where o.current_status = 'Delayed' group by c.customer_id, c.name, c.email order by delayed_orders desc limit 5""").show()


# COMMAND ----------

#more sql queries
# 1. Total revenue per region
spark.sql("""select c.region, round(sum(o.amount), 2) as total_revenue from orders_status o join customers c on o.customer_id = c.customer_id group by c.region order by total_revenue desc""").show()

#  2. Count of orders by delivery status
spark.sql("""select current_status, count(*) as total_orders from orders_status group by current_status order by total_orders desc""").show()

# 3. Top 5 customers by total spending
spark.sql("""select c.customer_id, c.name, round(sum(o.amount), 2) as total_spent from orders_status o join customers c on o.customer_id = c.customer_id group by c.customer_id, c.name order by total_spent desc limit 5""").show()

# 4. Orders delivered late (delivery_date after status_date)
spark.sql("""select o.order_id, o.delivery_date, o.status_date, o.customer_id from orders_status o where o.current_status = 'delivered' and o.delivery_date > o.status_date""").show()

# 5. Average order amount per product
spark.sql("""select product_name, round(avg(amount), 2) as average_amount from orders_status group by product_name order by average_amount desc""").show()

# 6. Most purchased products (by order count)
spark.sql("""select product_name, count(*) as total_orders from orders_status group by product_name order by total_orders desc limit 5""").show()

# 7. Customers who placed more than 1 delayed order
spark.sql("""select c.customer_id, c.name, count(*) as delayed_orders from orders_status o join customers c on o.customer_id = c.customer_id where o.current_status = 'delayed' group by c.customer_id, c.name having count(*) > 1""").show()

