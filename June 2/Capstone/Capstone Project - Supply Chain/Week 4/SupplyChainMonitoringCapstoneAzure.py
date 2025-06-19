# Databricks notebook source
from pyspark.sql import SparkSession
saprk=SparkSession.builder.appName("SupplyChainMonitoringCapstone").getOrCreate()
spark

# COMMAND ----------

#Read the CSV Files using PySpark
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","t9GPzv3EUQuPiAc+xYOGz8ugxHJYyeq+mZwiYW3CowXMCr4j0H0sofY2yXGapzCyksI7PYl/rUDj+ASt2AFRBQ==")

# Read inventory.csv
inventory_df=spark.read.csv("wasbs://supplychaincapstone@hestore.blob.core.windows.net/inventory.csv",header=True,inferSchema=True)

# Read suppliers.csv
suppliers_df=spark.read.csv("wasbs://supplychaincapstone@hestore.blob.core.windows.net/suppliers.csv",header=True,inferSchema=True)

# Read orders.csv
orders_df=spark.read.csv("wasbs://supplychaincapstone@hestore.blob.core.windows.net/orders.csv",header=True,inferSchema=True)

inventory_df.printSchema()
suppliers_df.printSchema()
orders_df.printSchema()

# COMMAND ----------

#clean and filter data
# Drop rows with nulls
inventory_df_clean = inventory_df.dropna()
suppliers_df_clean = suppliers_df.dropna()
orders_df_clean = orders_df.dropna()

# Filter orders with valid supplier_id
valid_supplier_ids = [row['supplier_id'] for row in suppliers_df.select("supplier_id").collect()]
orders_df_clean = orders_df_clean.filter(orders_df_clean['supplier_id'].isin(valid_supplier_ids))

# On inventory_df
#1. Remove duplicate rows
inventory_df_clean = inventory_df.dropDuplicates()

# 2. Filter items with zero or negative quantity/price
inventory_df_clean = inventory_df_clean.filter("quantity > 0 AND price > 0")

# 3. Standardize column names (lowercase, no spaces)
for col in inventory_df_clean.columns:
    inventory_df_clean = inventory_df_clean.withColumnRenamed(col, col.lower().replace(" ", "_"))

# 4. Detect missing supplier_ids (foreign key check)
from pyspark.sql.functions import col

valid_supplier_ids_df = suppliers_df.select("supplier_id").distinct()
inventory_df_clean = inventory_df_clean.join(valid_supplier_ids_df, on="supplier_id", how="inner")


# On suppliers_df
# 1. Trim and standardize text fields
from pyspark.sql.functions import trim, lower

suppliers_df_clean = suppliers_df.withColumn("supplier_name", trim(lower(col("supplier_name"))))

# 2. Validate emails
from pyspark.sql.functions import regexp_extract

# Extract email pattern; valid if match is non-empty
suppliers_df_clean = suppliers_df_clean.withColumn(
    "valid_email",
    regexp_extract(col("contact_email"), r"^[\w\.-]+@[\w\.-]+\.\w+$", 0)
).filter("valid_email != ''")


# On orders_df
# 1. Remove future dates or invalid formats
from pyspark.sql.functions import to_date, current_date

orders_df_clean = orders_df.withColumn("order_date", to_date("order_date"))
orders_df_clean = orders_df_clean.filter(col("order_date") <= current_date())

# 2. Filter for only allowed statuses
valid_statuses = ['pending', 'shipped', 'delivered']
orders_df_clean = orders_df_clean.filter(col("status").isin(valid_statuses))

# 3. Remove orders with unrealistic quantities (e.g., negative or too high)
orders_df_clean = orders_df_clean.filter((col("quantity") > 0) & (col("quantity") <= 1000))

# 4. Remove orders with missing/invalid supplier IDs
orders_df_clean = orders_df_clean.join(valid_supplier_ids_df, on="supplier_id", how="inner")



# COMMAND ----------

#Save Cleaned Data as Delta or CSV
#delta
inventory_df_clean.write.format("delta").mode("overwrite").save("/tmp/inventory_clean_delta")
suppliers_df_clean.write.format("delta").mode("overwrite").save("/tmp/suppliers_clean_delta")
orders_df_clean.write.format("delta").mode("overwrite").save("/tmp/orders_clean_delta")

#csv
inventory_df_clean.write.option("header", True).mode("overwrite").csv("/tmp/inventory_clean_csv")
suppliers_df_clean.write.option("header", True).mode("overwrite").csv("/tmp/suppliers_clean_csv")
orders_df_clean.write.option("header", True).mode("overwrite").csv("/tmp/orders_clean_csv")



# COMMAND ----------

# Register as Temp Views
inventory_df_clean.createOrReplaceTempView("inventory")
suppliers_df_clean.createOrReplaceTempView("suppliers")
orders_df_clean.createOrReplaceTempView("orders")

# total quantity of all inventory
saprk.sql("select sum(quantity) as total_inventory_quantity from inventory").show()

# join orders with supplier info
spark.sql("select o.order_id, o.status, s.supplier_name from orders o join suppliers s on o.supplier_id = s.supplier_id").show()

# count of orders per status
spark.sql("select status, count(*) as order_count from orders group by status").show()

# 1. total quantity of all inventory
spark.sql("""select sum(quantity) as total_inventory_quantity from inventory""").show()

# 2. total inventory value per item
spark.sql("""select item_name, quantity, price, (quantity * price) as total_value from inventory""").show()

# 3. total value of inventory per supplier
spark.sql("""select s.supplier_name, sum(i.quantity * i.price) as total_inventory_value from inventory i join suppliers s on i.supplier_id = s.supplier_id group by s.supplier_name""").show()

# 4. number of orders per supplier
spark.sql("""select s.supplier_name, count(o.order_id) as total_orders from orders o join suppliers s on o.supplier_id = s.supplier_id group by s.supplier_name order by total_orders desc""").show()

# 5. number of orders by status
spark.sql("""select status, count(*) as status_count from orders group by status""").show()

# 6. average quantity per order by supplier
spark.sql("""select s.supplier_name, round(avg(o.quantity), 2) as avg_order_quantity from orders o join suppliers s on o.supplier_id = s.supplier_id group by s.supplier_name""").show()

# 7. orders placed in the last 30 days
spark.sql("""select * from orders where order_date >= date_sub(current_date(), 30)""").show()

# 8. suppliers without inventory items
spark.sql("""select s.* from suppliers s left join inventory i on s.supplier_id = i.supplier_id where i.supplier_id is null""").show()

# 9. suppliers who have never delivered any order
spark.sql("""select s.* from suppliers s left join ( select distinct supplier_id from orders where status = 'delivered' ) d on s.supplier_id = d.supplier_id where d.supplier_id is null""").show()

# 10. total quantity ordered per supplier (all statuses)
spark.sql("""select s.supplier_name, sum(o.quantity) as total_ordered_quantity from orders o join suppliers s on o.supplier_id = s.supplier_id group by s.supplier_name order by total_ordered_quantity desc""").show()
