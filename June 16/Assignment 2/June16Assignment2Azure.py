# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('June16Assignment2').getOrCreate()
spark

# COMMAND ----------

# Data Loading
# 1. Load the data with schema inference enabled.
spark.conf.set(

  "fs.azure.account.key.hestore.blob.core.windows.net",

  "t9GPzv3EUQuPiAc+xYOGz8ugxHJYyeq+mZwiYW3CowXMCr4j0H0sofY2yXGapzCyksI7PYl/rUDj+ASt2AFRBQ=="

)
 
subscriptions_df= spark.read.option("header", True).option("inferSchema", True).csv(

  "wasbs://june16assignment2@hestore.blob.core.windows.net/subscriptions.csv"

)

user_activity_df= spark.read.option("header", True).option("inferSchema", True).csv(

  "wasbs://june16assignment2@hestore.blob.core.windows.net/user_activity.csv"

)
 
subscriptions_df.show()
user_activity_df.show()


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Subscription Engagement Score (Real Metric Modeling)
# Combine both datasets.
subscriptions_user_activity_df = subscriptions_df.join(user_activity_df, "UserID")
subscriptions_user_activity_df.show()
# Calculate:
# active_days = EndDate - StartDate
# events_per_user = count(EventType) grouped by UserID
# Create a score: engagement_score = (events_per_user / active_days) * PriceUSD
subscriptions_user_activity_df = subscriptions_user_activity_df.withColumn("active_days", datediff("EndDate", "StartDate")).withColumn("events_per_user", count("EventType").over(Window.partitionBy("UserID"))).withColumn("engagement_score", (col("events_per_user") / col("active_days")) * col("PriceUSD")).withColumn("engagement_score", round(col("engagement_score"), 2)).withColumn("engagement_score", col("engagement_score").cast("double")).withColumnRenamed("UserID", "id").withColumnRenamed("SubscriptionName", "subscription").withColumnRenamed("StartDate", "start_date").withColumnRenamed("EndDate", "end_date").withColumnRenamed("PriceUSD", "price")
subscriptions_user_activity_df.show()


# COMMAND ----------

# Anomaly Detection via SQL
# Identify users with:
# Subscription inactive but recent activity AutoRenew is true but no events in Use SQL views to expose this logic.
# B.Register the views
subscriptions_df.createOrReplaceTempView("subscriptions")
user_activity_df.createOrReplaceTempView("user_activity")
#B1. Identify Users with:
spark.sql("""
CREATE OR REPLACE TEMP VIEW recent_inactive_users AS SELECT DISTINCT s.UserID FROM subscriptions s JOIN user_activity u ON s.UserID = u.UserID WHERE s.IsActive = false;""")
# View the results
spark.sql("SELECT * FROM recent_inactive_users").show()


#AutoRenew true but no events in last 30 days ---
spark.sql("""CREATE OR REPLACE TEMP VIEW stale_autorenew_users AS SELECT s.UserID FROM subscriptions s LEFT JOIN user_activity u ON s.UserID = u.UserID WHERE s.AutoRenew = true GROUP BY s.UserID HAVING MAX(to_date(u.EventTime)) < current_date() - interval 30 days""")

# View the results
spark.sql("SELECT * FROM stale_autorenew_users").show()

# COMMAND ----------

# Delta Lake + Merge Simulation
# Imagine a billing fix needs to be applied:
# For all Pro plans in March, increase price by $5 retroactively.
# Use MERGE INTO on Delta table to apply the change.
subscriptions_user_activity_df.write.format("delta").mode("overwrite").save("/tmp/subscriptions_user_activity_delta")

# COMMAND ----------

# D. Time Travel Debugging
# Show describe history of the table before and after the billing fix.
# Query using VERSION AS OF to prove the issue existed.
display(spark.sql("DESCRIBE HISTORY delta.`/tmp/subscriptions_user_activity_delta`"))

# COMMAND ----------

# Build Tier Migration Table
# Identify users who upgraded:
# From Basic → Pro → Premium
# Use PySpark with lag() function to model this.
subscriptions_user_activity_df = subscriptions_user_activity_df.withColumn("previous_plan", lag("PlanType").over(Window.partitionBy("id").orderBy("start_date"))).withColumn("previous_plan", when(col("previous_plan").isNull(), "Basic").otherwise(col("previous_plan")))
subscriptions_user_activity_df.show()

# COMMAND ----------

# Power Users Detection
# Define a power user as:
# Used ≥ 2 features
# Logged in ≥ 3 times
# Create a separate Delta table power_users
subscriptions_user_activity_df.createOrReplaceTempView("subscriptions_user_activity")
spark.sql("""
CREATE OR REPLACE TABLE power_users AS
SELECT DISTINCT id
FROM subscriptions_user_activity
GROUP BY id
HAVING count(DISTINCT FeatureUsed) >= 2 AND count(DISTINCT EventTime) >= 3
""").show()

# COMMAND ----------

# Session Replay View
# Build a user session trace table using:
# Window.partitionBy("UserID").orderBy("EventTime")
# Show how long each user spent between login and logout events.
from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window
window_spec = Window.partitionBy("id").orderBy("EventTime")

subscriptions_user_activity_df = subscriptions_user_activity_df \
    .withColumn("session_num", row_number().over(window_spec)) \
    .withColumn("session",
        when(col("EventType") == "login", col("session_num") - 1)
        .when(col("EventType") == "logout", col("session_num") + 1)
        .otherwise(col("session_num"))
    )

subscriptions_user_activity_df.show()