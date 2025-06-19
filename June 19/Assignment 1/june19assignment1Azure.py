# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("june19assignment1").getOrCreate()
spark

# COMMAND ----------

# Data Ingestion & Schema Analysis
# Load CSV using PySpark with schema inference
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","---------AccessKeyyy----------")

traffic_log_df=spark.read.csv("wasbs://june19assignment1@hestore.blob.core.windows.net/traffic_logs.csv",header=True,inferSchema=True)

traffic_log_df.printSchema()
# Manually define schema and compare
from pyspark.sql.types import *
from pyspark.sql.functions import *

traffic_log_schema=StructType([StructField("LogID",StringType(),True),StructField("VehicleID",StringType(),True),StructField("EntryPoint",StringType(),True),StructField("ExitPoint",StringType(),True),StructField("EntryTime",TimestampType(),True),StructField("ExitTime",TimestampType(),True),StructField("VehicleType",StringType(),True),StructField("SpeedKMH",IntegerType(),True),StructField("TollPaid",IntegerType(),True)])

traffic_log_df=spark.read.csv("wasbs://june19assignment1@hestore.blob.core.windows.net/traffic_logs.csv",header=True,schema=traffic_log_schema)
traffic_log_df.printSchema()
# Ensure EntryTime/ExitTime are timestamp
traffic_log_df=spark.read.csv("wasbs://june19assignment1@hestore.blob.core.windows.net/traffic_logs.csv",header=True,inferSchema=True)
traffic_log_df=traffic_log_df.withColumn("EntryTime",to_timestamp("EntryTime"))
traffic_log_df=traffic_log_df.withColumn("ExitTime",to_timestamp("ExitTime"))
traffic_log_df.printSchema()


# COMMAND ----------

# Derived Column Creation
# Calculate TripDurationMinutes = ExitTime - EntryTime
traffic_log_df=traffic_log_df.withColumn("TripDurationMinutes",round((unix_timestamp("ExitTime")-unix_timestamp("EntryTime"))/60))
# Add IsOverspeed = SpeedKMH > 60
traffic_log_df=traffic_log_df.withColumn("IsOverspeed",when(col("SpeedKMH")>60,1).otherwise(0))
traffic_log_df.printSchema()
traffic_log_df.show()

# COMMAND ----------

# Vehicle Behavior Aggregations
# Average speed per VehicleType
traffic_log_df.groupBy("VehicleType").agg(avg("SpeedKMH").alias("AvgSpeedKMH")).show()
# Total toll collected per gate (EntryPoint)
traffic_log_df.groupBy("ExitPoint").agg(sum("TollPaid").alias("TotalTollPaid")).show
# Most used ExitPoint
traffic_log_df.groupBy("ExitPoint").count().orderBy(desc("count")).show(1)

# COMMAND ----------

# Window Functions
# Rank vehicles by speed within VehicleType
from pyspark.sql.window import Window
traffic_log_df_window=traffic_log_df.withColumn("Rank",dense_rank().over(Window.partitionBy("VehicleType").orderBy(desc("SpeedKMH"))))
t=traffic_log_df_window
traffic_log_df_window.show()
# Find last exit time for each vehicle using lag()
traffic_log_df_window=traffic_log_df_window.withColumn("LastExitTime",lag("ExitTime",1).over(Window.partitionBy("VehicleID").orderBy(desc("ExitTime"))))
traffic_log_df_window.show()

# COMMAND ----------

# Session Segmentation
# Group by VehicleID to simulate route sessions
traffic_log_df_window=traffic_log_df_window.groupBy("VehicleID").agg(min("EntryTime").alias("SessionStart"),max("ExitTime").alias("SessionEnd"))
traffic_log_df_window.show()
# Find duration between subsequent trips (idle time)
traffic_log_df_window.withColumn("IdleTime",round((unix_timestamp("SessionEnd")-unix_timestamp("SessionStart"))/60)).show()


# COMMAND ----------

# Anomaly Detection
# Identify vehicles with speed > 70 and TripDuration < 10 minutes
t.filter((col("SpeedKMH")>70) & (col("TripDurationMinutes")<10)).show()
# Vehicles that paid less toll for longer trips
traffic_log_df=traffic_log_df.withColumn("TripTime",round((unix_timestamp("ExitTime")-unix_timestamp("EntryTime"))/60))
traffic_log_df.filter((col("TripTime")<60) & (col("TollPaid")<100)).show()
# Suspicious backtracking (ExitPoint earlier than EntryPoint)
traffic_log_df.filter((col("ExitTime")>col("EntryTime"))).show()

# COMMAND ----------

from pyspark.sql import Row

vehicle_data = [
    Row(VehicleID="V001", OwnerName="Anil", Model="Hyundai i20", RegisteredCity="Delhi"),
    Row(VehicleID="V002", OwnerName="Rakesh", Model="Tata Truck", RegisteredCity="Chennai"),
    Row(VehicleID="V003", OwnerName="Sana", Model="Yamaha R15", RegisteredCity="Mumbai"),
    Row(VehicleID="V004", OwnerName="Neha", Model="Honda City", RegisteredCity="Bangalore"),
    Row(VehicleID="V005", OwnerName="Zoya", Model="Volvo Bus", RegisteredCity="Pune"),
]

df_registry = spark.createDataFrame(vehicle_data)

# Join and group trips by RegisteredCity
df_joined = traffic_log_df.join(df_registry, on="VehicleID", how="left")
df_joined.groupBy("RegisteredCity").count().show()

# COMMAND ----------

# Q8_DeltaLakeFeatures

from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable

# Save as Delta
traffic_log_df.write.format("delta").mode("overwrite").save("dbfs:/Workspace/Shared/traffic_delta")

# Load Delta table
traffic_delta = DeltaTable.forPath(spark, "dbfs:/Workspace/Shared/traffic_delta")

# Merge: Update tolls for Bikes (+10)
traffic_delta.alias("old").merge(
    traffic_log_df.filter(col("VehicleType") == "Bike").withColumn("TollPaid", expr("TollPaid + 10")).alias("new"),
    "old.LogID = new.LogID"
).whenMatchedUpdate(set={"TollPaid": "new.TollPaid"}).execute()

# Delete trips > 60 minutes
traffic_delta.delete("TripDurationMinutes > 60")

# Describe history programmatically
traffic_delta.history().show(truncate=False)

# Read version 0
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("dbfs:/Workspace/Shared/traffic_delta")
df_v0.show()


# COMMAND ----------

# Advanced Conditions
# when/otherwise : Tag trip type as:
# "Short" <15min
# "Medium" 15-30min
# "Long" >30min
traffic_log_df_t2=traffic_log_df.withColumn("TripType",when(col("TripDurationMinutes")<15,"Short").when((col("TripDurationMinutes")>=15) & (col("TripDurationMinutes")<=30),"Medium").otherwise("Long"))
traffic_log_df_t2.show()
# Flag vehicles with more than 3 trips in a day
traffic_log_df_t=traffic_log_df.withColumn("TripDate",date_format("EntryTime","yyyy-MM-dd"))
traffic_log_df_t=traffic_log_df_t.groupBy("VehicleID","TripDate").count().withColumnRenamed("count","TripCount")
traffic_log_df_t=traffic_log_df_t.withColumn("IsMoreThan3Trips",when(col("TripCount")>3,1).otherwise(0))
traffic_log_df_t.show()

# COMMAND ----------

# Export & Reporting
# Write final enriched DataFrame to:
# Parquet partitioned by VehicleType
traffic_log_df.printSchema()

traffic_log_df.write.mode('overwrite').partitionBy("VehicleType").format("parquet").mode("overwrite").save("dbfs:/FileStore/shared_uploads/traffic_logs_enriched")
# CSV for dashboards
traffic_log_df.write.mode('overwrite').format("csv").mode("overwrite").save("dbfs:/FileStore/shared_uploads/traffic_logs_summary")
# Create summary SQL View: total toll by VehicleType + ExitPoint
spark.sql("CREATE OR REPLACE TEMP VIEW traffic_logs_summary AS SELECT VehicleType, ExitPoint, sum(TollPaid) AS TotalTollPaid FROM traffic_logs_delta GROUP BY VehicleType, ExitPoint")