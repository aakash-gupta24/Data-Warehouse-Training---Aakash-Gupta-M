# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("june19assignment2").getOrCreate()
spark

# COMMAND ----------

# Ingestion & Time Fields
# Load into PySpark with inferred schema
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","---------AccessKeyyy----------")

course_enrollments=spark.read.csv("wasbs://june19assignment2@hestore.blob.core.windows.net/course_enrollments.csv",header=True,inferSchema=True)

course_enrollments.printSchema()
course_enrollments.show()
# Convert EnrollDate and CompletionDate to date type
import pyspark.sql.functions as F
from pyspark.sql.functions import col,datediff,to_date
from pyspark.sql.functions import *
from pyspark.sql.types import *
course_enrollments=course_enrollments.withColumn("EnrollDate",to_date(col("EnrollDate"),"MM-dd-yyyy"))
# Add DaysToComplete column if completed
course_enrollments = course_enrollments.withColumn("DaysToComplete",when(col("ProgressPercent") == 100, datediff(col("CompletionDate"), col("EnrollDate"))).otherwise(0))
course_enrollments.show()

# COMMAND ----------

# User Learning Path Progress
# Group by UserID : count of courses enrolled
course_enrollments.groupBy("UserID").agg(count("*").alias("CourseCount")).show()
# Avg progress % across all enrollments
course_enrollments.groupBy("UserID").agg(avg("ProgressPercent").alias("AvgProgressPercent")).show()
# Flag IsCompleted = ProgressPercent = 100
course_enrollments=course_enrollments.withColumn("IsCompleted",when(col("ProgressPercent")==100,"Yes").otherwise("No"))
course_enrollments.show()

# COMMAND ----------

# Engagement Scoring
# Create a score: ProgressPercent * Rating (if not null)
course_enrollments=course_enrollments.withColumn("Score",col("ProgressPercent")*col("Rating"))
# Replace null Rating with 0 before computing
course_enrollments=course_enrollments.fillna(0,['Rating'])
course_enrollments.show()

# COMMAND ----------

# Identify Drop-offs
# Filter all records with ProgressPercent < 50 and CompletionDate is null
dropouts=course_enrollments.filter((col("ProgressPercent")<50) & (col("CompletionDate").isNull()))
# Create a view called Dropouts
dropouts.createOrReplaceTempView("Dropouts")

# COMMAND ----------

# Joins with Metadata
# Create course_catalog.csv :
# CourseID,Instructor,DurationHours,Level
# C001,Abdullah Khan,8,Beginner
# C002,Sana Gupta,5,Beginner
# C003,Ibrahim Khan,10,Intermediate
# C004,Zoya Sheikh,6,Beginner
course_catalog=spark.read.csv("wasbs://june19assignment2@hestore.blob.core.windows.net/course_catalog.csv",header=True,inferSchema=True)
# Join to find average progress per instructor
course_enrollments.join(course_catalog,["CourseID"]).groupBy("Instructor").agg(avg("ProgressPercent").alias("AvgProgressPercent")).show()
# Show who teaches the most enrolled course
course_enrollments.join(course_catalog,["CourseID"]).groupBy("Instructor").agg(count("CourseID").alias("CourseCount")).orderBy(col("CourseCount").desc()).show()

# COMMAND ----------

# Delta Lake Practice
# Save as Delta Table enrollments_delta
course_enrollments.write.mode('overwrite').format("delta").saveAsTable("enrollments_delta")
# Apply:
# Update: Set all ratings to 5 where Course = 'Python Basics'
# Delete: All rows where ProgressPercent = 0
# Show DESCRIBE HISTORY
spark.sql("UPDATE enrollments_delta SET Rating=5 WHERE CourseName='Python Basics'").show()
spark.sql("DELETE FROM enrollments_delta WHERE ProgressPercent=0").show()
spark.sql("DESCRIBE HISTORY enrollments_delta").show()

# COMMAND ----------

# Window Functions
# Use dense_rank() to rank courses by number of enrollments
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("UserID").orderBy("EnrollDate")
course_enrollments.withColumn("rank",dense_rank().over(windowSpec)).show()
# lead() to find next course by each user (sorted by EnrollDate)
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("UserID").orderBy("EnrollDate")
course_enrollments.withColumn("next_course",lead("CourseName",1).over(windowSpec)).show()


# COMMAND ----------

# SQL Logic for Dashboard Views
# Create views:
# daily_enrollments
# category_performance (avg rating by category)
# top_3_courses
daily_enrollments=spark.sql("SELECT date_format(EnrollDate,'yyyy-MM-dd') as Date,COUNT(*) as DailyEnrollments FROM enrollments_delta GROUP BY Date")

daily_enrollments.createOrReplaceTempView("daily_enrollments")

category_performance=spark.sql("SELECT Category,AVG(Rating) as AvgRating FROM enrollments_delta GROUP BY Category")

category_performance.createOrReplaceTempView("category_performance")

top_3_courses=spark.sql("SELECT CourseName,AVG(Rating) as AvgRating FROM enrollments_delta GROUP BY CourseName ORDER BY AvgRating DESC LIMIT 3")

top_3_courses.createOrReplaceTempView("top_3_courses")

# COMMAND ----------

# Time Travel
# View previous version before update/delete
spark.sql("SELECT * FROM enrollments_delta VERSION AS OF 0").show()
# Use VERSION AS OF and TIMESTAMP AS OF
df_time = spark.read.format("delta").option("timestampAsOf", "2025-06-19 06:25:52").table("enrollments_delta")

df_time.show()


# COMMAND ----------

# Export Reporting
# Write to JSON, partitioned by Category
course_enrollments.write.partitionBy("Category").mode("overwrite").format("json").save("dbfs:/FileStore/tables/assignment2/json")
# Create summary DataFrame:
# CourseName, TotalEnrollments, AvgRating, AvgProgress
summary=spark.sql("SELECT CourseName, COUNT(*) as TotalEnrollments, AVG(Rating) as AvgRating, AVG(ProgressPercent) as AvgProgress FROM enrollments_delta GROUP BY CourseName")
summary.show()
# Save as Parquet
summary.write.mode("overwrite").format("parquet").save("dbfs:/FileStore/tables/assignment2/parquet")