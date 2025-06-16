# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('June16Assignment1').getOrCreate()
spark

# COMMAND ----------

# Data Loading
# 1. Load the data with schema inference enabled.
spark.conf.set(

  "fs.azure.account.key.hestore.blob.core.windows.net",

  "---------------AccessKeyyyy----------"

)
 
course_enr_df= spark.read.option("header", True).option("inferSchema", True).csv(

  "wasbs://june16assignment1@hestore.blob.core.windows.net/course_enrollments.csv"

)

course_det_df= spark.read.option("header", True).option("inferSchema", True).csv(

  "wasbs://june16assignment1@hestore.blob.core.windows.net/course_details.csv"

)
 
course_enr_df.show()
course_det_df.show()


# COMMAND ----------

# Data Loading
# 1. Load the data with schema inference enabled.
# 2. Manually define schema and compare both approaches.

# COMMAND ----------

# Filtering and Transformation
# 3. Filter records where ProgressPercent < 50.
course_enr_df.filter(course_enr_df.ProgressPercent < 50).show()
# 4. Replace null ratings with average rating.
from pyspark.sql.functions import avg; rating_mean = course_enr_df.agg(avg('Rating')).collect()[0][0]; course_enr_df.fillna({'Rating': rating_mean}).show()
# 5. Add column IsActive → 1 if Status is Active, else 0.
course_enr_df.withColumn('IsActive', course_enr_df.Status == 'Active').show()

# COMMAND ----------

# Aggregations & Metrics
# 6. Find average progress by course.
course_enr_df.groupBy('CourseName').agg({'ProgressPercent': 'avg'}).show()
# 7. Get count of students in each course category.
course_enr_df.groupBy('Category').agg({'StudentName': 'count'}).show()
# 8. Identify the most enrolled course.
course_enr_df.groupBy('CourseName').agg({'StudentName': 'count'}).orderBy('count(StudentName)', ascending=False).show()

# COMMAND ----------

# 10. Join course_enrollments with course_details to include duration and instructor.
course_enr_df.join(course_det_df, course_enr_df.CourseName == course_det_df.CourseName).show()

# COMMAND ----------

# Window Functions
# 11. Rank students in each course based on ProgressPercent.
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
windowSpec = Window.partitionBy('CourseName').orderBy('ProgressPercent')
course_enr_df.withColumn('rank', rank().over(windowSpec)).show()
# 12. Get lead and lag of EnrollDate by Category.
from pyspark.sql.functions import lag, lead
course_enr_df.withColumn('lead', lead('EnrollDate', 1).over(windowSpec)).withColumn('lag', lag('EnrollDate', 1).over(windowSpec)).show()

# COMMAND ----------

# Pivoting & Formatting
# 13. Pivot data to show total enrollments by Category and Status.
course_enr_df.groupBy('Category', 'Status').agg({'StudentName': 'count'}).groupBy('Category').pivot('Status').agg({'count(StudentName)': 'sum'}).show()
# 14. Extract year and month from EnrollDate.
from pyspark.sql.functions import year, month
course_enr_df.withColumn('year', year('EnrollDate')).withColumn('month', month('EnrollDate')).show()

# COMMAND ----------

# Cleaning and Deduplication
# 15. Drop rows where Status is null or empty.
course_enr_df.filter(course_enr_df.Status != '').show()
# 16. Remove duplicate enrollments using dropDuplicates().
course_enr_df.dropDuplicates().show()

# COMMAND ----------

# Export
# 17. Write the final cleaned DataFrame to:
# CSV (overwrite mode)
# JSON (overwrite mode)
# Parquet (snappy compression)
course_enr_df.write.mode('overwrite').csv('dbfs:/FileStore/tables/course_enrollments_cleaned.csv')
course_enr_df.write.mode('overwrite').json('dbfs:/FileStore/tables/course_enrollments_cleaned.json')
course_enr_df.write.mode('overwrite').option('compression', 'snappy').parquet('dbfs:/FileStore/tables/course_enrollments_cleaned.parquet')