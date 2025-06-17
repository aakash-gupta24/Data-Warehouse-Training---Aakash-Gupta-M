# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("June17Assignment3").getOrCreate()
spark

# COMMAND ----------


# Data Ingestion & Schema Handling
# 1. Load the CSV using inferred schema.
spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","------------AccessKeyyy---------------------")

employee_timesheet_df=spark.read.csv("wasbs://june17assignment3@hestore.blob.core.windows.net/employee_timesheet.csv",header=True,inferSchema=True)
department_location_df=spark.read.csv("wasbs://june17assignment3@hestore.blob.core.windows.net/department_location.csv",header=True,inferSchema=True)

employee_timesheet_df.show()
department_location_df.show()
# 2. Load the same file with schema explicitly defined.
from pyspark.sql.types import *
employee_timesheet_schema=StructType([StructField("EmployeeId",IntegerType(),True),
                                      StructField("Name",StringType(),True),
                                      StructField("Department",StringType(),True),
                                      StructField("Project",StringType(),True),
                                      StructField("WorkDate",DateType(),True),
                                      StructField("Location",StringType(),True),
                                      StructField("Mode",StringType(),True)])
department_location_schema=StructType([StructField("Department",StringType(),True),
                                      StructField("Location",StringType(),True)])
# 3. Add a new column Weekday extracted from WorkDate .
from pyspark.sql.functions import *
employee_timesheet_df=employee_timesheet_df.withColumn("Weekday",date_format(employee_timesheet_df.WorkDate,"E"))
employee_timesheet_df.show()


# COMMAND ----------

# Aggregations & Grouping
# 4. Calculate total work hours by employee.
employee_timesheet_df.groupBy("EmployeeId").agg(sum("WorkHours").alias("TotalHours")).show()
# 5. Calculate average work hours per department.
employee_timesheet_df.groupBy("Department").agg(sum("WorkHours").alias("TotalHours")).orderBy(desc("TotalHours")).limit(2).show()
# 6. Get top 2 employees by total hours using window function.
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
w=Window.partitionBy().orderBy(desc("TotalHours"))
employee_timesheet_df.groupBy("EmployeeId").agg(sum("WorkHours").alias("TotalHours")).orderBy(desc("TotalHours")).limit(2).show()

# COMMAND ----------

# Date Operations
# 7. Filter entries where WorkDate falls on a weekend.
employee_timesheet_df.filter(employee_timesheet_df.Weekday.isin(["Sat","Sun"])).show()
# 8. Calculate running total of hours per employee using window.
from pyspark.sql.window import Window
from pyspark.sql.functions import sum,rank
w=Window.partitionBy("EmployeeId").orderBy(employee_timesheet_df.WorkDate)
employee_timesheet_df.withColumn("RunningTotal",sum("WorkHours").over(w)).show()


# COMMAND ----------

# Joining DataFrames
# 10. Join with timesheet data and list all employees with their DeptHead.
employee_timesheet_df.join(department_location_df,employee_timesheet_df.Department==department_location_df.Department,"inner").select(employee_timesheet_df.Name,department_location_df.DeptHead).show()

# COMMAND ----------

# Pivot & Unpivot
# 11. Pivot table: total hours per employee per project.
pivot_df=employee_timesheet_df.groupBy("EmployeeId","Project").pivot("Mode").sum("workHours")
# 12. Unpivot example: Convert mode-specific hours into rows.
from pyspark.sql.functions import expr

unpivot_df = pivot_df.selectExpr(
    "EmployeeId",
    "Project",
    "stack(2, 'Onsite', Onsite, 'Remote', Remote) as (Mode, Hours)"
)

unpivot_df.show()


# COMMAND ----------

# UDF & Conditional Logic
# 13. Create a UDF to classify work hours:

def workload_tag(hours):
    if hours >= 8: return "Full"
    elif hours >= 4: return "Partial"
    else: return "Light"
workload_tag_udf = udf(workload_tag,StringType())
employee_timesheet_df.withColumn("WorkloadCategory",workload_tag_udf(employee_timesheet_df.WorkHours)).show()
# 14. Add a column WorkloadCategory using this UDF.
employee_timesheet_df.withColumn("WorkloadCategory",workload_tag_udf(employee_timesheet_df.WorkHours)).show()

# COMMAND ----------

# Nulls and Cleanup
# 15. Introduce some nulls in Mode column.
employee_timesheet_df=employee_timesheet_df.withColumn("Mode",when(rand()<0.1,"").otherwise(employee_timesheet_df.Mode))
employee_timesheet_df.show()
# 16. Fill nulls with "Not Provided".
employee_timesheet_df=employee_timesheet_df.fillna("Not Provided",subset=["Mode"])
employee_timesheet_df.show()
# 17. Drop rows where WorkHours < 4.
employee_timesheet_df=employee_timesheet_df.filter(employee_timesheet_df.WorkHours>4)
employee_timesheet_df.show()

# COMMAND ----------

# Advanced Conditions
# 18. Use when-otherwise to mark employees as "Remote Worker" if >80% entries are
# Remote.
employee_timesheet_df=employee_timesheet_df.withColumn("RemoteWorker",when((employee_timesheet_df.Mode=="Remote") & (employee_timesheet_df.Mode=="Remote") & (employee_timesheet_df.Mode=="Remote") & (employee_timesheet_df.Mode=="Remote") & (employee_timesheet_df.Mode=="Remote"),"Yes").otherwise("No"))
employee_timesheet_df.show()
# 19. Add a new column ExtraHours where hours > 8.
employee_timesheet_df=employee_timesheet_df.withColumn("ExtraHours",when(employee_timesheet_df.WorkHours>8,employee_timesheet_df.WorkHours-8).otherwise(0))
employee_timesheet_df.show()

# COMMAND ----------

# Union + Duplicate Handling
# 20. Append a dummy timesheet for new interns using unionByName() .
employee_timesheet_df=employee_timesheet_df.unionByName(employee_timesheet_df.limit(1))
employee_timesheet_df.show()
# 21. Remove duplicate rows based on all columns.
employee_timesheet_df=employee_timesheet_df.dropDuplicates()
employee_timesheet_df.show()