# -*- coding: utf-8 -*-
"""Assignment 2.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/10-n34SO95yTaAClvAZcbRa6Tw9XdOxHh
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('June11Assignment2').getOrCreate()

spark

data = [
("Ananya", "HR", 52000),
("Rahul", "Engineering", 65000),
("Priya", "Engineering", 60000),
("Zoya", "Marketing", 48000),
("Karan", "HR", 53000),
("Naveen", "Engineering", 70000),
("Fatima", "Marketing", 45000)
]
columns = ["Name", "Department", "Salary"]
df_emp = spark.createDataFrame(data, columns)

performance = [
("Ananya", 2023, 4.5),
("Rahul", 2023, 4.9),
("Priya", 2023, 4.3),
("Zoya", 2023, 3.8),
("Karan", 2023, 4.1),
("Naveen", 2023, 4.7),
("Fatima", 2023, 3.9)
]
columns_perf = ["Name", "Year", "Rating"]
df_perf = spark.createDataFrame(performance, columns_perf)

#GroupBy and Aggregations
#1. Get the average salary by department.
df_emp.groupBy("Department").agg({"Salary": "avg"}).show()
#2. Count of employees per department.
df_emp.groupBy("Department").agg({"Name": "count"}).show()
#3. Maximum and minimum salary in Engineering.
df_emp.filter(df_emp["Department"] == "Engineering").agg({"Salary": "max"}).show()
df_emp.filter(df_emp["Department"] == "Engineering").agg({"Salary": "min"}).show()

# Join and Combine Data
#4. Perform an inner join between employee_data and performance_data on Name .
df_emp.join(df_perf, on="Name",how='inner').show()
#5. Show each employee’s salary and performance rating.
df_emp.join(df_perf, on="Name",how='inner').select("Name","Salary","Rating").show()
#6. Filter employees with rating > 4.5 and salary > 60000.
df_emp.join(df_perf, on="Name",how='inner').filter((df_perf["Rating"] > 4.5) & (df_emp["Salary"] > 60000)).show()

#Window & Rank (Bonus Challenge)
#7. Rank employees by salary department-wise.
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
windowSpec = Window.partitionBy("Department").orderBy(col("Salary").desc())
df_emp.withColumn("rank",rank().over(windowSpec)).show()
#8. Calculate cumulative salary in each department.
from pyspark.sql.functions import sum
df_emp.withColumn("cumulative_salary",sum(col("Salary")).over(windowSpec)).show()

#Date Operations
#9. Add a new column JoinDate with random dates between 2020 and 2023.
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import random
start_date = datetime(2020, 1, 1)
end_date = datetime(2023, 12, 31)
total_days = (end_date - start_date).days
random_dates = [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for _ in range(df_emp.count())]
df_emp = df_emp.withColumn("JoinDate", F.date_add(F.lit(start_date), (F.rand() * total_days).cast("int")))
df_emp.show()
#10. Add column YearsWithCompany using current_date() and datediff() .
from pyspark.sql.functions import current_date, datediff
df_emp = df_emp.withColumn("YearsWithCompany", datediff(current_date(), "JoinDate") / 365)
df_emp.show()

#Writing to Files
#11. Write the full employee DataFrame to CSV with headers.
df_emp.write.csv("employee_data.csv", header=True)
#12. Save the joined DataFrame to a Parquet file.
df_emp.join(df_perf, on="Name",how='inner').write.parquet("joined_data.parquet")

spark.stop()