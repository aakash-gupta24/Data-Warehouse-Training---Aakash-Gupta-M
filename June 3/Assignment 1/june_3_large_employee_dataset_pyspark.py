# -*- coding: utf-8 -*-
"""June 3 large_employee_dataset pyspark.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1xZp5nLh3MUnY5fK9a9qS3n8WmM8Hl12T
"""

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Colab PySpark Setup").getOrCreate()

spark


#mount google drive
from google.colab import drive
from pyspark.sql.functions import year
drive.mount('/content/drive')

#read csv
sales_df=spark.read.csv('/content/drive/MyDrive/large_employee_dataset.csv',header=True,inferSchema=True)

#1.show top 10 rows
sales_df.show(10)

#2.count total no of employees(records)
sales_df.count()

#3.display unique department
sales_df.select("Department").distinct().show()

#4. Filter all employees in the "IT" department.
sales_df.filter(sales_df["Department"]=="IT").show()

#5. Show employees aged between 30 and 40.
sales_df.filter((sales_df["Age"] > 30) & (sales_df["Age"] < 40)).show()

#6. Sort employees by Salary in descending order.
sales_df.orderBy(sales_df["Salary"].desc()).show()

#7. Get the average salary by department.
sales_df.groupBy("Department").avg("Salary").show()

#8. Count of employees by Status.
sales_df.groupBy("Status").count().show()

#9. Highest salary in each city.
sales_df.groupBy("City").max("Salary").show()

#10. Total number of employees who joined each year.
sales_df.groupBy(year("JoiningDate")).count().show()

#11. Department-wise count of employees who are currently "Active".
sales_df.filter(sales_df["Status"]=="Active").groupBy("Department").count().show()

#12. Average age of employees per department.
sales_df.groupBy("Department").avg("Age").show()

#13. Create another dataset with City and Region , and join it.
city_df=spark.read.csv('/content/drive/MyDrive/location.csv',header=True,inferSchema=True)
sales_df=sales_df.join(city_df,sales_df["City"]==city_df["City"],"inner")
sales_df.show()

#14. Group salaries by Region after the join.
sales_df.groupBy("Region").sum("Salary").show()

#15. Calculate years of experience for each employee (current date - JoiningDate).
from pyspark.sql.functions import current_date, datediff, col
sales_df = sales_df.withColumn("Experience", datediff(current_date(), col("JoiningDate")) / 365)
sales_df.show()

#16. List all employees with more than 5 years of experience.
sales_df.filter(sales_df["Experience"] > 5).show()