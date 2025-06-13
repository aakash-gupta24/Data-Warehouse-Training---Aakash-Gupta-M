# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("June13Assignment2").getOrCreate()
spark

# COMMAND ----------

!pip install dlt

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import dlt

from pyspark.sql.functions import col
@dlt.table
def rea():
    spark.conf.set("fs.azure.account.key.hestore.blob.core.windows.net","t9GPzv3EUQuPiAc+xYOGz8ugxHJYyeq+mZwiYW3CowXMCr4j0H0sofY2yXGapzCyksI7PYl/rUDj+ASt2AFRBQ==")
    customers_df= spark.read.option("header", True).option("inferSchema", True).csv("wasbs://june13assignment1@hestore.blob.core.windows.net/customers.csv")
    return customers_df
df=rea()
display(df)