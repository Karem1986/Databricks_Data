# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/karem.ortiz@cgi.com/products2.csv")

#  Creating our delta lake table
# 
# Save the dataframe as a delta table
delta_table_path = "/delta/products2.csv"
df1.write.format("delta").save(delta_table_path)



# COMMAND ----------

display(df1)
