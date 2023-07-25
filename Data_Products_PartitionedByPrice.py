# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/karem.ortiz@cgi.com/products.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.saveAsTable("products", partitionBy="ListPrice")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Category
# MAGIC FROM products
# MAGIC
