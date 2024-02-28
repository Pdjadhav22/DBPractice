# Databricks notebook source
# MAGIC %run /Workspace/Repos/user-xqquomuwpziq@oreilly-cloudlabs.com/Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

# COMMAND ----------

demoPath = spark.conf.get('dataset.bookstore')
demoPath

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/demo-datasets/bookstore/orders-json-raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select '${dataset.bookstore}/orders-json-raw'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE order_bronze
# MAGIC COMMENT "Order table Raw at Bronze layer" AS
# MAGIC SELECT * FROM cloud_files('${dataset.bookstore}/orders-json-raw','json', map('cloudFiles.inferColumnTypes','true'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC COMMENT "customer static table" AS
# MAGIC SELECT * FROM JSON.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Table

# COMMAND ----------


