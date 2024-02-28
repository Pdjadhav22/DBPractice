# Databricks notebook source
# MAGIC %run /Workspace/Repos/user-scwfgltzalyd@oreilly-cloudlabs.com/Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

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

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_silver(
# MAGIC   CONSTRAINT vaild_order EXPECT(order_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC Comment "Order table cleaned with Valid order only" AS
# MAGIC Select order_id, quantity, b.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC CAST(from_unixtime(order_timestamp,'yy-MM-dd HH:mm:ss') as timestamp) order_timestamp, b.books,
# MAGIC c.profile:address:country as country
# MAGIC from STREAM(LIVE.order_bronze) b
# MAGIC Left Join Live.customers c
# MAGIC on b.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE order_gold
# MAGIC COMMENT "Gold Table for orders" AS
# MAGIC SELECT customer_id, f_name, l_name, date_trunc('DD',order_timestamp) order_date, sum(quantity) total_books
# MAGIC FROM Live.orders_silver
# MAGIC where country = 'china'
# MAGIC GROUP BY customer_id, f_name, l_name, date_trunc('DD',order_timestamp)

# COMMAND ----------


