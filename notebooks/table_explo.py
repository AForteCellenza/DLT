# Databricks notebook source
dbutils.fs.ls("/pipelines/")

# COMMAND ----------

dbutils.fs.ls("/pipelines/8b0817be-a9b2-4e2c-89a0-0b0c1dee16f2/tables/")

# COMMAND ----------

df_raw = (spark.read.format("delta").load('/pipelines/8b0817be-a9b2-4e2c-89a0-0b0c1dee16f2/tables/clickstream_raw'))
display(df_raw)

# COMMAND ----------

df_prepared = (spark.read.format("delta").load('/pipelines/8b0817be-a9b2-4e2c-89a0-0b0c1dee16f2/tables/clickstream_prepared'))
display(df_prepared)

# COMMAND ----------

df = (spark.read.format("delta").load('/pipelines/8b0817be-a9b2-4e2c-89a0-0b0c1dee16f2/tables/top_spark_referers'))
display(df)

# COMMAND ----------

dbutils.fs.ls("/pipelines/8b0817be-a9b2-4e2c-89a0-0b0c1dee16f2/system/")

# COMMAND ----------

# pipeline logs
event_log_path = "/pipelines/8b0817be-a9b2-4e2c-89a0-0b0c1dee16f2/system/events/"
event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

display(spark.sql("select * from event_log_raw"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lineage
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets FROM event_log_raw WHERE event_type = 'flow_definition'

# COMMAND ----------

dbutils.fs.ls("/pipelines/8b0817be-a9b2-4e2c-89a0-0b0c1dee16f2/checkpoints/clickstream_prepared/")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls("/pipelines/5c674d0c-a870-47f0-b07b-f154b0c6168c/system/events/_delta_log")

# COMMAND ----------

display(spark.read.option(header=True).csv("/mnt/dlt/data/personal_infos.csv")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


