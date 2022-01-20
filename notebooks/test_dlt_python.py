# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

json_path = "/path/to/my/data.json"
@dlt.table(
  comment="Dataset, ingested from /data.json."
)
def read_data():
    return (spark.read.json(json_path))

@dlt.table(
  comment="Data cleaned and prepared."
)
def function_for_preprocess():
    return (
        dlt.read("read_data")
      .withColumnRenamed("col2", "thing")
      .select("col1", "col2")
    )

@dlt.table(
  comment="A table that contains things."
)
def other_function():
    return (
        dlt.read("clickstream_prepared")
      .filter(expr("col1 == 'stuff'"))
      .select("col1", "col2")
)

# COMMAND ----------


