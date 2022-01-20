# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

csv_path_pers_infos = "/mnt/dlt/data/personal_infos.csv"
csv_path_trip_infos = "/mnt/dlt/data/trip_infos.csv"
@dlt.table(
  comment="Passengers personal infos"
)
def personal():
    return (spark.read.options(header=True).csv(csv_path_pers_infos))

@dlt.table(
  comment="Passengers trip infos"
)
def trip():
    return (spark.read.options(header=True).csv(csv_path_trip_infos))


# COMMAND ----------

csv_path_pers_infos = "/mnt/dlt/data/personal_infos.csv"
csv_path_trip_infos = "/mnt/dlt/data/trip_infos.csv"
df_pers = spark.read.options(header=True).csv(csv_path_pers_infos)
df_trip = spark.read.options(header=True).csv(csv_path_trip_infos)

display(df_trip)

# COMMAND ----------

display((df_trip.withColumnRenamed('PassengerId', 'passengerId')
         .withColumnRenamed('Pclass', 'class')
         .withColumnRenamed('Ticket', 'ticketNum')
         .withColumnRenamed('Fare', 'ticketPrice')
         .withColumnRenamed('Cabin', 'cabinNum')
         .withColumnRenamed('Embarked', 'embarkedFrom')
         .select('passengerId', 'class', 'ticketNum', 'ticketPrice', 'cabinNum', 'embarkedFrom')           
                           ))

# COMMAND ----------

df_pers.select([count(when(isnull(c), c)).alias(c) for c in df_pers.columns]).show()
df_trip.select([count(when(isnull(c), c)).alias(c) for c in df_trip.columns]).show()

# COMMAND ----------


