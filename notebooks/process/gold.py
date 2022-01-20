# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  comment="Dataset for ML model"
)
def dataset():
    df_pers = dlt.read("personal_infos")
      .select('passengerId', 'firstname', 'lastname', 'age', 'survived')
    df_trip = dlt.read("trip_infos")
      .select('passengerId', 'class', 'ticketPrice', 'embarkedFrom')
    df = df_pers.join(df_trip, on='passengerId')
    return (
        df
)



