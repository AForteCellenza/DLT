# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  comment="Personal data cleaned and prepared."
)
def personal_infos():
    return (
        dlt.read("personal")
         .withColumn('firstname', split(col('Name'), '. ').getItem(2))
         .withColumn('lastname', split(col('Name'), '. ').getItem(0))
         .withColumnRenamed('PassengerId', 'passengerId')
         .withColumnRenamed('Sex', 'sex')
         .withColumnRenamed('Age', 'age')
         .withColumnRenamed('Survived', 'survived')
         .select('passengerId', 'firstname', 'lastname', 'sex', 'age', 'survived')           
                           ) 

@dlt.table(
  comment="Trip data cleaned and prepared."
)
def trip_infos():
    return (
        dlt.read("trip")
        .withColumnRenamed('PassengerId', 'passengerId')
        .withColumnRenamed('Pclass', 'class')
        .withColumnRenamed('Ticket', 'ticketNum')
        .withColumnRenamed('Fare', 'ticketPrice')
        .withColumnRenamed('Cabin', 'cabinNum')
        .withColumnRenamed('Embarked', 'embarkedFrom')
        .select('passengerId', 'class', 'ticketNum', 'ticketPrice', 'cabinNum', 'embarkedFrom') 
)

