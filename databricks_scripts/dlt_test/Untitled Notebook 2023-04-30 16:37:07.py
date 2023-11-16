# Databricks notebook source
@dlt.table
def customers():
    return(
        spark.readstream.format("cloudfiles)
    )

# COMMAND ----------

checkpoint_path = "abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/_checkpoint/teams/"

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("cloudFiles.schemaHints", "team_id int")
  .load("abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/teams/")
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable("dfl.nfl.bronze_teams"))

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

@dlt.create_table(comment="load team data into bronze delta table")

def teams_dlt():
    return(
        spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("cloudFiles.schemaLocation", checkpoint_path)
         .option("cloudFiles.schemaHints", "team_id int")
         .load("abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/teams/"))

# COMMAND ----------


