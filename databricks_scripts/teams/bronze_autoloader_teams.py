# Databricks notebook source
# MAGIC %md
# MAGIC #### Load team data into bronze table using Autoloader

# COMMAND ----------

# DBTITLE 1,Autoloader Teams
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
