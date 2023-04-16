-- Databricks notebook source
-- MAGIC %md
-- MAGIC Create Temp View 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW temp_teams USING CSV OPTIONS (
path = 'abfss://root@sadfldatabricks.dfs.core.windows.net/raw/teams/',
header = "true",
mode = 'FAILFAST'
);


-- COMMAND ----------

select * from temp_teams

-- COMMAND ----------

create table dfl.nfl.teams
using delta
select * from temp_teams

-- COMMAND ----------

describe dfl.nfl.teams

-- COMMAND ----------

describe extended dfl.nfl.teams

-- COMMAND ----------

 CREATE OR REPLACE TABLE dfl.nfl.silver_teams (
      team_abbr STRING,
      team_name STRING,
      team_id STRING,
      team_nick STRING,
      team_conf STRING,
      team_division STRING,
      team_color STRING,
      team_color2 STRING,
      team_logo_wikipedia STRING,
      team_logo_espn STRING,
      team_wordmark STRING,
      team_conference_logo STRING,
      team_league_logo STRING,
      team_logo_squared STRING

) USING DELTA;

-- COMMAND ----------

describe dfl.nfl.teams

-- COMMAND ----------

select * from dfl.nfl.teams

-- COMMAND ----------

--drop table dfl.nfl.teams;

-- COMMAND ----------

-- View all tables in the schema
SHOW TABLES IN dfl.nfl;

-- COMMAND ----------

select * from dfl.nfl.teams

-- COMMAND ----------

-- MAGIC %md now set up auto loader

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Import functions
-- MAGIC from pyspark.sql.functions import input_file_name, current_timestamp
-- MAGIC 
-- MAGIC # Define variables used in code below
-- MAGIC file_path = "abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/teams"
-- MAGIC table_name = "teams"
-- MAGIC checkpoint_path = "abfss://root@sadfldatabricks.dfs.core.windows.net/raw/_checkpoint/nfl/teams"
-- MAGIC 
-- MAGIC # Configure Auto Loader to ingest JSON data to a Delta table
-- MAGIC (spark.readStream
-- MAGIC   .format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "csv")
-- MAGIC   .option("cloudFiles.schemaLocation", checkpoint_path)
-- MAGIC   .load(file_path)
-- MAGIC   .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
-- MAGIC   .writeStream
-- MAGIC   .option("checkpointLocation", checkpoint_path)
-- MAGIC   .trigger(availableNow=True)
-- MAGIC   .toTable(table_name))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC checkpoint_path = "abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/_checkpoint/teams/"
-- MAGIC 
-- MAGIC (spark.readStream
-- MAGIC   .format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "csv")
-- MAGIC   .option("cloudFiles.schemaLocation", checkpoint_path)
-- MAGIC   .load("abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/teams")
-- MAGIC   .writeStream
-- MAGIC   .option("checkpointLocation", checkpoint_path)
-- MAGIC   .option("mergeSchema", "true")
-- MAGIC   .trigger(availableNow=True)
-- MAGIC   .toTable("dfl.nfl.teams"))

-- COMMAND ----------

select * from dfl.nfl.teams

-- COMMAND ----------

describe extended dfl.nfl.teams

-- COMMAND ----------

select
team_nick,
count(1) cnt
from dfl.nfl.teams
group by team_nick
order by cnt desc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC checkpoint_path = "abfss://dfl-unity@sadfldatabricks.dfs.core.windows.net/7f0f1906-f3b3-4e33-8c20-7b2e7f4cd78a/_checkpoint/teams/"
-- MAGIC 
-- MAGIC ##"abfss://dfl-unity@sadfldatabricks.dfs.core.windows.net/_checkpoint/teams"
-- MAGIC 
-- MAGIC (spark.readStream
-- MAGIC   .format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "csv")
-- MAGIC   .option("cloudFiles.schemaLocation", checkpoint_path)
-- MAGIC   .load("abfss://root@sadfldatabricks.dfs.core.windows.net/raw/teams/")
-- MAGIC   .writeStream
-- MAGIC   .option("checkpointLocation", checkpoint_path)
-- MAGIC   .trigger(availableNow=True)
-- MAGIC   .trigger(once=True)
-- MAGIC   .toTable("dfl.nfl.teams"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from python.sql.types import DoubleTYpe, IntegerType, StringType, StructType, StructField
-- MAGIC 
-- MAGIC # Define variables used in code below
-- MAGIC file_path = "/databricks-datasets/songs/data-001/"
-- MAGIC table_name = "dfl.nfl.teams"
-- MAGIC checkpoint_path = "<checkpoint-path>"
-- MAGIC 
-- MAGIC # For purposes of this example, clear out data from previous runs. Because Auto Loader
-- MAGIC # is intended for incremental loading, in production applications you normally won't drop
-- MAGIC # target tables and checkpoints between runs.
-- MAGIC spark.sql(f"DROP TABLE IF EXISTS {table_name}")
-- MAGIC dbutils.fs.rm(checkpoint_path, True)
-- MAGIC 
-- MAGIC schema = StructType(
-- MAGIC   [
-- MAGIC     StructField("artist_id", StringType(), True),
-- MAGIC     StructField("artist_lat", DoubleType(), True),
-- MAGIC     StructField("artist_long", DoubleType(), True),
-- MAGIC     StructField("artist_location", StringType(), True),
-- MAGIC     StructField("artist_name", StringType(), True),
-- MAGIC     StructField("duration", DoubleType(), True),
-- MAGIC     StructField("end_of_fade_in", DoubleType(), True),
-- MAGIC     StructField("key", IntegerType(), True),
-- MAGIC     StructField("key_confidence", DoubleType(), True),
-- MAGIC     StructField("loudness", DoubleType(), True),
-- MAGIC     StructField("release", StringType(), True),
-- MAGIC     StructField("song_hotnes", DoubleType(), True),
-- MAGIC     StructField("song_id", StringType(), True),
-- MAGIC     StructField("start_of_fade_out", DoubleType(), True),
-- MAGIC     StructField("tempo", DoubleType(), True),
-- MAGIC     StructField("time_signature", DoubleType(), True),
-- MAGIC     StructField("time_signature_confidence", DoubleType(), True),
-- MAGIC     StructField("title", StringType(), True),
-- MAGIC     StructField("year", IntegerType(), True),
-- MAGIC     StructField("partial_sequence", IntegerType(), True)
-- MAGIC   ]
-- MAGIC )
-- MAGIC 
-- MAGIC (spark.readStream
-- MAGIC   .format("cloudFiles")
-- MAGIC   .schema(schema)
-- MAGIC   .option("cloudFiles.format", "csv")
-- MAGIC   .option("sep","\t")
-- MAGIC   .load(file_path)
-- MAGIC   .writeStream
-- MAGIC   .option("checkpointLocation", checkpoint_path)
-- MAGIC   .trigger(availableNow=True)
-- MAGIC   .toTable(table_name))
