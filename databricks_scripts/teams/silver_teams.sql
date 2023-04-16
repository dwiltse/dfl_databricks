-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Create Temp View that adds "is_current" flag and then inserts that view into final gold 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW silver_teams as
Select t.*,
CASE
WHEN team_abbr IN ('LA', 'STL', 'OAK', 'SD') THEN 0
ELSE 1
END as is_current,
current_timestamp() as insert_timestamp
from dfl.nfl.bronze_teams t
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Merge new records into silver table

-- COMMAND ----------

 MERGE INTO dfl.nfl.silver_teams target USING silver_teams source
  ON target.team_abbr = source.team_abbr
  WHEN NOT MATCHED THEN INSERT *
