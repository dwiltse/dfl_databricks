-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Merge silver tables into final gold teams table

-- COMMAND ----------

WITH silver_teams as 
(
SELECT *
FROM dfl.nfl.silver_teams 
WHERE is_current = 1
)
MERGE INTO dfl.nfl.gold_teams target USING silver_teams source
  ON target.team_abbr = source.team_abbr
  WHEN NOT MATCHED THEN INSERT *
