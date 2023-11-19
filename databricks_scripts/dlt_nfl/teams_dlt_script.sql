-- Databricks notebook source
-- MAGIC %md #### Load Bronze data into dfl.nfl workspace
-- MAGIC
-- MAGIC ###### SOURCE: cloud_files('abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/teams/', 'csv',

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_teams(
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw team data from csv export of AFC and NFC teams"
AS  SELECT current_timestamp() processing_time,  * FROM
    cloud_files('${source}/teams/', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md #### Create Silver Materialized View - SCD Type 2
-- MAGIC

-- COMMAND ----------

-- Create the table
CREATE INCREMENTAL LIVE TABLE silver_teams
COMMENT 'Slowly Changing Dimension Type 2 for Teams';

-- Store all changes as SCD2
APPLY 
  CHANGES INTO live.silver_teams
  FROM stream(live.bronze_teams)
  KEYS(team_id)
  SEQUENCE BY processing_time
  COLUMNS *
  EXCEPT (_rescued_data)
  STORED AS SCD TYPE 2;

-- COMMAND ----------

-- OLD SILVER SCRIPT BEFORE SCD TYPE 2 LOGIC

--CREATE OR REFRESH LIVE TABLE silver_teams (
--  CONSTRAINT user_valid_id EXPECT (team_id IS NOT NULL) ON VIOLATION DROP ROW
--)
--TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "team_id")
--COMMENT "create silver layer where old team cities are flagged as not current teams"
--AS Select t.*,
--CASE
--WHEN team_abbr IN ('LA', 'STL', 'OAK', 'SD') THEN 0
--ELSE 1
--END as is_current,
--current_timestamp() as insert_timestamp
-- from live.bronze_teams t

-- COMMAND ----------

-- MAGIC %md #### Create Gold NFL Teams Table

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_teams
COMMENT "current NFL teams info"
AS 
SELECT *
FROM live.silver_teams
WHERE is_current = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CREATE PHYSICAL GOLD DELTA TABLE BASED ON MATERIALIZED VIEW
-- MAGIC
-- MAGIC CREATE TABLE dfl.nfl.teams
-- MAGIC USING DELTA
-- MAGIC AS SELECT *
-- MAGIC FROM live.gold_teams

-- COMMAND ----------


