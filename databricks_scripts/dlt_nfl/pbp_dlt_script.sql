-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Load Bronze data into dfl.nfl workspace
-- MAGIC ##### SOURCE:
-- MAGIC ##### abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_pbp(
    -- Set multi field primary key consisting of game_id and play_id
    CONSTRAINT pk_game_play_ids PRIMARY KEY(old_game_id, play_id),
    CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams"
AS SELECT current_timestamp() processing_time, * FROM
    cloud_files('${source}/pbp/', 'parquet',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Filter the bronze data to only offensive plays and limit the fields from bronze table to only relevant ones
-- MAGIC

-- COMMAND ----------

-- Create the table
CREATE INCREMENTAL LIVE TABLE silver_pbp_offense(
CONSTRAINT user_valid_id EXPECT (play_id IS NOT NULL) ON VIOLATION DROP ROW
)

COMMENT 'Incremental Update of Offensive Data from Play-By-Play data ');

