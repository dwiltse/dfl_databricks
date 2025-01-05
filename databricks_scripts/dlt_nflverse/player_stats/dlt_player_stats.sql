-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Load Bronze data into dfl.nfl workspace
-- MAGIC ##### SOURCE:
-- MAGIC ##### abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nflverse/

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_player_stats(
    -- Set multi field primary key consisting of game_id and play_id
    CONSTRAINT pk_game_player_ids PRIMARY KEY (player_id, season, week, season_type)),
    CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams"
AS SELECT current_timestamp() processing_time, * FROM
    cloud_files('${source}/player_stats/', 'parquet',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))
