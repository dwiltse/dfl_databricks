-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Load Bronze data into dfl.nfl workspace
-- MAGIC ##### SOURCE:
-- MAGIC ##### abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nflverse/

-- COMMAND ----------

CREATE STREAMING TABLE bronze_player_stats(
    CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams"
AS SELECT current_timestamp() processing_time, * FROM
    cloud_files('${source}/player_stats/', 'parquet',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

CREATE MATERIALIZED VIEW silver_player_stats(
    CONSTRAINT non_negative_carries EXPECT (carries >= 0) ON VIOLATION DROP ROW,
    CONSTRAINT non_null_recent_team EXPECT (recent_team IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned player stats data with expectations"
AS SELECT * FROM LIVE.bronze_player_stats;

-- COMMAND ----------

CREATE MATERIALIZED VIEW dim_player
COMMENT "Dimension view for player information"
AS
SELECT DISTINCT
    player_id,
    player_name,
    player_display_name,
    position,
    position_group,
    headshot_url,
    recent_team
FROM LIVE.silver_player_stats;

-- COMMAND ----------

CREATE MATERIALIZED VIEW dim_game
COMMENT "Dimension view for game information"
AS
   SELECT DISTINCT
       season,
       week,
       season_type,
       opponent_team
   FROM LIVE.silver_player_stats;

-- COMMAND ----------

CREATE MATERIALIZED VIEW fact_player_stats
COMMENT "Fact table for game information"
AS
   SELECT
       player_id,
       recent_team,
       season,
       week,
       season_type,
       completions,
       attempts,
       passing_yards,
       passing_tds,
       interceptions,
       sacks,
       sack_yards,
       sack_fumbles,
       passing_air_yards,
       passing_yards_after_catch,
       passing_first_downs,
       carries,
       rushing_yards,
       rushing_tds,
       rushing_fumbles,
       rushing_fumbles_lost,
       rushing_first_downs,
       receptions,
       targets,
       receiving_yards,
       receiving_tds,
       receiving_air_yards,
       receiving_yards_after_catch,
       fantasy_points
   FROM LIVE.silver_player_stats;
