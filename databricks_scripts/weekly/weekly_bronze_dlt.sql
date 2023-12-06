-- Databricks notebook source
-- MAGIC %md #### Load Bronze data into dfl.nfl workspace
-- MAGIC
-- MAGIC ###### SOURCE:
-- MAGIC ###### abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/

-- COMMAND ----------

CREATE  STREAMING LIVE TABLE bronze_weekly_dlt(
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams"
AS  SELECT * FROM
    cloud_files('${source}/weekly/', 'parquet',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create QB Feature table joined to teams table

-- COMMAND ----------

CREATE LIVE TABLE qb_features
COMMENT "Final QB data "
AS 
  WITH qb_agg_stats AS
 (
SELECT 
player_id,
player_name,
player_display_name,
position_group,
recent_team as team_abbr,
season,
season_Type,
SUM(completions) as tot_completions,
SUM(attempts) as tot_attemps,
SUM(passing_yards) as tot_passing_yds,
AVG(passing_yards) as avg_passing_yds,
SUM(passing_tds) as tot_passing_tds,
SUM(interceptions) as tot_int,
SUM(sacks) as tot_qb_sacks
 FROM live.bronze_weekly
 where position_group = 'QB'
 group by
 player_id,
player_name,
player_display_name,
position_group,
recent_team,
season,
season_Type
  )

select
team_abbr,
team_name,
Team_id,
team_conf,
team_division,
player_id,
player_name,
player_display_name,
position_group,
season,
season_Type,
tot_completions,
tot_attemps,
tot_passing_yds,
avg_passing_yds,
tot_passing_tds,
tot_int,
tot_qb_sacks,
team_logo_wikipedia as team_logo
from live.gold_teams_dlt
inner join qb_agg_stats using (team_abbr)

