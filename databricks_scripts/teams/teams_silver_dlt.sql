-- Databricks notebook source


-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_teams_dlt (
  CONSTRAINT user_valid_id EXPECT (team_id IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "team_id")
COMMENT "create silver layer where old team cities are flagged as not current teams"
AS Select t.*,
CASE
WHEN team_abbr IN ('LA', 'STL', 'OAK', 'SD') THEN 0
ELSE 1
END as is_current,
current_timestamp() as insert_timestamp
from STREAM(live.bronze_teams_dlt) t
