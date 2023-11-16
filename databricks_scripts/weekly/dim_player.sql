-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ### Create dim table to join back to fact tables based on stats

-- COMMAND ----------

CREATE LIVE TABLE qb_features
COMMENT "player dimensions"
AS 
select 
player_id,
player_name,
player_display_name,
position,
position_group,
recent_team,
season,
headshot_url
from live.bronze_weekly_dlt
