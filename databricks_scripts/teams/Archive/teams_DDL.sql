-- Databricks notebook source
 CREATE OR REPLACE TABLE dfl.nfl.bronze_teams (
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
      team_logo_squared STRING,
      is_current INT,
      insert_timestamp TIMESTAMP

) USING DELTA;

-- COMMAND ----------

 CREATE OR REPLACE TABLE dfl.nfl.gold_teams (
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
      team_logo_squared STRING,
      insert_timestamp TIMESTAMP

) USING DELTA;
