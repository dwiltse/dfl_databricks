-- Databricks notebook source
CREATE  STREAMING LIVE TABLE bronze_teams_dlt(
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw team data from csv export of AFC and NFC teams"
AS  SELECT * FROM
    cloud_files('abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/teams/', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))
