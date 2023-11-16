-- Databricks notebook source
CREATE  STREAMING LIVE TABLE bronze_weekly_dlt(
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams"
AS  SELECT * FROM
    cloud_files('abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/weekly/', 'parquet',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))
