-- Databricks notebook source
CREATE STREAMING LIVE TABLE gold_teams_dlt
COMMENT "current NFL teams info"
AS 
SELECT *
FROM STREAM(live.silver_teams_dlt)
WHERE is_current = 1
