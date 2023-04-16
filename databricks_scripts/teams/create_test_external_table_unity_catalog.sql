-- Databricks notebook source
create catalog dfl;

-- COMMAND ----------

create database dfl.nfl

-- COMMAND ----------

create table dfl.nfl.teams
using csv
location 'abfss://root@sadfldatabricks.dfs.core.windows.net/raw/teams/'

-- COMMAND ----------

select * from dfl.nfl.teams

-- COMMAND ----------

drop table dfl.nfl.teams

-- COMMAND ----------

CREATE TABLE dfl.nfl.teams
USING CSV
OPTIONS (header "true", inferSchema "true")
LOCATION 'abfss://root@sadfldatabricks.dfs.core.windows.net/raw/teams/'

-- COMMAND ----------

select * from dfl.nfl.teams

-- COMMAND ----------

create view teams
using csv
location 'abfss://root@sadfldatabricks.dfs.core.windows.net/raw/teams/'

-- COMMAND ----------

drop table dfl.nfl.teams
