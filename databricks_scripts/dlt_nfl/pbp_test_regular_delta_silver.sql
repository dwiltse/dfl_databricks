-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Load Bronze data into dfl.nfl workspace
-- MAGIC ##### SOURCE:
-- MAGIC ##### abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC CREATE OR REFRESH STREAMING LIVE TABLE bronze_pbp_new
-- MAGIC (
-- MAGIC     processing_time TIMESTAMP,
-- MAGIC     play_id FLOAT NOT NULL,
-- MAGIC     old_game_id STRING NOT NULL
-- MAGIC    -- CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
-- MAGIC )
-- MAGIC COMMENT "raw weekly data from parquet export of AFC and NFC teams"
-- MAGIC AS SELECT current_timestamp() processing_time, play_id, old_game_id FROM
-- MAGIC     cloud_files('${source}/pbp/', 'parquet',
-- MAGIC                  map('header', 'true', 
-- MAGIC                      'inferSchema', 'true', 
-- MAGIC                      'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

CREATE STREAMING TABLE bronze_pbp_new2(
    CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams"
AS SELECT current_timestamp() processing_time, * FROM
    cloud_files('${source}/pbp/', 'parquet',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE SILVER TABLE (NOT DELTA LIVE TABLE) WITH KEYS AND CONSTRAINTS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- Create the Silver Table
-- MAGIC CREATE TABLE IF NOT EXISTS silver_pbp_new
-- MAGIC (
-- MAGIC     dfl_key STRING NOT NULL COMMENT 'composite key of play_id and old_game_id'
-- MAGIC     ,play_id STRING NOT NULL COMMENT 'The unique id of the play for each game'
-- MAGIC     ,game_id STRING NOT NULL COMMENT 'The unique id of the game that denotes year, the week, and the teams involved'
-- MAGIC     ,old_game_id STRING NOT NULL COMMENT 'The unique id of game that has the YYYY-MM-DD the game was played along with 2 digit identifier'
-- MAGIC     ,posteam STRING COMMENT 'The three letter abbreviation of the team on offense who posseses the ball. Is null when a kickoff is involved'
-- MAGIC     ,postteam_type STRING COMMENT 'denotes whether the team possessing the ball is playing at home, away or netural field'
-- MAGIC     ,qtr INT COMMENT 'the quarter of the game'
-- MAGIC     ,down INT COMMENT 'the down of the play (1,2,3,4) '
-- MAGIC     ,goal_to_go BOOLEAN COMMENT 'boolean identifier if the play is goal to go or not'
-- MAGIC     ,time STRING COMMENT 'the time showing on the game clock (ex 14:44)'
-- MAGIC     ,ydstogo INT COMMENT 'The number of yards to go for the play to get a first down, ranging from 0 to 99 yards'
-- MAGIC     ,ydsnet INT COMMENT 'The yards remaining to the end zone (ex - If on the oppponents 25 yard line, they have 75 ydsnet)'
-- MAGIC     ,play_desc STRING COMMENT 'text description of play'
-- MAGIC     ,play_type STRING COMMENT 'the type of play (examples: run, pass, punt, field goal)'
-- MAGIC     ,passer_player_id STRING COMMENT'the player_id for the passer on the play'
-- MAGIC     ,receiver_player_id STRING COMMENT 'The player_id for the receiver on the play'
-- MAGIC     ,rusher_player_id STRING COMMENT 'The player_id for the rusher on the play'
-- MAGIC     ,pass BOOLEAN COMMENT 'boolean identifier if play was a pass or not'
-- MAGIC     ,rush BOOLEAN COMMENT 'boolean identifier if play was a rush or not'
-- MAGIC      ,touchdown BOOLEAN COMMENT 'Touchdown indicator'
-- MAGIC     ,shotgun BOOLEAN COMMENT 'boolean identifier if play was in shotgun formation or not'
-- MAGIC     ,no_huddle BOOLEAN COMMENT 'boolean identifier if play was no huddle or not'
-- MAGIC     ,qb_dropback BOOLEAN COMMENT 'boolean identifier if quarterback dropped back on play or not'
-- MAGIC     ,qb_kneel BOOLEAN COMMENT 'boolean identifier if quarterback kneeled down or not'
-- MAGIC     ,qb_spike BOOLEAN COMMENT 'boolean identifier if quarterback spiked the ball to stop the clock or not'
-- MAGIC     ,qb_scramble BOOLEAN COMMENT 'boolean identifier if quarterback scrambled with the ball on a passing play that turned into a run or not'
-- MAGIC     ,pass_length STRING COMMENT 'description of type of pass: short, deep or not a pass'
-- MAGIC     ,pass_location STRING COMMENT 'location of pass: left, right, middle or null'
-- MAGIC     ,run_location STRING COMMENT 'location of run: left, right, middle or null'
-- MAGIC     ,run_gap STRING COMMENT 'location of run gap on rushing play: guard, tackle, center or null'
-- MAGIC     ,yards_gained INT COMMENT 'number of yards gained on a play, from negative to 99 yards'
-- MAGIC     ,air_yards INT COMMENT 'number of yards in the air for a pass'
-- MAGIC     ,yards_after_catch INT COMMENT 'number of yards the ball was carried by reciever after catching the ball'
-- MAGIC     ,ep DOUBLE COMMENT 'EP rating'
-- MAGIC     ,epa DOUBLE COMMENT 'EPA rating'
-- MAGIC     ,passing_yards INT COMMENT 'passing yards for the play'
-- MAGIC     ,receiving_yards INT COMMENT 'recieving yards for the play'
-- MAGIC     ,rushing_yards INT COMMENT 'rushing yards for the play'
-- MAGIC     ,penalty_yards INT COMMENT 'penalty yards for the play'
-- MAGIC     ,drive_play_count INT COMMENT 'number of plays on the drive'
-- MAGIC     ,drive_time_of_possession STRING COMMENT 'time of possession for the drive'
-- MAGIC     ,drive_first_downs INT COMMENT 'number of first downs on the drive'
-- MAGIC     ,drive_yards_penalized INT COMMENT 'yards penalized during the drive'
-- MAGIC     ,PRIMARY KEY (dfl_key)
-- MAGIC )
-- MAGIC COMMENT "Incremental Refresh of Regular Delta Table  from Bronze Table due to issues with trying to connect materialized views directly to a STREAMING LIVE TABLE (formerly INCREMENTAL LIVE TABLES)";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC CREATE OR REFRESH STREAMING LIVE TABLE silver_pbp_new2
-- MAGIC (
-- MAGIC     processing_time TIMESTAMP,
-- MAGIC     play_id FLOAT NOT NULL,
-- MAGIC     old_game_id STRING NOT NULL
-- MAGIC    -- CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
-- MAGIC )
-- MAGIC COMMENT "raw weekly data from parquet export of AFC and NFC teams"
-- MAGIC AS SELECT current_timestamp() processing_time, play_id, old_game_id FROM
-- MAGIC     cloud_files('${source}/pbp/', 'parquet',
-- MAGIC                  map('header', 'true', 
-- MAGIC                      'inferSchema', 'true', 
-- MAGIC                      'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### MERGE NEW RECORDS INTO SILVER 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_pbp_new2
(
    processing_time TIMESTAMP,
    play_id FLOAT NOT NULL,
    old_game_id STRING NOT NULL
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams silver"
AS 
SELECT 
    processing_time,
    play_id,
    old_game_id
FROM STREAM(live.bronze_pbp_new2) pbp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC MERGE INTO silver_pbp_new s
-- MAGIC USING (
-- MAGIC     SELECT
-- MAGIC         CONCAT(b.play_id, '_', b.old_game_id) AS dfl_key,
-- MAGIC         b.play_id,
-- MAGIC         b.passer_player_id,
-- MAGIC         b.receiver_player_id,
-- MAGIC         b.rusher_player_id,
-- MAGIC         b.qtr,
-- MAGIC         b.down,
-- MAGIC         b.goal_to_go,
-- MAGIC         b.time,
-- MAGIC         b.ydstogo,
-- MAGIC         b.ydsnet,
-- MAGIC         b.desc as play_desc,
-- MAGIC         b.play_type,
-- MAGIC         b.pass,
-- MAGIC         b.rush,
-- MAGIC         b.shotgun,
-- MAGIC         b.no_huddle,
-- MAGIC         b.qb_dropback,
-- MAGIC         b.qb_kneel,
-- MAGIC         b.qb_spike,
-- MAGIC         b.qb_scramble,
-- MAGIC         b.pass_length,
-- MAGIC         b.pass_location,
-- MAGIC         b.run_location,
-- MAGIC         b.run_gap,
-- MAGIC         b.game_id,
-- MAGIC         b.old_game_id,
-- MAGIC         b.yards_gained,
-- MAGIC         b.air_yards,
-- MAGIC         b.yards_after_catch,
-- MAGIC         b.ep,
-- MAGIC         b.epa,
-- MAGIC         b.passing_yards,
-- MAGIC         b.receiving_yards,
-- MAGIC         b.rushing_yards,
-- MAGIC         b.penalty_yards,
-- MAGIC         b.drive_play_count,
-- MAGIC         b.drive_time_of_possession,
-- MAGIC         b.drive_first_downs,
-- MAGIC         b.drive_yards_penalized
-- MAGIC     FROM live.bronze_pbp b
-- MAGIC ) b
-- MAGIC ON s.dfl_key = b.dfl_key
-- MAGIC WHEN MATCHED THEN
-- MAGIC     UPDATE SET *
-- MAGIC WHEN NOT MATCHED THEN
-- MAGIC     INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DIM GAME INFO WITHIN MATERIALIZED VIEW

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE OR REPLACE MATERIALIZED VIEW dim_play_new_vw(
-- MAGIC      dfl_key STRING COMMENT 'Unique identifier for the play'
-- MAGIC     ,play_id STRING COMMENT 'Unique play identifier for the play'
-- MAGIC     ,qtr INT COMMENT 'Quarter of the game'
-- MAGIC     ,down INT COMMENT 'Down number'
-- MAGIC     ,play_type STRING COMMENT 'the type of play (examples: run, pass, punt, field goal)'
-- MAGIC     ,touchdown BOOLEAN COMMENT 'Touchdown indicator'
-- MAGIC     )
-- MAGIC COMMENT 'Materialized view of play-by-play data looking at play dimension'
-- MAGIC
-- MAGIC AS 
-- MAGIC select
-- MAGIC dfl_key,
-- MAGIC play_id,
-- MAGIC qt,
-- MAGIC down,
-- MAGIC play_type,
-- MAGIC touchdown
-- MAGIC from
-- MAGIC FROM silver_pbp_new pbp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####FACT TABLE

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW fact_play_new_vw (
     play_id FLOAT NOT NULL COMMENT 'Unique identifier for the play',
     old_game_id STRING COMMENT 'Old identifier for the game',
     CONSTRAINT fact_play_pk PRIMARY KEY (play_id)
)
COMMENT "Update of Fact Offensive Data from Play-By-Play data"
AS
SELECT
    pbp.play_id,
    pbp.old_game_id
FROM live.silver_pbp_new2 pbp

-- COMMAND ----------


