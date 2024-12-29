-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Load Bronze data into dfl.nfl workspace
-- MAGIC ##### SOURCE:
-- MAGIC ##### abfss://root@sadfldatabricks.dfs.core.windows.net/raw/nfl/

-- COMMAND ----------

CREATE STREAMING TABLE bronze_pbp(
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
-- MAGIC ####SILVER TABLE

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_pbp
(
dfl_key STRING NOT NULL COMMENT 'composite key of play_id and old_game_id'
,play_id FLOAT NOT NULL COMMENT 'The unique id of the play for each game'
,game_id STRING NOT NULL COMMENT 'The unique id of the play for each game'
,old_game_id STRING NOT NULL COMMENT 'The unique id of the game that denotes year, the week, and the teams involved'
,season_type STRING COMMENT 'type of game (REG-regular vs PLY-Playoff)'
,week INT COMMENT 'week number (1 through 17 plus playoffs)'
,game_half STRING COMMENT 'description of what half the game is on, Half1 vs Half2'
,quarter_end FLOAT COMMENT 'boolean flag denoting whether the play is the end of the quarter'
,drive FLOAT COMMENT 'number of drive in the game'
,sp FLOAT COMMENT 'boolean flag denoting special teams play'
,qtr FLOAT COMMENT 'the quarter of the game'
,down FLOAT COMMENT  'the down of the play (1,2,3,4) '
,goal_to_go FLOAT COMMENT 'boolean identifier if the play is goal to go or not'
,time STRING COMMENT 'the time showing on the game clock (ex 14:44)'
,yrdln STRING COMMENT 'text label of the yard line (example DEN 35)'
,ydstogo FLOAT COMMENT 'The number of yards to go for the play to get a first down, ranging from 0 to 99 yards'
,ydsnet FLOAT COMMENT 'The yards remaining to the end zone (ex - If on the oppponents 25 yard line, they have 75 ydsnet)'
,desc STRING COMMENT 'text description of play'
,play_type STRING COMMENT 'the type of play (examples: run, pass, punt, field goal)'
,yards_gained FLOAT COMMENT 'number of yards gained on a play, from negative to 99 yards'
,shotgun FLOAT COMMENT 'boolean identifier if play was in shotgun formation or not'
,no_huddle FLOAT COMMENT 'boolean identifier if play was no huddle or not'
,qb_dropback FLOAT COMMENT 'boolean identifier if quarterback dropped back on play or not'
,qb_kneel FLOAT COMMENT 'boolean identifier if quarterback kneeled down or not'
,qb_spike FLOAT COMMENT 'boolean identifier if quarterback spiked the ball to stop the clock or not'
,qb_scramble FLOAT COMMENT 'boolean identifier if quarterback spiked the ball to stop the clock or not'
,pass_length STRING COMMENT 'description of type of pass: short, deep or not a pass'
,pass_location STRING COMMENT 'location of pass: left, right, middle or null'
,air_yards FLOAT COMMENT 'number of yards in the air for a pass'
,yards_after_catch FLOAT COMMENT  'number of yards the ball was carried by reciever after catching the ball'
,run_location STRING COMMENT 'location of run: left, right, middle or null'
,run_gap STRING COMMENT 'location of run gap on rushing play: guard, tackle, center or null'
,field_goal_result STRING COMMENT 'description of field goal attempt (made, missed or null)'
,kick_distance FLOAT COMMENT 'distance of field goal attempt'
,extra_point_result STRING COMMENT 'result of extra point attempt (good, missed or null)'
,two_point_conv_result STRING COMMENT 'result of two point conversion attempt'
,home_timeouts_remaining FLOAT COMMENT 'number of timeouts left for home team'
,away_timeouts_remaining FLOAT COMMENT 'number of timeouts left for away team'
,timeout FLOAT COMMENT 'boolean identifier for whether play was timeout'
,timeout_team STRING COMMENT 'team name of who called timeout'
,ep FLOAT COMMENT 'Expected Points (EP) rating - the value of the current down, distance, and field position situation in terms of future expected net point advantage'
,epa FLOAT COMMENT 'Expected Points Added (EPA) is a commonly used advanced statistic in football. In short, this stat measures how well a team performs compared to their expectation on a play-by-play basis'
,sack FLOAT COMMENT  ''
,passer_player_id STRING COMMENT 'the player_id for the passer on the play'
,passing_yards FLOAT COMMENT  'passing yards for the play'
,receiver_player_id STRING COMMENT  'the player_id for the receiver on the play'
,receiving_yards FLOAT COMMENT 'recieving yards for the play'
,rusher_player_id STRING COMMENT  'the player_id for the rusher on the play'
,rushing_yards FLOAT COMMENT  'rushing yards for the play'
,penalty_yards FLOAT COMMENT   'penalty yards for the play'
,season BIGINT COMMENT  'year of the NFL season'
,cp FLOAT COMMENT  'Completion Probabilty '
,cpoe FLOAT COMMENT  'Completion Percentage Over Expected (CPOE) is an advanced NFL metric that estimates the likelihood of a pass being completed based on the locations, speeds, and directions of relevant players'
,series FLOAT COMMENT  'number of series in the game'
,series_success FLOAT COMMENT  'boolean indicator of whether series was considered a success (first down or touchdown)'
,series_result STRING COMMENT  'description of series result'
,start_time STRING COMMENT 'game start time'
,stadium STRING COMMENT 'name of stadium'
,weather STRING COMMENT 'description of weather'
,drive_play_count FLOAT COMMENT  'number of plays on the drive'
,drive_time_of_possession STRING COMMENT 'time of possession for the drive'
,drive_first_downs FLOAT COMMENT 'number of first downs on the drive'
,drive_yards_penalized FLOAT COMMENT 'yards penalized during the drive'
,roof STRING COMMENT 'roof type '
,surface STRING COMMENT 'surface type'
,temp FLOAT COMMENT 'field temperature'
,wind FLOAT COMMENT 'field wind speed'
,home_coach STRING COMMENT 'name of home team coach'
,away_coach STRING COMMENT 'name of away team coach'
,stadium_id STRING COMMENT 'id of stadium'
,game_stadium STRING COMMENT 'name of game stadium'
,pass FLOAT COMMENT 'boolean identifier if play was a pass or not'
,rush FLOAT COMMENT  'boolean identifier if play was a rush or not'
,CONSTRAINT fact_silver_pbp PRIMARY KEY (dfl_key)
)
COMMENT "Incremental refresh of intermediate silver table where composite key is derived and other transformations are done. Also limiting data to offensive data only"
AS
SELECT
CONCAT(pbp.play_id, '_', pbp.old_game_id) AS dfl_key
,pbp.play_id
,pbp.game_id
,pbp.old_game_id
,pbp.season_type
,pbp.week
,pbp.game_half
,pbp.quarter_end
,pbp.drive
,pbp.sp
,pbp.qtr
,pbp.down
,pbp.goal_to_go
,pbp.time
,pbp.yrdln
,pbp.ydstogo
,pbp.ydsnet
,pbp.desc
,pbp.play_type
,pbp.yards_gained
,pbp.shotgun
,pbp.no_huddle
,pbp.qb_dropback
,pbp.qb_kneel
,pbp.qb_spike
,pbp.qb_scramble
,pbp.pass_length
,pbp.pass_location
,pbp.air_yards
,pbp.yards_after_catch
,pbp.run_location
,pbp.run_gap
,pbp.field_goal_result
,pbp.kick_distance
,pbp.extra_point_result
,pbp.two_point_conv_result
,pbp.home_timeouts_remaining
,pbp.away_timeouts_remaining
,pbp.timeout
,pbp.timeout_team
,pbp.ep
,pbp.epa
,pbp.sack
,pbp.passer_player_id
,pbp.passing_yards
,pbp.receiver_player_id
,pbp.receiving_yards
,pbp.rusher_player_id
,pbp.rushing_yards
,pbp.penalty_yards
,pbp.season
,pbp.cp
,pbp.cpoe
,pbp.series
,pbp.series_success
,pbp.series_result
,pbp.start_time
,pbp.stadium
,pbp.weather
,pbp.drive_play_count
,pbp.drive_time_of_possession
,pbp.drive_first_downs
,pbp.drive_yards_penalized
,pbp.roof
,pbp.surface
,pbp.temp
,pbp.wind
,pbp.home_coach
,pbp.away_coach
,pbp.stadium_id
,pbp.game_stadium
,pbp.pass
,pbp.rush

FROM STREAM(live.bronze_pbp) pbp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####GAME DIMENSION MATERIALIZED VIEW

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW dim_game_vw (
dfl_key STRING NOT NULL COMMENT 'composite key of play_id and old_game_id'
,old_game_id STRING NOT NULL COMMENT 'The unique id of the game that denotes year, the week, and the teams involved'
,season_type STRING COMMENT  'type of game (REG-regular vs PLY-Playoff)'
,week INT COMMENT 'week number (1 through 17 plus playoffs)'
,season BIGINT COMMENT  'year of the NFL season'
,start_time STRING COMMENT 'game start time'
,stadium STRING COMMENT 'name of stadium'
,weather STRING COMMENT 'description of weather'
,roof STRING COMMENT 'roof type '
,surface STRING COMMENT 'surface type'
,temp FLOAT COMMENT 'field temperature'
,wind FLOAT COMMENT 'field wind speed'
,home_coach STRING COMMENT 'name of home team coach'
,away_coach STRING COMMENT 'name of away team coach'
,stadium_id STRING COMMENT 'id of stadium'
,game_stadium STRING COMMENT 'name of game stadium'
,CONSTRAINT dim_game_pk PRIMARY KEY (old_game_id)
)
COMMENT 'Dimension table for game data'
AS
SELECT
     pbp.dfl_key
    ,pbp.old_game_id
    ,pbp.season_type
    ,pbp.week
    ,pbp.season
    ,pbp.start_time
    ,pbp.stadium
    ,pbp.weather
    ,pbp.roof
    ,pbp.surface
    ,pbp.temp
    ,pbp.wind
    ,pbp.home_coach
    ,pbp.away_coach
    ,pbp.stadium_id
    ,pbp.game_stadium
FROM live.silver_pbp pbp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####PLAY DIMENSION MATERIALZED VIEW

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW dim_play_vw (
 dfl_key STRING  NOT NULL COMMENT 'composite key of play_id and old_game_id'
,play_id FLOAT  NOT NULL COMMENT  'The unique id of the play for each game'
,game_id STRING  NOT NULL COMMENT 'The unique id of the play for each game'
,game_half STRING COMMENT  'description of what half the game is on, Half1 vs Half2'
,quarter_end FLOAT COMMENT  'boolean flag denoting whether the play is the end of the quarter'
,drive FLOAT COMMENT 'number of drive in the game'
,sp FLOAT COMMENT 'boolean flag denoting special teams play'
,qtr FLOAT COMMENT 'the quarter of the game'
,down FLOAT COMMENT 'the down of the play (1,2,3,4) '
,goal_to_go FLOAT COMMENT  'boolean identifier if the play is goal to go or not'
,time STRING COMMENT 'the time showing on the game clock (ex 14:44)'
,yrdln STRING COMMENT 'text label of the yard line (example DEN 35)'
,ydstogo FLOAT COMMENT 'The number of yards to go for the play to get a first down, ranging from 0 to 99 yards'
,ydsnet FLOAT COMMENT  'The yards remaining to the end zone (ex - If on the oppponents 25 yard line, they have 75 ydsnet)'
,desc STRING COMMENT 'text description of play'
,play_type STRING COMMENT 'the type of play (examples: run, pass, punt, field goal)'
,shotgun FLOAT COMMENT 'boolean identifier if play was in shotgun formation or not'
,no_huddle FLOAT COMMENT 'boolean identifier if play was no huddle or not'
,qb_dropback FLOAT COMMENT 'boolean identifier if quarterback dropped back on play or not'
,qb_kneel FLOAT COMMENT 'boolean identifier if quarterback kneeled down or not'
,qb_spike FLOAT COMMENT 'boolean identifier if quarterback spiked the ball to stop the clock or not'
,qb_scramble FLOAT COMMENT 'boolean identifier if quarterback spiked the ball to stop the clock or not'
,pass_length STRING COMMENT 'description of type of pass: short, deep or not a pass'
,pass_location STRING COMMENT 'location of pass: left, right, middle or null'
,run_location STRING COMMENT 'location of run: left, right, middle or null'
,run_gap STRING COMMENT 'location of run gap on rushing play: guard, tackle, center or null'
,field_goal_result STRING COMMENT 'description of field goal attempt (made, missed or null)'
,kick_distance FLOAT COMMENT  'distance of field goal attempt'
,extra_point_result STRING COMMENT 'result of extra point attempt (good, missed or null)'
,two_point_conv_result STRING COMMENT 'result of two point conversion attempt'
,home_timeouts_remaining FLOAT COMMENT  'number of timeouts left for home team'
,away_timeouts_remaining FLOAT COMMENT 'number of timeouts left for away team'
,timeout FLOAT COMMENT 'boolean identifier for whether play was timeout'
,timeout_team STRING COMMENT  'team name of who called timeout'
,sack FLOAT COMMENT  ''
,passer_player_id STRING COMMENT 'the player_id for the passer on the play'
,receiver_player_id STRING COMMENT 'the player_id for the receiver on the play'
,rusher_player_id STRING COMMENT  'the player_id for the rusher on the play'
,series FLOAT COMMENT 'number of series in the game'
,series_success FLOAT COMMENT  'boolean indicator of whether series was considered a success (first down or touchdown)'
,series_result STRING COMMENT  'description of series result'
,pass FLOAT COMMENT  'boolean identifier if play was a pass or not'
,rush FLOAT COMMENT  'boolean identifier if play was a rush or not'
,CONSTRAINT dim_play_pk PRIMARY KEY (play_id)
)
COMMENT "Dimension table for play data"
AS
 SELECT
pbp.dfl_key
,pbp.play_id
,pbp.game_id
,pbp.game_half
,pbp.quarter_end
,pbp.drive
,pbp.sp
,pbp.qtr
,pbp.down
,pbp.goal_to_go
,pbp.time
,pbp.yrdln
,pbp.ydstogo
,pbp.ydsnet
,pbp.desc
,pbp.play_type
,pbp.shotgun
,pbp.no_huddle
,pbp.qb_dropback
,pbp.qb_kneel
,pbp.qb_spike
,pbp.qb_scramble
,pbp.pass_length
,pbp.pass_location
,pbp.run_location
,pbp.run_gap
,pbp.field_goal_result
,pbp.kick_distance
,pbp.extra_point_result
,pbp.two_point_conv_result
,pbp.home_timeouts_remaining
,pbp.away_timeouts_remaining
,pbp.timeout
,pbp.timeout_team
,pbp.sack
,pbp.passer_player_id
,pbp.receiver_player_id
,pbp.rusher_player_id
,pbp.series
,pbp.series_success
,pbp.series_result
,pbp.pass
,pbp.rush
FROM live.silver_pbp pbp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE FACT FOR PLAYS MATERIALIZED VIEW

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW fact_play_vw (
dfl_key STRING NOT NULL  COMMENT  'composite key of play_id and old_game_id'
,play_id FLOAT COMMENT  'The unique id of the play for each game'
,old_game_id STRING COMMENT  'The unique id of the game that denotes year, the week, and the teams involved'
,yards_gained FLOAT COMMENT  'number of yards gained on a play, from negative to 99 yards'
,air_yards FLOAT COMMENT  'number of yards in the air for a pass'
,yards_after_catch FLOAT COMMENT  'number of yards the ball was carried by reciever after catching the ball'
,ep FLOAT COMMENT  'Expected Points (EP) rating - the value of the current down, distance, and field position situation in terms of future expected net point advantage'
,epa FLOAT COMMENT  'Expected Points Added (EPA) is a commonly used advanced statistic in football. In short, this stat measures how well a team performs compared to their expectation on a play-by-play basis'
,passing_yards FLOAT COMMENT  'passing yards for the play'
,receiving_yards FLOAT COMMENT  'recieving yards for the play'
,rushing_yards FLOAT COMMENT  'rushing yards for the play'
,penalty_yards FLOAT COMMENT  'penalty yards for the play'
,cp FLOAT COMMENT 'Completion Probabilty '
,cpoe FLOAT COMMENT 'Completion Percentage Over Expected (CPOE) is an advanced NFL metric that estimates the likelihood of a pass being completed based on the locations, speeds, and directions of relevant players'
,drive_play_count FLOAT COMMENT 'number of plays on the drive'
,drive_time_of_possession STRING COMMENT 'time of possession for the drive'
,drive_first_downs FLOAT COMMENT 'number of first downs on the drive'
,drive_yards_penalized FLOAT COMMENT 'yards penalized during the drive'
,CONSTRAINT fact_play_vw_pk PRIMARY KEY (dfl_key)

)
COMMENT "Update of Fact Offensive Data from Play-By-Play data"
AS
SELECT
 pbp.dfl_key
,pbp.play_id
,pbp.old_game_id
,pbp.yards_gained
,pbp.air_yards
,pbp.yards_after_catch
,pbp.ep
,pbp.epa
,pbp.passing_yards
,pbp.receiving_yards
,pbp.rushing_yards
,pbp.penalty_yards
,pbp.cp
,pbp.cpoe
,pbp.drive_play_count
,pbp.drive_time_of_possession
,pbp.drive_first_downs
,pbp.drive_yards_penalized
FROM live.silver_pbp pbp
