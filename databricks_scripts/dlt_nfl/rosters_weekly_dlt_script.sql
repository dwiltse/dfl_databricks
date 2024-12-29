-- Databricks notebook source
CREATE STREAMING LIVE TABLE bronze_weekly_roster_nfl(
    CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw weekly data from parquet export of AFC and NFC teams"
AS SELECT current_timestamp() processing_time, * FROM
    cloud_files('${source}/weekly_rosters/', 'json',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE silver_weekly_roster_nfl(
 season BIGINT
,week BIGINT
,game_type STRING
,team STRING
,position STRING
,jersey_number DOUBLE
,player_status STRING
,player_name STRING
,birth_date STRING
,height DOUBLE
,weight DOUBLE
,college STRING
,player_id STRING NOT NULL
,headshot_url STRING
,rookie_year DOUBLE
,draft_club STRING
,draft_number DOUBLE
,age DOUBLE
CONSTRAINT user_valid_player_id EXPECT (player_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Incremental Update of Weekly Roster Data"
AS
select
r.season,
r.week,
r.game_type,
r.team,
r.position,
r.jersey_number,
r.status as player_status,
r.player_name,
 from_unixtime(r.birth_date / 1000) AS birth_date,
r.height,
r.weight,
r.college,
r.player_id,
r.headshot_url,
r.rookie_year,
r.draft_club,
r.draft_number,
r.age
FROM STREAM(live.bronze_weekly_roster_nfl) r






-- COMMAND ----------

CREATE MATERIALIZED VIEW IF NOT EXISTS dim_player_vw (
    season LONG COMMENT 'Season year'
    ,week LONG COMMENT 'Week of the season'
    ,game_type STRING COMMENT 'Type of the game (e.g., regular, playoff)'
    ,team STRING COMMENT 'Team name'
    ,position STRING COMMENT 'Player position'
    ,jersey_number DOUBLE COMMENT 'Player jersey number'
    ,player_status STRING COMMENT 'Player status (e.g., active, inactive)'
    ,player_name STRING COMMENT 'Player name'
    ,birth_date STRING COMMENT 'Player birth date'
    ,height DOUBLE COMMENT 'Player height'
    ,weight DOUBLE COMMENT 'Player weight'
    ,college STRING COMMENT 'College attended by the player'
    ,player_id STRING NOT NULL COMMENT 'Unique identifier for the player'
    ,headshot_url STRING COMMENT 'URL to the player headshot image'
    ,rookie_year DOUBLE COMMENT 'Rookie year of the player'
    ,draft_club STRING COMMENT 'Club that drafted the player'
    ,draft_number DOUBLE COMMENT 'Draft number of the player'
    ,age DOUBLE COMMENT 'Player age'
   ,CONSTRAINT dim_player_pk PRIMARY KEY (player_id)
)
COMMENT 'Materialized view of the silver_weekly_roster table'
AS
SELECT
    season,
    week,
    game_type,
    team,
    position,
    jersey_number,
    player_status,
    player_name,
    from_unixtime(birth_date / 1000) AS birth_date,
    height,
    weight,
    college,
    player_id,
    headshot_url,
    rookie_year,
    draft_club,
    draft_number,
    age
FROM live.silver_weekly_roster_nfl;
