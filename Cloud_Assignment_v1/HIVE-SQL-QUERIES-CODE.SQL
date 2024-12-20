-- CREATE STAGE DATABASE IN HIVE

CREATE DATABASE DB_OPEN_STATS_STAGE;

-- T_COMPETITIONS Hive Data Ingestion

docker cp pySparkCode/competitions-etl-job.py spark-master:/tmp/competitions-etl-job.py

docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:8088 /tmp/competitions-etl-job.py

CREATE DATABASE DB_OPEN_STATS_STAGE;

CREATE TABLE IF NOT EXISTS DB_OPEN_STATS_STAGE.T_COMPETITIONS (
    competition_id INT,
    season_id INT,
    country_name STRING,
    competition_name STRING,
    competition_gender STRING,
    competition_youth STRING,
    competition_international STRING,
    season_name STRING,
    match_updated TIMESTAMP,
    match_updated_360 TIMESTAMP,
    match_available_360 TIMESTAMP,
    match_available TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/standard-csv-data/competitions/' INTO TABLE DB_OPEN_STATS_STAGE.T_COMPETITIONS;

--

-- T_EVENTS Hive Data Ingestion

docker cp pySparkCode/events-etl-job.py spark-master:/tmp/events-etl-job.py

docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:8088 /tmp/events-etl-job.py

docker cp /Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/events/ namenode:/standard-data/events/
-- hdfs dfs -put ./standard-data/events/*.csv /standard-data/events/

docker exec -it namenode bash  

docker exec -it hive-server bash

hive

CREATE TABLE IF NOT EXISTS DB_OPEN_STATS_STAGE.T_EVENTS (
    event_id STRING,
    index INT,
    period STRING,
    timestamp_time TIMESTAMP,
    minute INT,
    second INT,
    type_id STRING,
    type_name STRING,
    team_id INT,
    team_name STRING,
    formation STRING,
    player_id INT,
    player_name STRING,
    position_id INT,
    position_name STRING,
    jersey_number INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/standard-data/events/' INTO TABLE DB_OPEN_STATS_STAGE.T_EVENTS;


-- T_LINEUPS Hive Data Ingestion

docker cp pySparkCode/lineups-etl-job.py spark-master:/tmp/lineups-etl-job.py

docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:8088 /tmp/lineups-etl-job.py

docker cp /Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/lineups/ namenode:/standard-data/lineups/
docker exec -it namenode bash  

-- hdfs dfs -put ./standard-data/lineups/*.csv /standard-data/lineups/

docker exec -it hive-server bash

hive

CREATE TABLE IF NOT EXISTS DB_OPEN_STATS_STAGE.T_LINEUPS (
    team_id INT,
    team_name STRING,
    player_id INT,
    player_name STRING,
    player_nickname STRING,
    jersey_number INT,
    country_id INT,
    country_name STRING,
    position_id INT,
    position_name STRING,
    position_start_time STRING,
    position_end_time STRING,
    start_period INT,
    end_period INT,
    start_reason STRING,
    end_reason STRING,
    card_time STRING,
    card_type STRING,
    reason STRING,
    card_period INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/standard-data/lineups/' INTO TABLE DB_OPEN_STATS_STAGE.T_LINEUPS;


-- T_MATCHES Hive Data Ingestion

docker cp pySparkCode/matches-etl-job.py spark-master:/tmp/matches-etl-job.py

docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:8088 /tmp/matches-etl-job.py

docker cp /Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/matches/ namenode:/standard-data/matches/
docker exec -it namenode bash  

hdfs dfs -mkdir -p /standard-data/matches

-- hdfs dfs -put ./standard-data/matches/*.csv /standard-data/matches/

docker exec -it hive-server bash

hive

CREATE TABLE IF NOT EXISTS DB_OPEN_STATS_STAGE.T_MATCHES (
    match_id INT,
    match_date DATE,
    kick_off STRING,
    match_week INT,
    home_score INT,
    away_score INT,
    competition_id INT,
    competition_name STRING,
    country_name STRING,
    season_id INT,
    season_name STRING,
    team_id INT,
    team_name STRING,
    team_gender STRING,
    team_country_id INT,
    team_country_name STRING,
    manager_id INT,
    manager_name STRING,
    manager_nickname STRING,
    manager_dob DATE,
    manager_country_id INT,
    manager_country_name STRING,
    home_or_away STRING,
    result STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/standard-data/matches/' INTO TABLE DB_OPEN_STATS_STAGE.T_MATCHES;


-- T_THREE_SIXTY Hive Data Ingestion

docker cp pySparkCode/three-sixty-etl-job.py spark-master:/tmp/three-sixty-etl-job.py

docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:8088 /tmp/three-sixty-etl-job.py

docker cp /Users/aakashgangurde/Desktop/CSC1142_Cloud_Technologies/Assignment/standard-data/three-sixty/ namenode:/standard-data/three-sixty/
docker exec -it namenode bash  

hdfs dfs -mkdir -p /standard-data/three-sixty

-- hdfs dfs -put ./standard-data/three-sixty/*.csv /standard-data/three-sixty/

docker exec -it hive-server bash

hive

CREATE TABLE IF NOT EXISTS DB_OPEN_STATS_STAGE.T_THREE_SIXTY (
    event_uuid STRING,
    visible_area_str STRING,
    teammate STRING,
    actor STRING,
    keeper STRING,
    location_x STRING,
    location_y STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/standard-data/three-sixty/' INTO TABLE DB_OPEN_STATS_STAGE.T_THREE_SIXTY;


--  CREATING ANALYTICAL DATASET IN HIVE
CREATE DATABASE DB_OPEN_STATS_ANALYTICS;


-- CREATING DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_15_16 TABLE FOR VISUALISATION

CREATE TABLE IF NOT EXISTS DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_15_16 (
    team_name STRING,
    percentage FLOAT,
    result STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_15_16
SELECT
    team_name,
    percentage,
    result
FROM (
    WITH team_results AS (
        SELECT
            team_name,
            result,
            COUNT(*) AS result_count
        FROM DB_OPEN_STATS_STAGE.T_MATCHES
        WHERE
            season_name = '2015/2016'
            AND competition_name = 'Premier League'
        GROUP BY team_name, result
    ),
    result_totals AS (
        SELECT
            team_name,
            SUM(result_count) AS total_matches
        FROM team_results
        GROUP BY team_name
    ),
    result_percentages AS (
        SELECT
            tr.team_name,
            tr.result,
            tr.result_count,
            rt.total_matches,
            (tr.result_count * 100.0 / rt.total_matches) AS percentage
        FROM team_results tr
        JOIN result_totals rt
        ON tr.team_name = rt.team_name
    )
    SELECT
        team_name,
        percentage,
        result
    FROM result_percentages
) final_results;


INSERT OVERWRITE DIRECTORY '/tmp/premier_league_15_16_results'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_15_16;

-- hdfs dfs -get /tmp/premier_league_15_16_results ./premier_league_results.csv

-- docker cp docker-hive-namenode-1:/premier_league_results.csv /Users/aakashgangurde/Desktop/premier_league_results.csv


-- CREATING DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_TOP_4_POINTS FOR LINE GRAPH VISUALISATION


CREATE TABLE IF NOT EXISTS DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_TOP_4_POINTS (
    team_id INT,
    team_name STRING,
    match_week INT,
    cumulative_points INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_TOP_4_POINTS
SELECT
    c.team_id,
    c.team_name,
    c.match_week,
    c.cumulative_points
FROM
    (
        SELECT
            team_id,
            team_name,
            match_week,
            SUM(points) OVER (PARTITION BY team_id ORDER BY match_week) AS cumulative_points
        FROM
            (
                SELECT
                    match_week,
                    team_id,
                    team_name,
                    CASE
                        WHEN result = 'win' THEN 3
                        WHEN result = 'draw' THEN 1
                        ELSE 0
                    END AS points
                FROM
                    DB_OPEN_STATS_STAGE.T_MATCHES
                WHERE
                    competition_name = 'Premier League'
                    AND season_name = '2015/2016'
            ) AS match_points
    ) c
JOIN
    (
        SELECT
            team_id,
            team_name,
            SUM(points) AS total_points
        FROM
            (
                SELECT
                    team_id,
                    team_name,
                    CASE
                        WHEN result = 'win' THEN 3
                        WHEN result = 'draw' THEN 1
                        ELSE 0
                    END AS points
                FROM
                    DB_OPEN_STATS_STAGE.T_MATCHES
                WHERE
                    competition_name = 'Premier League'
                    AND season_name = '2015/2016'
            ) AS match_points
        WHERE
            team_name != 'Manchester United'
        GROUP BY
            team_id, team_name
        ORDER BY
            total_points DESC
        LIMIT 4
    ) t
ON c.team_id = t.team_id
ORDER BY
    c.team_name, c.match_week;

INSERT OVERWRITE DIRECTORY '/tmp/premier_league_top_4_points'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM DB_OPEN_STATS_ANALYTICS.T_PREMIER_LEAGUE_TOP_4_POINTS;

docker exec -it namenode bash  

docker exec -it hive-server bash


-- hdfs dfs -get /tmp/premier_league_top_4_points ./premier_league_top_4_points

-- docker cp namenode:/premier_league_top_4_points /Users/aakashgangurde/Desktop/premier_league_top_4_points/