import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_DB = config['CLUSTER']['DB_NAME']
DWH_DB_USER = config['CLUSTER']['DB_USER']
DWH_DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
DWH_PORT  = config['CLUSTER']['DB_PORT']

IAM_ROLE = config['IAM_ROLE']['ARN']

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                                                artist            VARCHAR(200)        NOT NULL,
                                                                auth              VARCHAR(50)         NOT NULL,
                                                                firstName         VARCHAR(50)         NOT NULL,
                                                                gender            VARCHAR(1)          NOT NULL,
                                                                itemInSession     INTEGER             NOT NULL,
                                                                lastName          VARCHAR(50)         NOT NULL,
                                                                length            FLOAT               NOT NULL,
                                                                level             VARCHAR(4)          NOT NULL,
                                                                location          VARCHAR(150)        NOT NULL,
                                                                method            VARCHAR(3)          NOT NULL,
                                                                page              VARCHAR(10)         NOT NULL,
                                                                registration      BIGINT              NOT NULL,
                                                                sessionId         INTEGER             NOT NULL,
                                                                song              VARCHAR(150)        NOT NULL,
                                                                status            INTEGER             NOT NULL,
                                                                ts                BIGINT              NOT NULL,
                                                                userAgent         VARCHAR(150)        NOT NULL,
                                                                userId            INTEGER             NOT NULL
                                                                )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                                                num_songs         INTEGER             NOT NULL,
                                                                artist_id         VARCHAR(30)         NOT NULL,
                                                                artist_latitude   VARCHAR(30)         NOT NULL,
                                                                artist_longitude  VARCHAR(30)         NOT NULL,
                                                                artist_location   VARCHAR(30)         NOT NULL,
                                                                artist_name       VARCHAR(150)        NOT NULL,
                                                                song_id           VARCHAR(30)         NOT NULL,
                                                                title             VARCHAR(150)        NOT NULL,
                                                                duration          FLOAT               NOT NULL,
                                                                year              INTEGER             NOT NULL
                                                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                                                                songplay_id       INT  IDENTITY(0,1)  PRIMARY KEY,
                                                                start_time        BIGINT              NOT NULL sortkey,
                                                                user_id           INTEGER             NOT NULL distkey,
                                                                level             VARCHAR(4)          NOT NULL,
                                                                song_id           VARCHAR(30)         NOT NULL,
                                                                artist_id         VARCHAR(30)         NOT NULL,
                                                                session_id        INTEGER             NOT NULL,
                                                                location          VARCHAR(150)        NOT NULL,
                                                                user_agent        VARCHAR(150)        NOT NULL
                                                                )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                                                                user_id           INTEGER             PRIMARY KEY sortkey distkey,
                                                                first_name        VARCHAR(50)         NOT NULL,
                                                                last_name         VARCHAR(50)         NOT NULL,
                                                                gender            VARCHAR(1),
                                                                level             VARCHAR(4)
                                                                )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (
                                                                song_id           VARCHAR(30)         PRIMARY KEY sortkey,
                                                                title             VARCHAR(150)        NOT NULL,
                                                                artist_id         VARCHAR(30)         NOT NULL,
                                                                year              INTEGER,
                                                                duration          FLOAT               NOT NULL
                                                                )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
                                                                artist_id         VARCHAR(30)         PRIMARY KEY sortkey,
                                                                name              VARCHAR(150)        NOT NULL,
                                                                location          VARCHAR(30),
                                                                latitude          FLOAT,
                                                                longitude         FLOAT
                                                                )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                                                start_time        BIGINT              NOT NULL sortkey,
                                                                hour              TIME                        ,
                                                                day               INTEGER                     ,
                                                                week              INTEGER                     ,
                                                                month             INTEGER                     ,
                                                                year              INTEGER                     ,
                                                                weekday           VARCHAR(20)
                                                                )
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          format as json {};
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_events FROM 's3://udacity-dend/song_data'
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          format as json {};
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (
                                                  songplay_id,
                                                  start_time,
                                                  user_id,
                                                  level,
                                                  song_id,
                                                  artist_id,
                                                  session_id,
                                                  location,
                                                  user_agent
                                                  )
                                        VALUES   (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""INSERT INTO users(
                                 user_id
                                ,first_name
                                ,last_name
                                ,gender
                                ,level)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (user_id) DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""INSERT INTO songs(
                                 song_id
                                ,title
                                ,artist_id
                                ,year
                                ,duration)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists(
                                artist_id
                                ,name
                                ,location
                                ,latitude
                                ,longitude)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""INSERT INTO time(
                                 start_time
                                ,hour
                                ,day
                                ,week
                                ,month
                                ,year
                                ,weekday)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
