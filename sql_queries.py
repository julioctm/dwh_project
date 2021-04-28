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
                                                                event_id          BIGINT  IDENTITY(0,1) NOT NULL,
                                                                artist            VARCHAR(MAX),
                                                                auth              VARCHAR(MAX),
                                                                firstName         VARCHAR(MAX),
                                                                gender            VARCHAR(MAX),
                                                                itemInSession     INTEGER,
                                                                lastName          VARCHAR(MAX),
                                                                length            FLOAT,
                                                                level             VARCHAR(MAX),
                                                                location          VARCHAR(MAX),
                                                                method            VARCHAR(MAX),
                                                                page              VARCHAR(MAX),
                                                                registration      BIGINT,
                                                                sessionId         INTEGER SORTKEY DISTKEY,
                                                                song              VARCHAR(MAX),
                                                                status            INTEGER,
                                                                ts                BIGINT,
                                                                userAgent         VARCHAR(MAX),
                                                                userId            INTEGER
                                                                )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                                                num_songs         INTEGER,
                                                                artist_id         VARCHAR(MAX),
                                                                artist_latitude   DECIMAL,
                                                                artist_longitude  DECIMAL,
                                                                artist_location   VARCHAR(MAX),
                                                                artist_name       VARCHAR(MAX),
                                                                song_id           VARCHAR(MAX),
                                                                title             VARCHAR(MAX),
                                                                duration          FLOAT,
                                                                year              INTEGER
                                                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                                                                songplay_id       INT  IDENTITY(0,1)  PRIMARY KEY,
                                                                start_time        TIMESTAMP     sortkey NOT NULL,
                                                                user_id           INTEGER       distkey NOT NULL,
                                                                level             VARCHAR(MAX)          NOT NULL,
                                                                song_id           VARCHAR(MAX)          NOT NULL,
                                                                artist_id         VARCHAR(MAX)          NOT NULL,
                                                                session_id        INTEGER               NOT NULL,
                                                                location          VARCHAR(MAX),
                                                                user_agent        VARCHAR(MAX)
                                                                )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                                                                user_id           INTEGER sortkey distkey,
                                                                first_name        VARCHAR(MAX),
                                                                last_name         VARCHAR(MAX),
                                                                gender            VARCHAR(MAX),
                                                                level             VARCHAR(MAX)  NOT NULL
                                                                )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (
                                                                song_id           VARCHAR(MAX) sortkey NOT NULL,
                                                                title             VARCHAR(MAX)         NOT NULL,
                                                                artist_id         VARCHAR(MAX)         NOT NULL,
                                                                year              INTEGER,
                                                                duration          FLOAT NOT NULL
                                                                )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
                                                                artist_id         VARCHAR(MAX) sortkey NOT NULL,
                                                                name              VARCHAR(MAX)         NOT NULL,
                                                                location          VARCHAR(MAX),
                                                                latitude          FLOAT,
                                                                longitude         FLOAT
                                                                )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                                                start_time        TIMESTAMP sortkey NOT NULL,
                                                                hour              INTEGER           NOT NULL,
                                                                day               INTEGER           NOT NULL,
                                                                week              INTEGER           NOT NULL,
                                                                month             INTEGER           NOT NULL,
                                                                year              INTEGER           NOT NULL,
                                                                weekday           INTEGER           NOT NULL
                                                                )
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          format as json {}
                          EMPTYASNULL
                          BLANKSASNULL;
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM {}
                          credentials 'aws_iam_role={}'
                          json 'auto'
                          region 'us-west-2'
                          EMPTYASNULL
                          BLANKSASNULL;
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (
                                                  start_time,
                                                  user_id,
                                                  level,
                                                  song_id,
                                                  artist_id,
                                                  session_id,
                                                  location,
                                                  user_agent
                                                  )
                                        SELECT
                                            (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time,
                                            se.userId,
                                            se.level,
                                            ss.song_id,
                                            ss.artist_id,
                                            se.sessionId,
                                            se.location,
                                            se.userAgent
                                        FROM staging_events se
                                            INNER JOIN staging_songs ss
                                                ON se.song = ss.title
                                        WHERE se.page = 'NextSong';
                                            
""")

user_table_insert = ("""INSERT INTO users(
                                 user_id
                                ,first_name
                                ,last_name
                                ,gender
                                ,level)
                            SELECT
                                se.userId,
                                se.firstName,
                                se.lastName,
                                se.gender,
                                se.level
                            FROM staging_events se;
""")

song_table_insert = ("""INSERT INTO song(
                                 song_id
                                ,title
                                ,artist_id
                                ,year
                                ,duration)
                            SELECT
                                ss.song_id,
                                ss.title,
                                ss.artist_id,
                                ss.year,
                                ss.duration
                            FROM staging_songs ss;
""")

artist_table_insert = ("""INSERT INTO artist(
                                artist_id
                                ,name
                                ,location
                                ,latitude
                                ,longitude)
                            SELECT
                                ss.artist_id,
                                ss.artist_name,
                                ss.artist_location,
                                ss.artist_latitude,
                                ss.artist_longitude
                            FROM staging_songs ss;
""")

time_table_insert = ("""INSERT INTO time(
                                 start_time
                                ,hour
                                ,day
                                ,week
                                ,month
                                ,year
                                ,weekday)
                            SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time,
                                    extract(HOUR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as hour,
                                    extract(DAY FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as day,
                                    extract(WEEK FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as week,
                                    extract(MONTH FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as month,
                                    extract(YEAR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as year,
                                    extract(DAY FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as weekday
                            FROM staging_events se;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
