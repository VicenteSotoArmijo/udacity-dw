import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create = (""" CREATE TABLE staging_events (
                            artist VARCHAR, 
                            auth VARCHAR,
                            firstName VARCHAR,
                            gender CHAR(1),
                            itemInSession INT,
                            lastName VARCHAR,
                            length DECIMAL,
                            level VARCHAR,
                            location VARCHAR,
                            method VARCHAR,
                            page VARCHAR,
                            registration DECIMAL,
                            sessionId INT,
                            song VARCHAR, 
                            status INT,
                            ts BIGINT,
                            userAgent VARCHAR, 
                            userId INT)
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
                            num_songs INT,
                            artist_id VARCHAR,
                            artist_latitude DECIMAL,
                            artist_longitude DECIMAL,
                            artist_location VARCHAR,
                            artist_name VARCHAR,
                            song_id VARCHAR,
                            title VARCHAR,
                            duration DECIMAL,
                            year INT)
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id INT IDENTITY(0,1) CONSTRAINT songplays_pk PRIMARY KEY,
                        start_time TIMESTAMP REFERENCES time (start_time), 
                        user_id INT REFERENCES users (user_id), 
                        level VARCHAR NOT NULL, 
                        song_id VARCHAR REFERENCES songs (song_id), 
                        artist_id VARCHAR REFERENCES artists (artist_id), 
                        session_id INT NOT NULL, 
                        location VARCHAR, 
                        user_agent VARCHAR)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                    user_id INT IDENTITY(0,1) PRIMARY KEY,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender CHAR(1),
                    level VARCHAR NOT NULL)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR CONSTRAINT songs_pk PRIMARY KEY, 
                    title VARCHAR NOT NULL, 
                    artist_id VARCHAR REFERENCES artists (artist_id), 
                    year INT NOT NULL, 
                    duration DECIMAL NOT NULL)
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                        artist_id VARCHAR CONSTRAINT artists_pk PRIMARY KEY, 
                        name VARCHAR NOT NULL, 
                        location VARCHAR, 
                        latitude DECIMAL, 
                        longitude DECIMAL)
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                    start_time TIMESTAMP CONSTRAINT time_pk PRIMARY KEY, 
                    hour INT, 
                    day INT, 
                    week INT, 
                    month INT, 
                    year INT, 
                    weekday VARCHAR NOT NULL)
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    JSON {}                 
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (""" copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'                 
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, 
                        artist_id, session_id, location, user_agent) 
                        
                        SELECT DISTINCT 
                            TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
                            se.userId, 
                            se.level,
                            ss.song_id, 
                            ss.artist_id,
                            se.sessionId, 
                            se.location, 
                            se.userAgent
                        FROM staging_events se, staging_songs ss 
                        WHERE se.page = 'NextSong' 
                        AND ss.title = se.song 
                        AND ss.artist_name = se.artist
                        AND ss.duration = se.length
                        AND ss.song_id is NOT NULL
""")

user_table_insert = (""" INSERT INTO users (first_name, last_name, gender, level) 

                        SELECT DISTINCT 
                            firstName,
                            lastName,
                            gender,
                            level
                        FROM staging_events 
                        WHERE page = 'NextSong' 
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)

                        SELECT DISTINCT 
                            song_id,
                            title, 
                            artist_id, 
                            year, 
                            duration 
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude)

                        SELECT DISTINCT 
                            artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude, 
                            artist_longitude
                        FROM staging_songs
                        WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
        SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events      
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
