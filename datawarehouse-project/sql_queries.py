import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events 
    (
      artist     VARCHAR(256),
      auth        VARCHAR(22),
      firstName     VARCHAR(10),
      gender    VARCHAR(2),
      itemInSession      INTEGER,
      lastName       VARCHAR(10),
      length        float,
      level        VARCHAR(5),
      location   VARCHAR(100),
      method       VARCHAR(5),
      page         VARCHAR(20),
      registration     float,
      sessionId       INTEGER,
      song             VARCHAR(256),
      status           INTEGER,
      ts               VARCHAR(16),
      userAgent       VARCHAR(22),
      userId          VARCHAR(22)
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs 
    (
      num_songs     INTEGER NOT NULL,
      artist_id        VARCHAR(22),
      artist_latitude     float,
      artist_longitude    float,
      artist_location      VARCHAR(256),
      artist_name       VARCHAR(256),
      song_id        VARCHAR(22),
      title        VARCHAR(256),
      duration   float,
      year       INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id varchar PRIMARY KEY, 
        start_time varchar NOT NULL, 
        user_id varchar, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id varchar PRIMARY KEY, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY, 
        name varchar, 
        location varchar, 
        latitude float, 
        longitude float);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time varchar PRIMARY KEY, 
        hour varchar, 
        day varchar, 
        week varchar, 
        month varchar, 
        year varchar, 
        weekday varchar);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    iam_role {}
    json 'auto'
    region 'us-west-2'
    ;
""").format('s3://udacity-dend/log_json_path.json',config['IAM_ROLE']['ARN'])
#'s3://udacity-dend/log_data'

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{}'
    iam_role {}
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON 'auto';
""").format('s3://udacity-dend/song_data', config["IAM_ROLE"]["ARN"])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT ts,userId,level,song_id,artist_id,sessionId,location,userAgent  
    FROM (
    SELECT se.ts, se.userId, se.level, sa.song_id, sa.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se
    JOIN
    (SELECT songs.song_id, artists.artist_id, songs.title, artists.name,songs.duration
    FROM songs
    JOIN artists
    ON songs.artist_id = artists.artist_id) AS sa
    ON (sa.title = se.song
    AND sa.name = se.artist
    AND sa.duration = se.length)
    WHERE se.page = 'NextSong');
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
    SELECT DISTINCT userId,firstName,lastName,gender,level FROM staging_events WHERE page='NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
    SELECT DISTINCT song_id,title,artist_id,year,duration FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    SELECT DISTINCT
    start_time,
    EXTRACT(HOUR FROM start_time),
    EXTRACT(DAY FROM start_time),
    EXTRACT(WEEK FROM start_time),
    EXTRACT(MONTH FROM start_time),
    EXTRACT(YEAR FROM start_time),
    EXTRACT(DOW FROM start_time)
    FROM (SELECT DISTINCT '1970-01-01'::date + ts/1000 * interval '1 second' as start_time
    FROM staging_events WHERE page='NextSong');
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
#insert_table_queries = [ songplay_table_insert]