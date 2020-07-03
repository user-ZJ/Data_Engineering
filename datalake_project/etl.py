import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek,hour, weekofyear, date_format
from pyspark.sql import types as T
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    #song_data = input_data+"song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.json(output_data + "songs_table_json", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table
    artists_table.write.json(output_data + "artists_table_json", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'
    #log_data = input_data+"log-data/2018-11-01-events.json"
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.select("*").where(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])
    
    # write users table to parquet files
    users_table.write.json(output_data + "users_table_json", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x))
    df = df.withColumn("timestamp",get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0),T.TimestampType())
    df = df.withColumn("start_time",get_timestamp("timestamp"))
    
    # extract columns to create time table
    time_table = df.withColumn('hour', hour('start_time'))\
        .withColumn('day', dayofmonth('start_time'))\
        .withColumn('week', weekofyear('start_time'))\
        .withColumn('month', month('start_time'))\
        .withColumn('year', year('start_time'))\
        .withColumn('weekday', dayofweek('start_time'))\
        .select('ts', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')\
        .drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.json(output_data + "time_table_json", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(output_data+"songs_table_json")

    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner') \
    .select(monotonically_increasing_id().alias('songplay_id'), 'start_time', 'userId', 'level', 'song_id', \
    'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.json(output_data + "songplays_table_json", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "/home/workspace/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
