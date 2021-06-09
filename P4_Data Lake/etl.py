import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: extracts song_data from S3, processes it, and loads the songs and artist tables
                back to S3
        
    Parameters:
            spark       : Spark Session
            input_data  : input files path in S3 bucket
            output_data : output file path
    """
    # get filepath to song data file
    
    song_data = f'{input_data}*/*/*/*.json'
    
#     song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates().cache()

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description: extracts log_data from S3, processes it, and loads user, time, and songsplay
                tables back to S3
        
    Parameters:
            spark       : Spark Session
            input_data  : input files path in S3 bucket
            output_data : output file path
    """
    # get filepath to log data file
    
    log_data = f'{input_data}*/*/*events.json'

#     log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data).dropDuplicates().cache()
    
    # filter by actions for song plays
    songplays_table = df['ts', 'userId', 'level','sessionId', 'location', 'userAgent']

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("users.parquet completed")

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x / 1000), TimestampType()
    )
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('start_time', get_timestamp(col('ts')))
    
    # extract columns to create time table
    time_table = df.select(
        col('start_time'), 
        col('hour'), 
        col('day'), 
        col('week'),
        col('month'),
        col('year'), 
        col('weekday')
    ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet(f'{output_data}time/time_table.parquet')

    print("time.parquet completed")
    
    # read in song data to use for songplays table
    song_df = os.path.join(input_data, "song-data/A/A/A/*.json")
    song_df = spark.read.json(song_data)
    
    df = df.join(song_df, song_df.title == df.song)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(
        col('ts'),
        col('userId'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('ssessionId'),
        col('location'),
        col('userAgent'),
        col('year'),
        month('datetime')
    ).distinct()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(f'{output_data}songplays/songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://dend-udacity/"
    output_data = "s3a://loaded-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
