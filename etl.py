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
    # get filepath to song data file
    song_data = f'{input_data}/song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs.parquet', partitionBy=('year', 'artist_id'), mode='overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet', partitionBy=("artist_id"), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    
    #log_data = f'{input_data}/log_data/*.json'
    log_data = f'{input_data}/*.json'

    # read log data file
    
    df = spark.read.json(log_data,mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    
  #  print(df)
    
    # filter by actions for song plays
    df = df.where(df['page']=="NextSong")
    

    # extract columns for users table    
    user_table = df.selectExpr(["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"])
    
    # write users table to parquet files
    user_table.write.parquet(output_data + 'users.parquet', partitionBy=("user_id"), mode='overwrite')

    # create timestamp column from original timestamp column
    #get_timestamp = udf(lambda x: x)
    #df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df = df.withColumn('start_time', from_unixtime(col('ts')/1000))
    time_table = df.select('start_time') \
    .withColumn("hour", hour("start_time")) \
    .withColumn("day", dayofmonth("start_time")) \
    .withColumn("month", month("start_time")) \
    .withColumn("week", weekofyear("start_time")) \
    .withColumn("year", year(df.start_time)) \
    .withColumn("weekday", dayofweek("start_time"))
    
    # extract columns to create time table
    #time_table = df.withColumn("hour",hour("start_time"))\
     #           .withColumn("day",dayofmonth("start_time"))\
     #           .withColumn("week",weekofyear("start_time"))\
     #           .withColumn("month",month("start_time"))\
     #           .withColumn("year",year("start_time"))\
     #           .withColumn("weekday",dayofweek("start_time"))\
     #           .select("start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = read_song_data(spark, input_data)
    
    song_df.createOrReplaceTempView("song_table")
    #log_df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    #log_df.createOrReplaceTempView('logs_table')
    df.createOrReplaceTempView("log_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  spark.sql("""
                                SELECT
                                    l.songplay_id,
                                    l.datetime as start_time,
                                    year(l.datetime) as year,
                                    month(l.datetime) as month,
                                    l.userId as user_id,
                                    l.level,
                                    s.song_id,
                                    s.artist_id,
                                    l.sessionId as session_id,
                                    l.location,
                                    l.userAgent as user_agent
                                FROM logs_table l
                                LEFT JOIN songs_table s ON
                                    l.song = s.title AND
                                    l.artist = s.artist_name
                                """) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays.parquet", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparky-dend/"
    
    #input_data = "data"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

