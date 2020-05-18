from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from utils import log_fn, DfInfo


@log_fn
def create_songs_table(song_data: DataFrame) -> DataFrame:
    """
    Process the song data and create table "songs". Processing:
    - select the relevant columns
    - song ID cannot be empty
    - if there is a conflict in the song ID, to keep one line per song, get the
      largest value of all attributes (arbitrary decision), which also gives
      preference to filled values over empty values

    Parameters
    ----------
    song_data : pyspark.sql.dataframe.DataFrame
        Song data.

    Returns
    -------
    songs : pyspark.sql.dataframe.DataFrame
    """
    cols = ('song_id', 'title', 'artist_id', 'year', 'duration')
    agg = (F.max(c).alias(c) for c in cols[1::])
    return (song_data
            .select(*cols)
            .where(F.col(cols[0]).isNotNull())
            .groupBy(cols[0])
            .agg(*agg))  # aggregate and rename cols to keep the original name


@log_fn
def create_artists_table(song_data: DataFrame) -> DataFrame:
    """
    Process the song data and create table "artists". Processing:
    - select the relevant columns
    - artist ID cannot be empty
    - if there is a conflict in the artist ID, to keep one line per artist, get
      the smallest value of all attributes (trying to keep the original artist
      name when there are invited artists in the song; arbitrary decision for
      the other attributes), which also gives preference to filled values over
      empty values
    - rename the columns

    Parameters
    ----------
    song_data : pyspark.sql.dataframe.DataFrame
        Song data.

    Returns
    -------
    artists : pyspark.sql.dataframe.DataFrame
    """
    cols = ('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
            'artist_longitude')
    agg = (F.min(c).alias(c.replace('artist_', '')) for c in cols[1::])
    return (song_data
            .select(*cols)
            .where(F.col(cols[0]).isNotNull())
            .groupBy(cols[0])
            .agg(*agg))  # aggregate and rename columns


@log_fn
def create_users_table(log_data: DataFrame) -> DataFrame:
    """
    Process the log data and create table "users". Processing:
    - select the relevant columns and rename them
    - keep only the latest row for every user (they can change "level")

    Parameters
    ----------
    log_data : pyspark.sql.dataframe.DataFrame
        Log data.

    Returns
    -------
    users, time : pyspark.sql.dataframe.DataFrame
    """
    return (log_data
            .select(F.col('userId').cast('integer').alias('user_id'),
                    F.col('firstName').alias('first_name'),
                    F.col('lastName').alias('last_name'),
                    'gender',
                    'level',
                    'ts')
            .withColumn('row', F.row_number().over((Window
                                                    .partitionBy('user_id')
                                                    .orderBy('ts'))))
            .where(F.col('row') == 1)
            .drop('ts', 'row'))


@log_fn
def create_time_table(log_data: DataFrame) -> DataFrame:
    """
    Process the log data and create table "time". Processing:
    - timestamp cannot be empty
    - select distinct timestamps
    - convert unix timestamp into timestamp
    - extract date parts

    Parameters
    ----------
    log_data : pyspark.sql.dataframe.DataFrame
        Log data.

    Returns
    -------
    time : pyspark.sql.dataframe.DataFrame
    """
    return (log_data
            .where(F.col('ts').isNotNull())
            .select('ts').distinct()
            .withColumn('start_time',
                        F.from_unixtime(F.col('ts') / 1000).cast('timestamp'))
            .select('start_time',
                    F.hour('start_time').alias('hour'),
                    F.dayofmonth('start_time').alias('day'),
                    F.weekofyear('start_time').alias('week'),
                    F.month('start_time').alias('month'),
                    F.year('start_time').alias('year'),
                    F.dayofweek('start_time').alias('weekday')))


@log_fn
def create_songplays(song_data: DataFrame, log_data: DataFrame) -> DataFrame:
    """
    Process the song and log data and create table "songplays". Processing:
    - user ID and timestamp cannot be empty
    - convert unix timestamp into timestamp
    - join the song and log tables (explanation below)
    - add an ID column
    - select the relevant columns and rename them

    Since there is no song ID nor artist ID in the log data, we need to join
    both tables using song title, artist name and song duration, which should
    uniquely describe a song. And this is the best we can do, because there is
    no other information about the songs in the log data.

    Even though some songs have the same artist and title, they don't have the
    same duration. So we will not use this information to join the tables.

    Parameters
    ----------
    song_data : pyspark.sql.dataframe.DataFrame
        Song dataset.
    log_data : pyspark.sql.dataframe.DataFrame
        Log dataset.

    Returns
    -------
    songplays : pyspark.sql.dataframe.DataFrame
    """
    return (log_data
            .alias('logs')
            .where((F.col('userId').isNotNull()) & (F.col('ts').isNotNull()))
            .withColumn('start_time',
                        F.from_unixtime(F.col('ts') / 1000).cast('timestamp'))
            .join(song_data.alias('songs'),
                  (F.col('logs.song') == F.col('songs.title')) &
                  (F.col('logs.artist') == F.col('songs.artist_name')),
                  'inner')
            .withColumn('songplay_id', F.monotonically_increasing_id())
            .select('songplay_id',
                    'start_time',
                    F.year('start_time').alias('year'),
                    F.month('start_time').alias('month'),
                    F.col('logs.userId').alias('user_id'),
                    'logs.level',
                    'songs.song_id',
                    'songs.artist_id',
                    F.col('logs.sessionId').alias('session_id'),
                    'logs.location',
                    F.col('logs.userAgent').alias('user_agent')))


@log_fn
def etl_transform(song_data: DataFrame, log_data: DataFrame
                  ) -> Dict[str, DfInfo]:
    """
    Perform the "transform" part of the ETL.

    Parameters
    ----------
    song_data : pyspark.sql.dataframe.DataFrame
        Song data.
    log_data : pyspark.sql.dataframe.DataFrame
        Log data.

    Returns
    -------
    dict[table name: str --> namedtuples with: table name, Spark DataFrame,
                                               file name, partition columns]
    """
    log_data_songplays = log_data.where(log_data['page'] == 'NextSong')

    songs = create_songs_table(song_data)
    artists = create_artists_table(song_data)
    users = create_users_table(log_data_songplays)
    time = create_time_table(log_data_songplays)
    songplays = create_songplays(song_data, log_data_songplays)
    return {'log_data_songplays': DfInfo('log_data_songplays',
                                         log_data_songplays, None, None),
            'songs': DfInfo('songs', songs, 'songs.parquet',
                            ['year', 'artist_id']),
            'artists': DfInfo('artists', artists, 'artists.parquet', None),
            'users': DfInfo('users', users, 'users.parquet', None),
            'time': DfInfo('time', time, 'time.parquet', ['year', 'month']),
            'songplays': DfInfo('songplays', songplays, 'songplays.parquet',
                                ['year', 'month'])}
