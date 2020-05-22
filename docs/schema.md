# Schema for song play analysis

The goal is to create a star schema optimized for queries on song play analysis. This includes the
following tables.

The following [Spark data types](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)
were used:

- Integer, i.e. a signed 32-bit integer in the range `[-2,147,483,648, 2,147,483,647]`
- Long, i.e. a signed 64-bit integer in the range
`[-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]`
- Double, representing double precision floats
- String
- Timestamp (`datetime.datetime`)

## JSON files

They are temporary tables to stage the data before loading them into the star schema tables. They
shouldn't be used for analytical purposes. The tables are:

- Log data in `s3://udacity-dend/log_data` has keys:
  - `artist`, `auth`, `firstName`, `gender`, `itemInSession`, `lastName`, `length`, `level`,
  `location`, `method`, `page`, `registration`, `sessionId`, `song`, `status`, `ts`, `userAgent`,
  `userId`
- Song data in `s3://udacity-dend/song_data` has keys:
  - `num_songs`, `artist_id`, `artist_latitude`, `artist_longitude`, `artist_location`,
  `artist_name`, `song_id`, `title`, `duration`, `year`

## Fact table

1. **songplays**: records in log data associated with song plays, i.e., records with page
`NextSong`. The table will be partitioned by year and month when saved as parquet.
   - `songplay_id`: long (no missing nor repeated values)
   - `start_time`: timestamp
   - `year`: integer
   - `month`: integer
   - `user_id`: string
   - `level`: string
   - `song_id`: string
   - `artist_id`: string
   - `session_id`: long
   - `location`: string
   - `user_agent`: string
  
   To join the log and song data, we must find the common keys:
   - log data has keys about:
     - Artist information: name
     - Song information: title, duration
   - song data has keys about:
     - Artist information: ID, name, latitude, longitude, location
     - Song information: ID, title, duration, year

   The possible columns for the join operation are:
   - artist name
   - song title
   - song duration

   The join was made using only artist name and song title, because some songs may not have the same
   duration although belong to the same artist and have the same title.

## Dimension tables

1. **users**: users in the app
   - `user_id`: integer (no missing nor repeated values)
   - `first_name`, `last_name`, `gender`, `level`: string

   The log data contain users that didn't play any song (they were dropped) and users who changed
   levels - paid and free (to avoid duplicates, the latest value was kept for each user ID).

1. **songs**: songs in music database
   - `song_id`: string (no missing nor repeated values)
   - `title`, `artist_id`: string
   - `year`: integer
   - `duration`: double

   To keep only unique song IDs, we selected the largest value of all attributes (arbitrary
   decision), which also gives preference to filled values over empty values.

1. **artists**: artists in music database
   - `artist_id`: string (no missing nor repeated values)
   - `name`, `location`: string
   - `latitude`, `longitude`: double

   In the song data, the same artist (same artist ID) may have different artist names, usually
   associated with songs with other invited artists. We chose the smallest value of the name,
   possibly leaving only the original artist, since it may be shorter. This also gives preference to
   filled values over empty values. We used the same aggregation for the other columns (arbitrary
   decision).

1. **time**: timestamps of records in **songplays** broken down into specific units
   - `start_time`: timestamp (no missing nor repeated values)
   - `hour`, `day`, `week`, `month`, `year`, `weekday`: integer

   Since the fact table `songplays` has only timestamps of when a song was played, the time table
   contains only those instants.
