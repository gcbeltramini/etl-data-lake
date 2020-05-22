# Project datasets

There are two datasets that reside in S3:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

## Song dataset

- Subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).
- Each file is in JSON format and contains metadata about a song and the artist of that song.
- The files are partitioned by the first three letters of each song's track ID. For example, here
are filepaths to two files in this dataset:
  - `song_data/A/B/C/TRABCEI128F424C983.json`
  - `song_data/A/A/B/TRAABJL12903CDCF1A.json`
- Example of the song file `song_data/A/A/B/TRAABCL128F4286650.json` (it's possible to view it in
the web browser: <http://udacity-dend.s3.amazonaws.com/song_data/A/A/B/TRAABCL128F4286650.json>):

  ```json
  {
    "artist_id": "ARC43071187B990240",
    "artist_latitude": null,
    "artist_location": "Wisner, LA",
    "artist_longitude": null,
    "artist_name": "Wayne Watson",
    "duration": 245.21098,
    "num_songs": 1,
    "song_id": "SOKEJEJ12A8C13E0D0",
    "title": "The Urgency (LP Version)",
    "year": 0
  }
  ```

## Log dataset

- Log files in JSON format generated by [this event simulator](https://github.com/Interana/eventsim)
based on the songs in the dataset above. These logs simulate app activity logs from an imaginary
music streaming app based on configuration settings.
- The log files in the dataset are partitioned by year and month. For example, here are filepaths to
two files in this dataset:
  - `log_data/2018/11/2018-11-12-events.json`
  - `log_data/2018/11/2018-11-13-events.json`
- Example of two lines in log file `log_data/2018/11/2018-11-01-events.json` (it's possible to view
it in the web browser: <http://udacity-dend.s3.amazonaws.com/log_data/2018/11/2018-11-01-events.json>):

  ```text
  {"artist":null,"auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":2,"lastName":"Summers","length":null,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"GET","page":"Upgrade","registration":1540344794796.0,"sessionId":139,"song":null,"status":200,"ts":1541106132796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
  {"artist":"Mr Oizo","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":3,"lastName":"Summers","length":144.03873,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Flat 55","status":200,"ts":1541106352796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
  ```