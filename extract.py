import os.path
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructType, StructField)

from utils import log_fn, set_aws_creds, create_spark_session


@log_fn
def read_json_data(spark: SparkSession,
                   input_path: str,
                   schema: Optional[StructType] = None
                   ) -> DataFrame:
    """
    Read JSON data.

    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        SparkSession object.
    input_path : str
        Path to the log data (file or folder name).
    schema : pyspark.sql.types.StructType, optional
        Input schema.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """
    return spark.read.json(input_path, schema=schema)


@log_fn
def etl_extract(base_path: str,
                song_path: str,
                log_path: str,
                aws_creds: Optional[Dict[str, str]] = None
                ) -> Dict[str, DataFrame]:
    """
    Perform the "extract" part of the ETL, by reading JSON files.

    Parameters
    ----------
    base_path : str
        Root directory where `song_path` and `log_path` are. It can be a local
        path or a path to S3.
    song_path : str
        Relative path for the song data.
    log_path : str
        Relative path for the log data.
    aws_creds : dict[str, str], optional
        AWS credentials with keys "AWS_ACCESS_KEY_ID" and
        "AWS_SECRET_ACCESS_KEY". Mandatory if `base_path` is in AWS S3.

    Returns
    -------
    dict[table name: str --> pyspark.sql.dataframe.DataFrame]
    """

    is_s3_input = base_path.startswith('s3')

    # Set AWS credentials
    if is_s3_input:
        set_aws_creds(access_key_id=aws_creds['AWS_ACCESS_KEY_ID'],
                      secret_access_key=aws_creds['AWS_SECRET_ACCESS_KEY'])

    spark = create_spark_session(is_s3_input=is_s3_input)

    # Specify schema manually, otherwise an "AnalysisException" will be raised:
    #   "Unable to infer schema for JSON. It must be specified manually.;"
    song_schema = StructType([
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_name', StringType()),
        StructField('duration', DoubleType()),
        StructField('num_songs', IntegerType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('year', IntegerType()),
    ])
    song_data = read_json_data(spark=spark,
                               input_path=os.path.join(base_path, song_path),
                               schema=song_schema)

    log_schema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('itemInSession', LongType()),
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', DoubleType()),
        StructField('sessionId', LongType()),
        StructField('song', StringType()),
        StructField('status', IntegerType()),
        StructField('ts', LongType()),
        StructField('userAgent', StringType()),
        StructField('userId', StringType())
    ])
    log_data = read_json_data(spark=spark,
                              input_path=os.path.join(base_path, log_path),
                              schema=log_schema)

    return {'song_data': song_data,
            'log_data': log_data}
