import configparser
import logging
import os
import sys
from functools import wraps
from typing import NamedTuple, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s - %(message)s')


class DfInfo(NamedTuple):
    name: str
    df: DataFrame
    file_name: Optional[str]
    partition_by: Optional[Sequence[str]]


def log_fn(fn):
    @wraps(fn)
    def with_logging(*args, **kwargs):
        logging.info(f'Running `{fn.__name__:s}`...')
        result = fn(*args, **kwargs)
        logging.info(f'`{fn.__name__:s}` finished!')
        return result

    return with_logging


def strip_str(s: str, chars: str = '"\' ') -> str:
    """
    Removing leading and trailing characters from string.

    Parameters
    ----------
    s : str
        String to clean up.
    chars : str
        Characters to remove from beginning and end of `s`.

    Returns
    -------
    str
    """
    return s.strip(chars)


def read_config(config_file: str) -> configparser.ConfigParser:
    """
    Read variables from configuration file.

    Parameters
    ----------
    config_file : str
        Configuration file, with AWS keys AWS_ACCESS_KEY_ID and
        AWS_SECRET_ACCESS_KEY in section "AWS"; and input and output paths.

    Returns
    -------
    dict[str, str]
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def set_aws_creds(access_key_id: str, secret_access_key: str) -> None:
    """
    Set AWS credentials in environment variables "AWS_ACCESS_KEY_ID" and
    "AWS_SECRET_ACCESS_KEY". They must have access to S3.

    Parameters
    ----------
    access_key_id : str
        AWS access key ID.
    secret_access_key : str
        AWS secret access key.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_access_key


@log_fn
def create_spark_session(is_s3_input: bool = True) -> SparkSession:
    """
    Create Spark session.

    Parameters
    ----------
    is_s3_input : bool, optional
        Whether the input files are stored on AWS S3.

    Returns
    -------
    spark : pyspark.sql.session.SparkSession
    """
    # comment out in case of problems, but it's necessary for S3 access
    if is_s3_input:
        return (SparkSession
                .builder
                .config('spark.jars.packages',
                        'org.apache.hadoop:hadoop-aws:2.7.0')
                .getOrCreate())
    else:
        return SparkSession.builder.getOrCreate()
