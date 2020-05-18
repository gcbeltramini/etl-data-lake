import os.path
from typing import Sequence

from utils import log_fn, DfInfo


@log_fn
def etl_load(output_path: str, tables: Sequence[DfInfo]) -> None:
    """
    Write parquet files.

    Parameters
    ----------
    output_path : str
        Path where the parquet files will be saved.
    tables
        Sequence of namedtuples with: table name, Spark DataFrame, file name,
        partition columns.
    """
    for d in tables:
        if d.file_name is not None:
            d.df.write.parquet(os.path.join(output_path, d.file_name),
                               partitionBy=d.partition_by)
