"""Module to ingest json raw data into bronze layer."""
from pathlib import PurePosixPath
from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from processing_datalake.extras.utils import get_dataset
from processing_datalake.extras.utils.scd_functions import (
    audit_cols,
    add_filename_column,
)


def single_table_processing(
    params: Dict[str, Any],
    last_update_ts: Dict[str, Any],
) -> DataFrame:
    """Process single table dimension data."""
    last_update_ts_formatted = last_update_ts.get("last_ts")
    year = last_update_ts_formatted[:4]
    month = last_update_ts_formatted[4:6]
    day = last_update_ts_formatted[6:8]
    catalog_table = params.get("catalog_dataset")

    table_catalog = get_dataset(catalog_table)
    file_path = str(table_catalog._filepath).format(
        last_ts=last_update_ts_formatted,
        year=year,
        month=month,
        day=day,
    )
    file_path_formatted = PurePosixPath(file_path)
    table_catalog._filepath = file_path_formatted

    table: DataFrame = table_catalog.load()

    columns = params.get("columns")

    table = table.withColumn("data", F.explode(F.col("data")))

    source = table.selectExpr(*columns)

    return source


def process_table(
    params: Dict[str, Any],
    last_update_ts: Dict[str, Any],
) -> DataFrame:
    """Process all tables dimension data first load
    Args:
        params (Dict[str, Any]): Parameters for processing

    Returns:
        DataFrame: Processed table.
    """

    table_ingest = single_table_processing(params, last_update_ts)

    source = add_filename_column(table_ingest)

    source = audit_cols(source, scd_key=True)

    source = source.filter(F.col("source_file").contains(last_update_ts))

    return source
