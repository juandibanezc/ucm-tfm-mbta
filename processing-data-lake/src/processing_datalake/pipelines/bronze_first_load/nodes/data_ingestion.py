"""Module to ingest json raw data into bronze layer."""

from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from processing_datalake.extras.utils.scd_functions import audit_cols


def single_table_processing(
    table: DataFrame,
    params: Dict[str, Any]
) -> DataFrame:
    """Process single table dimension data."""

    columns = params.get("columns")

    table = table.withColumn("data", F.explode(F.col("data")))

    source = table.selectExpr(*columns)

    return source


def process_table(
    table: DataFrame,
    params: Dict[str, Any]
) -> DataFrame:
    """Process all tables dimension data first load
    Args:
        table (DataFrame): Input table to process
        params (Dict[str, Any]): Parameters for processing

    Returns:
        DataFrame: Processed table.
    """

    source = single_table_processing(table, params)

    source = audit_cols(source, filename=True, scd_key=True)

    last_update_ts = params.get("last_ts")

    source = source.filter(F.col("source_file").contains(last_update_ts))

    return source
