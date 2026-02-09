"""Module to clean NWS tables in the silver layer on current load."""
from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from delta.tables import DeltaTable

from processing_datalake.extras.utils.scd_functions import (
    scd1_merge_delta_write,
    audit_cols,
)
from processing_datalake.pipelines.silver_first_load.nodes.clean_nws_tables import (
    transform_grids_table,
    transform_point_table,
)


def clean_current_load(
    table: DataFrame,
    params: Dict[str, Any],
    last_timestamp: Dict[str, str],
    target_table: DeltaTable,
) -> bool:
    """Clean current load data into silver layer with SCD1 logic.

    Args:
        table (DataFrame): Input table to process.
        params (Dict[str, Any]): Parameters for processing.
        target_table (DeltaTable): Target delta table to write data.
    """
    last_update_ts = last_timestamp.get("last_ts")
    grids = params.get("grids")

    table = table.filter(F.col("source_file").contains(last_update_ts))

    source = transform_grids_table(table) if grids else transform_point_table(table)

    source = audit_cols(source, scd_key=True)

    keys = params.get("keys")

    scd1_merge_delta_write(
        source=source,
        target=target_table,
        keys=keys,
    )

    return True
