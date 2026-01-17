"""Module for current load in silver to clean mbta api tables."""
from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from delta.tables import DeltaTable

from processing_datalake.extras.utils.scd_functions import (
    scd1_merge_delta_write,
    audit_cols,
)
from processing_datalake.pipelines.silver_first_load.nodes.clean_mbta_tables import (
    transformation,
)


def clean_current_load(
    table: DataFrame,
    params: Dict[str, Any],
    target_table: DeltaTable,
) -> bool:
    """Clean current load data into silver layer with SCD1 logic.

    Args:
        table (DataFrame): Input table to process.
        params (Dict[str, Any]): Parameters for processing.
        target_table (DeltaTable): Target delta table to write data.
    """

    source = transformation(table, params)

    source = audit_cols(source)

    last_update_ts = params.get("last_ts")

    source = source.filter(F.col("source_file").contains(last_update_ts))

    keys = params.get("keys")

    scd1_merge_delta_write(
        source=source,
        target=target_table,
        keys=keys,
    )

    return True
