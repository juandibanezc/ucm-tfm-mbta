"""Module for current load ingestion."""

from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from delta.tables import DeltaTable

from processing_datalake.extras.utils.scd_functions import (
    scd1_merge_delta_write,
    audit_cols,
)
from processing_datalake.pipelines.bronze_first_load.nodes.data_ingestion import (
    process_table,
)


def ingest_current_load(
    table: DataFrame,
    params: Dict[str, Any],
    target_table: DeltaTable,
) -> bool:
    """Ingest current load data into bronze layer with SCD1 logic.

    Args:
        table (DataFrame): Input table to process.
        params (Dict[str, Any]): Parameters for processing.
        target_table (DeltaTable): Target delta table to write data.
    """

    source = process_table(table, params)

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
