"""Module for current load ingestion."""

from typing import Dict, Any

import pyspark.sql.functions as F

from delta.tables import DeltaTable

from processing_datalake.extras.utils.scd_functions import (
    add_filename_column,
    scd1_merge_delta_write,
    audit_cols,
)
from processing_datalake.pipelines.bronze_first_load.nodes.mbta_ingestion import (
    single_table_processing,
)


def ingest_current_load(
    params: Dict[str, Any],
    last_timestamp: Dict[str, str],
    target_table: DeltaTable,
) -> bool:
    """Ingest current load data into bronze layer with SCD1 logic.

    Args:
        params (Dict[str, Any]): Parameters for processing.
        last_timestamp (Dict[str, str]): Dictionary containing the last execution timestamp.
        target_table (DeltaTable): Target delta table to write data.
    """

    source_df = single_table_processing(params, last_timestamp)

    source = add_filename_column(source_df)

    source = audit_cols(source, scd_key=True)

    last_update_ts = last_timestamp.get("last_ts")

    source = source.filter(F.col("source_file").contains(last_update_ts))

    keys = params.get("keys")
    extra_keys = params.get("extra_keys")

    scd1_merge_delta_write(
        source=source,
        target=target_table,
        keys=keys,
        extra_keys=extra_keys,
    )

    return True
