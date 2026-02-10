"""Module for processing routes forecast data and writing to gold delta table."""
from typing import Dict, Any
from datetime import datetime

from pyspark.sql import DataFrame

from processing_datalake.pipelines.gold_first_load.nodes.routes_forecast import (
    process_routes_forecast,
)
from processing_datalake.extras.utils.scd_functions import (
    incremental_load,
)


def process_route_forecast_metrics(
    grids: DataFrame,
    points: DataFrame,
    schedules: DataFrame,
    last_timestamp: Dict[str, Any],
    params: Dict[str, Any],
) -> bool:
    """
    Process current routes forecast data and write to gold delta table.

    Args:
        grids (DataFrame): DataFrame with the grids information.
        points (DataFrame): DataFrame with the points information.
        schedules (DataFrame): DataFrame with the schedules information.
        last_timestamp (Dict[str, Any]): Dictionary with the last execution timestamp.
        params (Dict[str, Any]): Dictionary with the parameters for the pipeline.
    Returns:
        bool: True if the process is successful.
    """

    source = process_routes_forecast(
        grids=grids,
        points=points,
        schedules=schedules,
    )

    last_update_ts = last_timestamp.get("last_ts")
    last_update_ts_formatted = str(last_update_ts)
    year = last_update_ts_formatted[:4]
    month = last_update_ts_formatted[4:6]
    day = int(last_update_ts_formatted[6:8]) - 1
    save_date = datetime(int(year), int(month), day).strftime("%Y-%m-%d")
    target_dataset = params.get("catalog_dataset")

    source = source.filter(f"service_date>='{save_date}'")
    # Write to gold delta table with predicate.
    incremental_load(
        source=source,
        partition_predicate=f"service_date>='{save_date}'",
        catalog_table=target_dataset,
    )

    return True
