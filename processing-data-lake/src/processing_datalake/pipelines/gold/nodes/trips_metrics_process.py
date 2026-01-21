"""Module to process trips metrics data."""
from typing import Dict, Any
from datetime import datetime

from pyspark.sql import DataFrame

from processing_datalake.pipelines.gold_first_load.nodes.trips_metrics import (
    create_trips_metrics_table,
)
from processing_datalake.extras.utils.scd_functions import (
    incremental_load,
)


def process_trips_metrics(
    silver_schedules: DataFrame,
    silver_routes: DataFrame,
    silver_trips: DataFrame,
    silver_route_patterns: DataFrame,
    params: Dict[str, Any],
) -> bool:
    """Process trips metrics data and write to gold delta table."""

    # Create trips metrics table
    trips_metrics_df = create_trips_metrics_table(
        silver_schedules,
        silver_routes,
        silver_trips,
        silver_route_patterns,
    )

    last_update_ts = params.get("last_ts")
    last_update_ts_formatted = str(last_update_ts)
    year = last_update_ts_formatted[:4]
    month = last_update_ts_formatted[4:6]
    day = int(last_update_ts_formatted[6:8]) - 1
    save_date = datetime(int(year), int(month), day).strftime("%Y-%m-%d")
    target_dataset = params.get("catalog_dataset")

    trips_metrics_df = trips_metrics_df.filter(f"service_date>='{save_date}'")
    # Write to gold delta table with predicate.
    incremental_load(
        source=trips_metrics_df,
        partition_predicate=f"service_date>='{save_date}'",
        catalog_table=target_dataset,
    )

    return True
