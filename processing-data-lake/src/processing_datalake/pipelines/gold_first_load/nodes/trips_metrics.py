"""Module to generate trips metrics table."""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from processing_datalake.extras.utils.scd_functions import audit_cols


def create_trips_metrics_table(
    schedules: DataFrame,
    routes: DataFrame,
    trips: DataFrame,
    route_patterns: DataFrame,
) -> DataFrame:
    """Create trips metrics table."""
    schedules_df = schedules.groupBy(
        "trip_id",
        "route_id",
        "direction_id",
    ).agg(
        F.min("departure_time").alias("departure_time"),
        F.max("arrival_time").alias("arrival_time"),
        F.count("stop_id").alias("num_stops"),
    ).withColumn(
        "duration_minutes",
        (F.unix_timestamp("arrival_time") - F.unix_timestamp("departure_time")) / 60,
    )

    trips_metrics = schedules_df.alias("s").join(
        routes.alias("r"),
        on=F.col("s.route_id") == F.col("r.id"),
        how="left",
    ).join(
        trips.alias("t"),
        on=(
            (F.col("s.trip_id") == F.col("t.id")) &
            (F.col("s.route_id") == F.col("t.route_id")) &
            (F.col("s.direction_id") == F.col("t.direction_id"))
        ),
        how="left",
    ).join(
        route_patterns.alias("rp"),
        on=(
            (F.col("t.direction_id") == F.col("rp.direction_id")) &
            (F.col("t.route_id") == F.col("rp.route_id")) &
            (F.col("t.route_pattern_id") == F.col("rp.id"))
        ),
        how="left",
    ).select(
        F.col("s.trip_id"),
        F.col("s.route_id"),
        F.col("s.direction_id"),
        F.col("s.departure_time"),
        F.col("s.arrival_time"),
        F.col("rp.name").alias("route_pattern_name"),
        F.col("rp.time_desc").alias("route_pattern_time_desc"),
        F.col("t.headsign").alias("trip_headsign"),
        F.col("r.long_name").alias("route_long_name"),
        F.col("r.fare_class").alias("route_fare_class"),
        F.when(
            F.col("s.direction_id") == 0,
            F.col("r.direction_destination_1"),
        ).otherwise(F.col("r.direction_destination_2")).alias("direction_destination"),
        F.col("s.num_stops"),
        F.col("s.duration_minutes"),
    )

    return trips_metrics


def process_trips_metrics(
    schedules: DataFrame,
    routes: DataFrame,
    trips: DataFrame,
    route_patterns: DataFrame
) -> DataFrame:
    """Process trips metrics table."""

    source = create_trips_metrics_table(schedules, routes, trips, route_patterns)

    return audit_cols(source)
