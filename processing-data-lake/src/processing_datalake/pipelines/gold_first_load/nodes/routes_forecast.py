"""Module to process routes forecast with NWS API."""
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W

from pyspark.sql import DataFrame

from processing_datalake.extras.utils.scd_functions import audit_cols


def process_routes_forecast(
    grids: DataFrame,
    points: DataFrame,
    schedules: DataFrame,
) -> DataFrame:
    """Processs the routes forecast with NWS API.

    Args:
        grids (DataFrame): DataFrame with the grids information.
        points (DataFrame): DataFrame with the points information.
        schedules (DataFrame): DataFrame with the schedules information.
    Returns:
        DataFrame: The routes with forecast information.
    """

    # Creates window.
    w_max = W.partitionBy("generated_at", "grid_id").orderBy(F.col("probability_precipitation").desc())
    # Transform the schedules dataframe to get the service date and other relevant information
    schedules_df = schedules.select(
        F.to_date(F.date_format(
            F.col("departure_time"), "yyyy-MM-dd"
        )).alias("service_date"),
        F.col("trip_id"),
        F.col("route_id"),
        F.col("direction_id"),
        F.col("departure_time"),
        F.col("arrival_time"),
        F.col("stop_id")
    )
    # Transform the grids dataframe to get the grid_id, generated_at and the period_forecast information
    grids_df = grids.select(
        F.col("grid_id"),
        F.col("generated_at"),
        F.explode(F.col("period_forecast")).alias("period_forecast"),
    )

    grids_table = grids_df.select(
        F.col("grid_id"),
        F.col("generated_at"),
        F.col("period_forecast.name").alias("name"),
        F.col("period_forecast.probabilityOfPrecipitation.value").alias("probability_precipitation"),
        F.col("period_forecast.temperature").alias("temperature"),
        F.col("period_forecast.shortForecast").alias("short_forecast"),
        F.col("period_forecast.startTime").alias("start_time"),
    ).withColumn(
        "generated_at",
        F.to_date(
            F.date_format(F.col("start_time"), "yyyy-MM-dd")
        )
    )

    grids_table_df = grids_table.withColumn(
        "max_prob_precipitation",
        F.row_number().over(w_max)
    ).filter(
        F.col("max_prob_precipitation") == 1
    ).drop("max_prob_precipitation")

    table = schedules_df.alias("s").join(
        points.alias("p"),
        F.col("s.stop_id") == F.col("p.stop_id"),
        "left"
    ).select(
        F.col("s.*"),
        F.col("p.grid_id")
    )

    table = table.groupBy(
        "service_date",
        "trip_id",
        "route_id",
        "grid_id",
    ).agg(
        F.count("stop_id").alias("num_stops"),
    )

    final_metrics = table.alias("t").join(
        grids_table_df.alias("g"),
        (F.col("t.service_date") == F.col("g.generated_at")) &
        (F.col("t.grid_id") == F.col("g.grid_id")),
        "left"
    ).select(
        F.col("t.service_date"),
        F.col("t.trip_id"),
        F.col("t.route_id"),
        F.col("g.probability_precipitation").alias("max_probability_precipitation"),
        F.col("g.temperature").alias("temperature"),
        F.col("g.short_forecast").alias("forecast")
    ).distinct().withColumn(
        "row_number_temp",
        F.row_number().over(
            W.partitionBy("service_date", "trip_id", "temperature")
            .orderBy(F.col("max_probability_precipitation").desc())
        )
    ).filter(
        (F.col("service_date").isNotNull())
        & (F.col("max_probability_precipitation").isNotNull())
        & (F.col("row_number_temp") == 1)
    ).drop("row_number_temp")

    return final_metrics


def create_routes_forecast(
    grids: DataFrame,
    points: DataFrame,
    schedules: DataFrame,
) -> DataFrame:
    """Processs the routes forecast with NWS API.

    Args:
        grids (DataFrame): DataFrame with the grids information.
        points (DataFrame): DataFrame with the points information.
        schedules (DataFrame): DataFrame with the schedules information.
    Returns:
        DataFrame: The routes with forecast information.
    """

    source = process_routes_forecast(
        grids=grids,
        points=points,
        schedules=schedules,
    )

    return audit_cols(source)
