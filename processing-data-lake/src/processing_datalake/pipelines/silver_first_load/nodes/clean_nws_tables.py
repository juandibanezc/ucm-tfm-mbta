"""Module to clean NWS tables in the silver layer."""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from processing_datalake.extras.utils.scd_functions import audit_cols


def transform_grids_table(
    source: DataFrame,
) -> DataFrame:
    """Transform NWS points table.

    Args:
        source (DataFrame): Input DataFrame to be transformed.
    Returns:
        DataFrame: Transformed DataFrame.

    """

    table_df = source.select(
        F.col("grid_id"),
        F.col("elevation.unitCode").alias("elevation_unit"),
        F.col("elevation.value").cast("double").alias("elevation_value"),
        F.col("forecast_generator"),
        F.col("generated_at").cast("timestamp").alias("generated_at"),
        F.col("periods").alias("period_forecast"),
        F.col("units"),
        F.col("update_time").cast("timestamp").alias("update_time"),
    )

    return table_df


def transform_point_table(
    source: DataFrame,
) -> DataFrame:
    """Transform NWS point table.

    Args:
        source (DataFrame): Input DataFrame to be transformed.
    Returns:
        DataFrame: Transformed DataFrame.
    """

    table_df = source.select(
        F.col("stop_id"),
        F.col("grid_box"),
        F.col("grid_x").cast("integer").alias("grid_x"),
        F.col("grid_y").cast("integer").alias("grid_y"),
        F.concat_ws("_", F.col("grid_x"), F.col("grid_y")).alias("grid_id"),
    )

    return table_df


def clean_grids_table(
    table: DataFrame,
) -> DataFrame:
    """Clean NWS grids table.

    Args:
        table (DataFrame): Input DataFrame to be cleaned.
    Returns:
        DataFrame: Cleaned DataFrame.

    """

    table_df = transform_grids_table(table)

    return audit_cols(table_df, scd_key=True)


def clean_points_table(
    table: DataFrame,
) -> DataFrame:
    """Clean NWS points table.

    Args:
        table (DataFrame): Input DataFrame to be cleaned.
    Returns:
        DataFrame: Cleaned DataFrame.

    """

    table_df = transform_point_table(table)

    return audit_cols(table_df, scd_key=True)
