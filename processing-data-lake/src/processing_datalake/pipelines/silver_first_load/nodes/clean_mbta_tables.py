"""Module for silver data cleaning."""

from typing import Dict, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from processing_datalake.extras.utils.scd_functions import audit_cols


def _build_schema(schema: Dict[str, str]) -> Dict[str, F.Column]:
    """Build schema with pyspark.sql.Column types from string types.

    Args:
        schema (Dict[str, str]): Dictionary with column names and their string types.

    Returns:
        Dict[str, F.Column]: Dictionary with column names and their pyspark Column types.
    """
    return {key: F.col(key).cast(value) for key, value in schema.items()}


def transformation(
    table: DataFrame,
    params: Dict[str, Any],
) -> DataFrame:
    """Transform MBTA tables according to the provided schema.

    Args:
        table (DataFrame): Input DataFrame to be transformed.
        params (Dict[str, Any]): Parameters containing the schema for transformation.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    schema = params.get("schema")
    route_table = params.get("route_table")

    audit_cols = ["created_ts", "updated_ts", "source_file", "scd_key"]

    columns = [col for col in table.columns if col not in audit_cols]

    table = table.select(*columns)

    casted_columns = _build_schema(schema)

    if route_table:
        # Special handling for route_table to explode array of strings and
        # assure that always have 2 elements in struct.
        table = table.withColumns({
            "direction_name_1": F.col("direction_names")[0],
            "direction_name_2": F.col("direction_names")[1],
            "direction_destination_1": F.col("direction_destinations")[0],
            "direction_destination_2": F.col("direction_destinations")[1],
        }).drop("direction_names", "direction_destinations")

    table_cleaned = table.withColumns(casted_columns)

    return table_cleaned


def clean_tables(
    table: DataFrame,
    params: Dict[str, Any],
) -> DataFrame:
    """Clean MBTA tables.

    Args:
        table (DataFrame): Input DataFrame to be cleaned.
        params (Dict[str, Any]): Parameters for cleaning.

    Returns:
        DataFrame: Cleaned DataFrame.
    """

    cleaned_table = transformation(table, params)

    return audit_cols(cleaned_table, scd_key=True)
