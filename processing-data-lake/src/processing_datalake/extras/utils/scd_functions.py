"""Module with SCD functions."""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def audit_cols(
    source: DataFrame,
    filename: bool = False,
) -> DataFrame:
    """Create audit columns for SCD."""

    source = source.withColumns(
        {
            "created_ts": F.current_timestamp(),
            "updated_ts": F.current_timestamp(),
        }
    )

    if filename:
        source = source.withColumn(
            "source_file", F.substring_index(F.input_file_name(), "/", -1)
        )

    return source
