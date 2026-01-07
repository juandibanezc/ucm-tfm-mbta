"""Module with SCD functions."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def audit_cols(
    source: DataFrame
) -> DataFrame:
    """Create audit columns for SCD."""

    source = source.withColumns(
        {
            "created_ts": F.current_timestamp(),
            "updated_ts": F.current_timestamp(),
        }
    )

    return source
