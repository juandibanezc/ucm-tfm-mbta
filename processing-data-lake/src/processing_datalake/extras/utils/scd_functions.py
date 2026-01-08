"""Module with SCD functions."""
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable


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


def scd1_merge_delta_write(
    source: DataFrame,
    target: DeltaTable,
    keys: List[str],
) -> None:
    """Write dimension data to delta table with SCD type 1 logic."""

    source_cols = [
        col for col in source.columns if col not in keys
    ]

    source = audit_cols(source)

    merge_builder = target.alias("t").merge(
        source.alias("s"),
        condition=" AND ".join(
            [f"t.{key} = s.{key}" for key in keys]
        ),
    )

    merge_builder = merge_builder.whenMatchedUpdate(
        set=dict(
            **{col: f"s.{col}" for col in source_cols},
            **{"updated_ts": "s.updated_ts"},
        )
    ).whenNotMatchedInsert(
        values={col: f"s.{col}" for col in target.toDF().columns}
    )

    merge_builder.execute()
