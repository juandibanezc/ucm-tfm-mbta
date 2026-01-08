"""Module with SCD functions."""
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable


def audit_cols(
    source: DataFrame,
    filename: bool = False,
    scd_key: bool = False,
) -> DataFrame:
    """Create audit columns for SCD."""

    source_cols = source.columns

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

    if scd_key:
        source = source.withColumn(
            "scd_key",
            F.sha2(
                F.concat_ws(
                    "_",
                    *[F.coalesce(F.col(col).cast("string"), F.lit("")) for col in source_cols],
                ),
                256
            )
        )

    return source


def scd1_merge_delta_write(
    source: DataFrame,
    target: DeltaTable,
    keys: List[str],
) -> None:
    """Write dimension data to delta table with SCD type 1 logic."""

    audit_col = ["created_ts", "updated_ts", "scd_key"]

    source_cols = [
        col for col in source.columns
        if col not in keys + audit_col
    ]

    merge_builder = target.alias("t").merge(
        source.alias("s"),
        condition=" AND ".join(
            [f"t.{key} = s.{key}" for key in keys]
        ),
    )

    merge_builder = merge_builder.whenMatchedUpdate(
        condition="s.scd_key <> t.scd_key",
        set=dict(
            **{col: f"s.{col}" for col in source_cols},
            **{"updated_ts": "s.updated_ts"},
        )
    ).whenNotMatchedInsert(
        values={col: f"s.{col}" for col in target.toDF().columns}
    )

    merge_builder.execute()
