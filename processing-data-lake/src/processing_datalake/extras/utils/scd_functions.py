"""Module with SCD functions."""
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from processing_datalake.hooks import get_context


def add_filename_column(
    source: DataFrame,
) -> DataFrame:
    """Add source filename column to DataFrame."""

    return source.withColumn(
        "source_file", F.substring_index(F.input_file_name(), "/", -1)
    )


def audit_cols(
    source: DataFrame,
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
    extra_keys: str = None,
) -> None:
    """Write dimension data to delta table with SCD type 1 logic."""

    audit_col = ["created_ts", "updated_ts", "scd_key"]

    source_cols = [
        col for col in source.columns
        if col not in keys + audit_col
    ]

    if extra_keys:
        keys.append(extra_keys)

    merge_builder = target.alias("t").merge(
        source.alias("s"),
        condition=" AND ".join(
            [f"t.{key}=s.{key}" for key in keys]
        ),
    )

    merge_builder = merge_builder.whenMatchedUpdate(
        condition="s.scd_key <> t.scd_key",
        set=dict(
            **{col: f"s.{col}" for col in source_cols},
            **{
                "updated_ts": "s.updated_ts",
                "scd_key": "s.scd_key",
            },
        )
    ).whenNotMatchedInsert(
        values={col: f"s.{col}" for col in target.toDF().columns}
    )

    merge_builder.execute()


def incremental_load(
    source: DataFrame,
    partition_predicate: str,
    catalog_table: str,
) -> None:
    """Perform incremental load from source to target delta table."""

    context = get_context()

    catalog = context.catalog

    catalog_dataset = catalog._get_dataset(catalog_table)

    catalog_dataset._save_args['replaceWhere'] = partition_predicate

    source = audit_cols(source)

    catalog_dataset.save(source)
