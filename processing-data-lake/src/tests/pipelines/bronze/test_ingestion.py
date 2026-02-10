"""Tests for bronze current-load ingestion pipeline."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.bronze import pipeline
from processing_datalake.pipelines.bronze.nodes import ingestion


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.filter.return_value = df
    return df


def test_ingest_current_load_calls_scd_and_filters():
    """ingest_current_load should filter by last_ts and call scd1_merge_delta_write."""

    params = {"keys": ["k1"], "extra_keys": ["k2"]}
    last_ts = {"last_ts": "20240102030405"}

    source_df = _df_mock("source")
    with (
        patch.object(ingestion, "single_table_processing", return_value=source_df) as mock_single,
        patch.object(ingestion, "add_filename_column", side_effect=lambda df: df) as mock_add_filename,
        patch.object(ingestion, "audit_cols", side_effect=lambda df, scd_key: df) as mock_audit,
        patch.object(ingestion.F, "col", side_effect=lambda name: MagicMock(name=f"col({name})")),
        patch.object(ingestion, "scd1_merge_delta_write") as mock_scd,
    ):

        target = MagicMock(name="delta_table")
        result = ingestion.ingest_current_load(params, last_ts, target)

    mock_single.assert_called_once_with(params, last_ts)
    mock_add_filename.assert_called_once_with(source_df)
    mock_audit.assert_called_once_with(source_df, scd_key=True)
    source_df.filter.assert_called_once()
    mock_scd.assert_called_once_with(source=source_df, target=target, keys=["k1"], extra_keys=["k2"])
    assert result is True


def test_bronze_pipeline_nodes():
    """Pipeline defines expected bronze current-load nodes."""

    p = pipeline.create_pipeline()
    node_names = {n.name for n in p.nodes}

    expected = {
        "routes_bronze",
        "route_pattern_bronze",
        "stops_bronze",
        "schedules_bronze",
        "trips_bronze",
        "points_bronze",
        "grids_bronze",
    }

    assert node_names == expected
    assert len(p.nodes) == len(expected)
