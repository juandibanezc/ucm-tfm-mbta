"""Tests for gold current-load trips metrics processing."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.gold import pipeline
from processing_datalake.pipelines.gold.nodes import trips_metrics_process


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.filter.return_value = df
    return df


def test_process_trips_metrics_filters_and_writes_incremental():
    """process_trips_metrics builds table, filters by save_date, and calls incremental_load."""

    params = {"catalog_dataset": "gold_trips"}
    last_ts = {"last_ts": "20240102030405"}

    trips_df = _df_mock("trips_metrics")
    with (
        patch.object(trips_metrics_process, "create_trips_metrics_table", return_value=trips_df) as mock_create,  # noqa: E501
        patch.object(trips_metrics_process, "incremental_load") as mock_incremental,
    ):

        result = trips_metrics_process.process_trips_metrics(
            MagicMock(name="schedules"),
            MagicMock(name="routes"),
            MagicMock(name="trips"),
            MagicMock(name="route_patterns"),
            last_ts,
            params,
        )

    mock_create.assert_called_once()
    trips_df.filter.assert_called_once()
    mock_incremental.assert_called_once()
    assert result is True


def test_gold_pipeline_nodes():
    """Pipeline defines expected gold current-load nodes."""

    p = pipeline.create_pipeline()
    node_names = {n.name for n in p.nodes}

    expected = {
        "gold_trips_metrics_transformation_node",
        "gold_routes_forecast_transformation_node",
    }

    assert node_names == expected
    assert len(p.nodes) == len(expected)
