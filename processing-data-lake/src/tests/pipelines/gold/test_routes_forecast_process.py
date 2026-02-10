"""Tests for gold current-load routes forecast processing."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.gold.nodes import routes_forecast_process


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.filter.return_value = df
    return df


def test_process_route_forecast_metrics_filters_and_writes_incremental():
    """process_route_forecast_metrics builds table, filters by save_date, and calls incremental_load."""

    params = {"catalog_dataset": "gold_routes"}
    last_ts = {"last_ts": "20240102030405"}

    forecast_df = _df_mock("routes_forecast")
    with (
        patch.object(routes_forecast_process, "process_routes_forecast", return_value=forecast_df) as mock_process,  # noqa: E501
        patch.object(routes_forecast_process, "incremental_load") as mock_incremental,
    ):

        result = routes_forecast_process.process_route_forecast_metrics(
            MagicMock(name="grids"),
            MagicMock(name="points"),
            MagicMock(name="schedules"),
            last_ts,
            params,
        )

    mock_process.assert_called_once()
    forecast_df.filter.assert_called_once()
    mock_incremental.assert_called_once()
    assert result is True
