"""Tests for extract_nws_api node."""
from unittest.mock import patch, MagicMock
from processing_datalake.pipelines.landing.nodes.extract_nws_api import (
    extract_points_api,
    extract_forecast_api,
)


@patch("processing_datalake.pipelines.landing.nodes.extract_nws_api.extract_nws_api_async")
@patch("processing_datalake.pipelines.landing.nodes.extract_nws_api.get_catalog_dataset")
def test_extract_points_api_success(mock_get_catalog, mock_async_extractor):
    """Test successful extraction from NWS points API using sync wrapper."""

    mock_async_extractor.return_value = [
        {"stop_id": "1", "forecast_endpoint": "http://forecast.com"}
    ]

    mock_catalog = MagicMock()
    mock_catalog._filepath = "/tmp/{last_ts}/{year}/{month}/{day}/stops.json"
    mock_catalog.load.return_value = {
        "data": [
            {"id": "1", "attributes": {"latitude": 42.0, "longitude": -71.0}}
        ]
    }
    mock_get_catalog.return_value = mock_catalog

    params = {
        "url": "http://test.com/{endpoint}?lat={latitude}&lon={longitude}",
        "endpoint": "points",
        "target_catalog": "test_dataset",
        "base_table": "stops_table",
    }
    last_exec = {"last_ts": "20230101000000"}

    result = extract_points_api(last_exec, params)

    assert isinstance(result, dict)
    assert "data" in result
    assert result["data"][0]["forecast_endpoint"] == "http://forecast.com"
    mock_async_extractor.assert_called_once()
    mock_get_catalog.assert_called_once_with("stops_table")


@patch("processing_datalake.pipelines.landing.nodes.extract_nws_api.extract_nws_api_async")
def test_extract_forecast_api_success(mock_async_extractor):
    """Test successful extraction from NWS forecast API."""

    mock_async_extractor.return_value = True

    params = {
        "target_catalog": "test_dataset",
        "max_retries": 3,
        "max_concurrency": 10,
    }
    points_dict = {
        "data": [{"stop_id": "1", "forecast_endpoint": "http://forecast.com/grid/1"}],
        "last_ts": "20230101000000",
    }

    result = extract_forecast_api(points_dict, params)

    assert result is True
    mock_async_extractor.assert_called_once()
