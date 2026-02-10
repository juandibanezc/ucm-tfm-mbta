"""Tests for extract_mbta_api node."""
from unittest.mock import patch, MagicMock
from datetime import datetime
from processing_datalake.pipelines.landing.nodes.extract_mbta_api import (
    last_ts,
    extract_mbta_endpoint,
)


def test_last_ts_return_type():
    """Test that last_ts returns a dictionary."""
    assert isinstance(last_ts(), dict)


def test_last_ts_has_correct_key():
    """Test that last_ts dictionary has the correct key."""
    assert "last_ts" in last_ts()


def test_last_ts_value_is_string():
    """Test that the value of last_ts is a string."""
    assert isinstance(last_ts()["last_ts"], str)


def test_last_ts_value_format():
    """Test that the value of last_ts has the correct format."""
    ts = last_ts()["last_ts"]
    assert datetime.strptime(ts, "%Y%m%d%H%M%S")


@patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_credentials")
@patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_catalog_dataset")
@patch("requests.get")
def test_extract_mbta_endpoint_success(mock_get, mock_get_catalog, mock_get_credentials, mock_response):
    """Test successful extraction from MBTA endpoint."""
    mock_get.return_value = mock_response({"data": "test"}, 200)
    mock_get_credentials.return_value = {"api_key": "test_key"}
    mock_catalog = MagicMock()
    mock_get_catalog.return_value = mock_catalog

    params = {
        "url": "http://test.com/{endpoint}",
        "credentials_name": "test_credentials",
        "extractions": [{"endpoint": "test_endpoint", "catalog_dataset": "test_dataset"}],
    }
    last_exec = {"last_ts": "20230101000000"}

    assert extract_mbta_endpoint(params, last_exec) is True
    mock_get.assert_called_once()
    mock_catalog.save.assert_called_once_with({"data": "test"})


@patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_credentials")
@patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_catalog_dataset")
@patch("requests.get")
def test_extract_mbta_endpoint_api_error(mock_get, mock_get_catalog, mock_get_credentials, mock_response):
    """Test API error during extraction from MBTA endpoint."""
    mock_get.return_value = mock_response(None, 404)
    mock_get_credentials.return_value = {"api_key": "test_key"}

    params = {
        "url": "http://test.com/{endpoint}",
        "credentials_name": "test_credentials",
        "extractions": [{"endpoint": "test_endpoint", "catalog_dataset": "test_dataset"}],
    }
    last_exec = {"last_ts": "20230101000000"}

    assert extract_mbta_endpoint(params, last_exec) is True
    mock_get.assert_called_once()
    mock_get_catalog.assert_not_called()


@patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_credentials")
def test_extract_mbta_endpoint_calls_get_credentials(mock_get_credentials):
    """Test that get_credentials is called with the correct name."""
    with patch("requests.get"), patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_catalog_dataset"):  # noqa: E501
        params = {
            "url": "http://test.com/{endpoint}",
            "credentials_name": "test_credentials",
            "extractions": [],
        }
        last_exec = {"last_ts": "20230101000000"}
        extract_mbta_endpoint(params, last_exec)
        mock_get_credentials.assert_called_with("test_credentials")


@patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_catalog_dataset")
def test_extract_mbta_endpoint_calls_get_catalog_dataset(mock_get_catalog):
    """Test that get_catalog_dataset is called for each extraction."""
    with patch("requests.get"), patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_credentials"):  # noqa: E501
        params = {
            "url": "http://test.com/{endpoint}",
            "credentials_name": "test_credentials",
            "extractions": [{"endpoint": "test_endpoint", "catalog_dataset": "test_dataset"}],
        }
        last_exec = {"last_ts": "20230101000000"}
        extract_mbta_endpoint(params, last_exec)
        mock_get_catalog.assert_called_with("test_dataset")


def test_extract_mbta_endpoint_empty_extractions():
    """Test behavior with empty extractions list."""
    params = {
        "url": "http://test.com/{endpoint}",
        "credentials_name": "test_credentials",
        "extractions": [],
    }
    last_exec = {"last_ts": "20230101000000"}
    with patch("requests.get") as mock_get, patch("processing_datalake.pipelines.landing.nodes.extract_mbta_api.get_credentials"):  # noqa: E501
        assert extract_mbta_endpoint(params, last_exec) is True
        mock_get.assert_not_called()
