"""Module to create nodes for MBTA API extraction."""
import requests as rq

from datetime import datetime
from typing import Any, Dict
from pathlib import PurePosixPath

from processing_datalake.hooks import (
    get_credentials,
    get_catalog_dataset,
)


def last_ts() -> Dict[str, Any]:
    """Define last_ts"""
    return {
        "last_ts": datetime.now().strftime("%Y%m%d%H%M%S")
    }


def extract_mbta_endpoint(
    params: Dict[str, Any],
    last_exec: Dict[str, str],
) -> bool:
    """Extract data from MBTA API endpoint and save to landing zone.

    Args:
        params (Dict[str, Any]): Parameters for processing.
        last_exec (Dict[str, str]): Last execution timestamp.
    Returns:
        bool: Return success status.
    """
    url = params.get("url")
    credentials_name = params.get("credentials_name")
    extractions = params.get("extractions")

    mbta_api_credentials = get_credentials(credentials_name)
    api_key = mbta_api_credentials.get("api_key")

    headers = {
        "x-api-key": api_key,
        "Accept": "application/json"
    }

    last_timestamp = last_exec.get("last_ts")

    year = last_timestamp[:4]
    month = last_timestamp[4:6]
    day = last_timestamp[6:8]

    for endpoint_config in extractions:
        endpoint = endpoint_config.get("endpoint")

        url_get = url.format(endpoint=endpoint)

        try:
            response = rq.get(url_get, headers=headers)

            response.raise_for_status()
            data = response.json()

            dataset_name = endpoint_config.get("catalog_dataset")
            table_catalog = get_catalog_dataset(dataset_name)

            file_path = str(table_catalog._filepath).format(
                last_ts=last_timestamp,
                year=year,
                month=month,
                day=day,
            )

            file_path_formatted = PurePosixPath(file_path)
            table_catalog._filepath = file_path_formatted

            table_catalog.save(data)

        except Exception as e:
            print(f"Error fetching data from {endpoint}: {e}")
            continue

    return True
