"""Module for extracting data from the NWS API."""
import logging

from typing import Any, Dict, List, Union
from pathlib import PurePosixPath

import asyncio
from asyncio import Semaphore
from aiohttp import ClientSession

from processing_datalake.hooks import (
    get_catalog_dataset,
)

logger = logging.getLogger(__name__)


async def get_endpoint_data(
    metadata: Dict[str, Any],
    headers: Dict[str, str],
    session: ClientSession,
    semaphore: Semaphore,
) -> Union[Dict[str, str], bool]:
    """Asynchronously fetch data from a specific NWS API endpoint.

    Args:
        metadata (Dict[str, Any]): Metadata for the API request.
        headers (Dict[str, str]): Headers for the API request.
        session (ClientSession): The aiohttp client session.
        semaphore (Semaphore): Semaphore to limit concurrent requests.

    Returns:
        Union[Dict[str, Any], bool]: The success status of the API request.
    """

    timestamp = metadata.get("timestamp")
    target_catalog = metadata.get("catalog_dataset")
    url_get = metadata.get("url")
    filter_id = metadata.get("id")
    is_forecast = metadata.get("is_forecast", False)

    year = timestamp[:4]
    month = timestamp[4:6]
    day = timestamp[6:8]

    table_catalog = get_catalog_dataset(target_catalog)

    file_path = str(table_catalog._filepath).format(
        last_ts=timestamp,
        year=year,
        month=month,
        day=day,
        id=filter_id,
    )

    file_path_formatted = PurePosixPath(file_path)
    table_catalog._filepath = file_path_formatted

    async with semaphore:
        async with session.get(url_get, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            data["stop_id"] = filter_id
            table_catalog.save(data)

            if is_forecast:
                return True

            returned_data = {
                "stop_id": filter_id,
                "forecast_endpoint": data["properties"]["forecast"],
            }

            return returned_data


async def extract_nws_api_async(
    headers: Dict[str, str],
    metadata_list: List[Dict[str, Any]],
    is_forecast: bool = False,
) -> Union[bool, List[Dict[str, str]]]:
    """Asynchronously extract data from NWS API endpoints.
    Args:
        url (str): Base URL for the NWS API.
        headers (Dict[str, str]): Headers for the API requests.
        metadata_list (List[Dict[str, Any]]): List of metadata for each API request.
    Returns:
        Union[bool, List[Dict[str, str]]]: Return success status or list of data.
    """

    semaphore = Semaphore(20)

    async with ClientSession() as session:

        tasks = [
            get_endpoint_data(
                metadata,
                headers,
                session,
                semaphore,
            )
            for metadata in metadata_list
        ]

        results = await asyncio.gather(*tasks)

        if is_forecast:
            return all(results)

        return results


def extract_points_api(
    last_exec: Dict[str, str],
    params: Dict[str, Any],
    *_: Any,
) -> Dict[str, Any]:
    """Extract data from NWS API endpoint and save to landing zone.

    Args:
        last_exec (Dict[str, str]): Last execution timestamp.
        params (Dict[str, Any]): Parameters for processing.
    Returns:
        Dict[str, Any]: Extracted data and last execution timestamp.
    """

    base_table = params.get("base_table")
    target_catalog = params.get("target_catalog")
    endpoint = params.get("endpoint")
    url = params.get("url")

    headers = {
        "User-Agent": "(NWS_TFM_PROJECT, juandibanezc@outlook.com)",
        "Accept": "application/json"
    }

    ids_dataset = get_catalog_dataset(base_table)
    last_ts = last_exec.get("last_ts")

    year = last_ts[:4]
    month = last_ts[4:6]
    day = last_ts[6:8]

    file_path = str(ids_dataset._filepath).format(
        last_ts=last_ts,
        year=year,
        month=month,
        day=day,
    )

    file_path_formatted = PurePosixPath(file_path)
    ids_dataset._filepath = file_path_formatted

    dataset_id = ids_dataset.load()

    logger.info("Building metadata list for asynchronous extraction.")

    data = dataset_id.get("data")

    points = [
        (stop["id"], stop["attributes"]["latitude"], stop["attributes"]["longitude"])
        for stop in data
    ]

    metadata_list = [
        {
            "url": url.format(
                endpoint=endpoint,
                latitude=latitude,
                longitude=longitude,
            ),
            "id": stop_id,
            "timestamp": last_ts,
            "catalog_dataset": target_catalog,
            "is_forecast": False,
        } for stop_id, latitude, longitude in points
    ]

    results = asyncio.run(
        extract_nws_api_async(
            headers,
            metadata_list,
            is_forecast=False,
        )
    )

    data = {
        "data": results,
        "last_ts": last_ts,
    }

    return data


def extract_forecast_api(
    points: Dict[str, str],
    params: Dict[str, str],
    *_: Any,
) -> bool:
    """Extract forecast data from NWS API endpoint and save to landing zone.

    Args:
        Points (Dict[str, str]): Data from previous extraction.
        params (Dict[str, str]): Parameters for processing.
    Returns:
        bool: Success status of the extraction.
    """

    target_catalog = params.get("target_catalog")
    headers = {
        "User-Agent": "(NWS_TFM_PROJECT, juandibanezc@outlook.com)",
        "Accept": "application/json"
    }

    metadata_list = [
        {
            "url": point["data"]["forecast_endpoint"],
            "id": point["data"]["id"],
            "timestamp": points["last_ts"],
            "catalog_dataset": target_catalog,
            "is_forecast": True,
        } for point in points
    ]

    results = asyncio.run(
        extract_nws_api_async(
            headers,
            metadata_list,
            is_forecast=True,
        )
    )

    return results
