"""Module to create nodes for MBTA API extraction."""
import requests as rq
import logging

from datetime import datetime
from typing import Any, Dict, List
from pathlib import PurePosixPath

import asyncio
from asyncio import Semaphore
from aiohttp import ClientSession

from processing_datalake.hooks import (
    get_credentials,
    get_catalog_dataset,
)

logger = logging.getLogger(__name__)


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


async def get_endpoint_data(
    metadata: Dict[str, Any],
    headers: Dict[str, str],
    session: ClientSession,
    semaphore: Semaphore,
) -> bool:
    """Asynchronously fetch data from a specific MBTA API endpoint.

    Args:
        metadata (Dict[str, Any]): Metadata for the API request.
        headers (Dict[str, str]): Headers for the API request.
        session (ClientSession): The aiohttp client session.
        semaphore (Semaphore): Semaphore to limit concurrent requests.

    Returns:
        bool: The success status of the API request.
    """

    timestamp = metadata.get("timestamp")
    target_catalog = metadata.get("catalog_dataset")
    url_get = metadata.get("url")
    filter_id = metadata.get("id")

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
            table_catalog.save(await response.json())

            return True


async def extract_mbta_endpoint_async(
    headers: Dict[str, str],
    metadata_list: List[Dict[str, Any]],
) -> bool:
    """Asynchronously extract data from MBTA API endpoints.
    Args:
        url (str): Base URL for the MBTA API.
        headers (Dict[str, str]): Headers for the API requests.
        metadata_list (List[Dict[str, Any]]): List of metadata for each API request.
    Returns:
        bool: Return success status.
    """

    semaphore = Semaphore(10)

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

        return all(results)


def extract_mbta_filter_endpoints(
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

    base_table = params.get("base_table")
    target_catalog = params.get("target_catalog")
    filter = params.get("filter")
    endpoint = params.get("endpoint")
    url = params.get("url")

    credentials_name = params.get("credentials_name")

    mbta_api_credentials = get_credentials(credentials_name)
    api_key = mbta_api_credentials.get("api_key")

    headers = {
        "x-api-key": api_key,
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

    metadata_list = [
        {
            "url": url.format(
                endpoint=endpoint,
                filter=filter,
                id=data['id'],
            ),
            "id": data['id'],
            "timestamp": last_ts,
            "catalog_dataset": target_catalog,
        } for data in dataset_id['data']
    ]

    logger.info("Starting asynchronous extraction from MBTA API.")

    asyncio.run(extract_mbta_endpoint_async(
        headers,
        metadata_list,
    ))

    logger.info("Asynchronous extraction completed.")

    return True
