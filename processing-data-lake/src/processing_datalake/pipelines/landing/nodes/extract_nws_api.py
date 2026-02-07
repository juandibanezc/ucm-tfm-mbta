"""Module for extracting data from the NWS API."""
import logging

from typing import Any, Dict, List, Union
from pathlib import PurePosixPath

import asyncio
from asyncio import Semaphore

import aiohttp
from aiohttp import ClientSession

from processing_datalake.hooks import (
    get_catalog_dataset,
)

logger = logging.getLogger(__name__)


async def get_endpoint_data(
    metadata: Dict[str, Any],
    session: ClientSession,
    semaphore: Semaphore,
    max_retries: int = 3,
) -> Union[Dict[str, str], bool, None]:
    """Asynchronously fetch data from a specific NWS API endpoint.

    Args:
        metadata (Dict[str, Any]): Metadata for the API request.
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

    if is_forecast:
        key = "grid_id"

    key = "stop_id_mbta"

    file_path_formatted = PurePosixPath(file_path)
    table_catalog._filepath = file_path_formatted

    async with semaphore:
        for attempt in range(max_retries + 1):
            try:
                # Individual request timeout
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                async with session.get(url_get, timeout=timeout) as response:
                    response.raise_for_status()
                    data = await response.json()
                    data[key] = filter_id
                    table_catalog.save(data)

                    if is_forecast:
                        return True

                    returned_data = {
                        "stop_id": filter_id,
                        "forecast_endpoint": data["properties"]["forecast"],
                    }

                    return returned_data

            except Exception as e:
                logger.warning(
                    "Attempt %d failed for ID %s: %s",
                    attempt + 1, filter_id, str(e)
                )
                if attempt == max_retries:
                    logger.error("Max retries exceeded for ID: %s", filter_id)
                    return None
                # Wait before retry (exponential backoff)
                await asyncio.sleep(2 ** attempt)


async def process_chunk(
    chunk: List[Dict[str, Any]],
    session: ClientSession,
    semaphore: Semaphore,
) -> List[Union[Dict[str, str], bool, None]]:
    """Process a chunk of metadata with controlled concurrency."""
    tasks = [
        get_endpoint_data(
            metadata,
            session,
            semaphore,
        )
        for metadata in chunk
    ]

    return await asyncio.gather(*tasks, return_exceptions=True)


async def extract_nws_api_async(
    metadata_list: List[Dict[str, Any]],
    is_forecast: bool = False,
    chunk_size: int = 50,
    max_concurrent: int = 5,
    delay_between_chunks: float = 1.0,
) -> Union[bool, List[Dict[str, str]]]:
    """Asynchronously extract data from NWS API endpoints with chunking and rate limiting.
    Args:
        metadata_list (List[Dict[str, Any]]): List of metadata for each API request.
        is_forecast (bool): Whether this is forecast data extraction.
        chunk_size (int): Number of requests to process in each chunk.
        max_concurrent (int): Maximum concurrent requests per chunk.
    Returns:
        Union[bool, List[Dict[str, str]]]: Return success status or list of data.
    """

    # Reduced concurrency to be respectful to the API
    semaphore = Semaphore(max_concurrent)

    # Session timeout configuration
    timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_read=30)

    async with ClientSession(timeout=timeout) as session:
        all_results = []

        # Process in chunks to avoid overwhelming the API
        for i in range(0, len(metadata_list), chunk_size):
            chunk = metadata_list[i:i + chunk_size]
            logger.info(
                "Processing chunk %d/%d (%d requests)",
                i // chunk_size + 1,
                (len(metadata_list) + chunk_size - 1) // chunk_size,
                len(chunk)
            )

            chunk_results = await process_chunk(
                chunk, session, semaphore
            )

            # Filter out failed requests and exceptions
            valid_results = [
                result for result in chunk_results
                if result is not None and not isinstance(result, Exception)
            ]

            all_results.extend(valid_results)

            # Add delay between chunks to be respectful to the API
            if i + chunk_size < len(metadata_list):
                await asyncio.sleep(delay_between_chunks)

        logger.info(
            "Completed processing. Successful requests: %d/%d",
            len(all_results), len(metadata_list)
        )

        if is_forecast:
            return len(all_results) > 0

        return all_results


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

    data_cleaned = [
        stop for stop in data
        if stop["attributes"]["latitude"] is not None and stop["attributes"]["longitude"] is not None
    ]

    points = [
        (stop["id"], stop["attributes"]["latitude"], stop["attributes"]["longitude"])
        for stop in data_cleaned
    ]

    logger.info("Points extracted from dataset. Number of points: %d", len(points))

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

    logger.info("Starting asynchronous extraction from NWS API.")

    # Get performance configuration parameters
    chunk_size = params.get("chunk_size", 50)
    max_concurrent = params.get("max_concurrent", 5)
    delay_between_chunks = params.get("delay_between_chunks", 1.0)

    results = asyncio.run(
        extract_nws_api_async(
            metadata_list,
            is_forecast=False,
            chunk_size=chunk_size,
            max_concurrent=max_concurrent,
            delay_between_chunks=delay_between_chunks,
        )
    )

    logger.info("Asynchronous extraction completed.")

    data = {
        "data": results,
        "last_ts": last_ts,
    }

    return data


def extract_forecast_api(
    points: Dict[str, str],
    params: Dict[str, str],
) -> bool:
    """Extract forecast data from NWS API endpoint and save to landing zone.

    Args:
        Points (Dict[str, str]): Data from previous extraction.
        params (Dict[str, str]): Parameters for processing.
    Returns:
        bool: Success status of the extraction.
    """

    target_catalog = params.get("target_catalog")

    # To avoid overwhelming the API, we will process only unique forecast endpoints
    forecast_to_stops = {}

    result = [
        forecast_to_stops.setdefault(d["forecast_endpoint"], []).append(d["stop_id"])
        for d in points.get("data")
    ]

    if not all(result):
        logger.info(
            "All points have valid forecast endpoints. Number of unique endpoints: %d",
            len(forecast_to_stops)
        )

    metadata_list = [
        {
            "url": grid,
            "id": grid.split("/")[-2].replace(",", "_"),
            "timestamp": points["last_ts"],
            "catalog_dataset": target_catalog,
            "is_forecast": True,
        } for grid in forecast_to_stops.keys()
    ]

    results = asyncio.run(
        extract_nws_api_async(
            metadata_list,
            is_forecast=True,
            chunk_size=params.get("chunk_size", 50),
            max_concurrent=params.get("max_concurrent", 5),
            delay_between_chunks=params.get("delay_between_chunks", 1.0),
        )
    )

    return results
