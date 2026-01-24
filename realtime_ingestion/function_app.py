import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import requests
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobClient


app = func.FunctionApp()

# Managed Identity (System Assigned) will work in Azure.
# Locally, DefaultAzureCredential will use Azure CLI login if you run: az login
credential = DefaultAzureCredential()

class EventHubProducerPool:
    """Manages EventHub producer connections with health checking and auto-reconnect."""
    
    def __init__(self):
        self._producers: Dict[str, EventHubProducerClient] = {}
    
    def get_producer(self, eventhub_name: str) -> EventHubProducerClient:
        """Get or create a producer, with automatic reconnection on failure."""
        if eventhub_name in self._producers:
            return self._producers[eventhub_name]
        
        namespace = get_env("EVENTHUB_NAMESPACE")
        if not namespace:
            raise ValueError("Missing EVENTHUB_NAMESPACE (e.g. xxx.servicebus.windows.net)")
        
        producer = EventHubProducerClient(
            fully_qualified_namespace=namespace,
            eventhub_name=eventhub_name,
            credential=credential
        )
        self._producers[eventhub_name] = producer
        logging.info("Created EventHub producer for '%s'", eventhub_name)
        return producer
    
    def send_events(self, eventhub_name: str, messages: List[str]) -> int:
        """Send events without closing connection. Auto-reconnects on failure."""
        if not messages:
            return 0
        
        try:
            producer = self.get_producer(eventhub_name)
            sent = 0
            batch = producer.create_batch()
            
            for msg in messages:
                ev = EventData(msg)
                try:
                    batch.add(ev)
                    sent += 1
                except ValueError:
                    # batch full -> send it and start new one
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(ev)
                    sent += 1
            
            if len(batch) > 0:
                producer.send_batch(batch)
            
            return sent
        
        except Exception as e:
            logging.error("Failed to send %d events to EventHub '%s': %s. Reconnecting...", len(messages), eventhub_name, str(e))
            # Remove dead connection so next call creates fresh one
            # Don't try to close - it may hang. Just remove from cache and let it GC.
            if eventhub_name in self._producers:
                del self._producers[eventhub_name]
            raise
    
    def close_all(self):
        """Cleanup on app shutdown. Ignore timeout errors during close."""
        for name, producer in self._producers.items():
            try:
                producer.close()
            except Exception:
                # Silently ignore timeout/AMQP errors during close
                # Connection will be cleaned up by garbage collection
                pass
        
        self._producers.clear()
        logging.info("EventHub producers cleanup complete")


# Global producer pool instance
_producer_pool = EventHubProducerPool()


def get_last_run_time() -> Optional[datetime]:
    """
    Retrieve the last successful run timestamp from Blob Storage.
    Returns None if this is the first run.
    """
    try:
        blob_connection_string = get_env("AzureWebJobsStorage")
        if not blob_connection_string:
            logging.warning("AzureWebJobsStorage not configured. Running in full backfill mode.")
            return None
        
        blob_client = BlobClient.from_connection_string(
            blob_connection_string,
            container_name="config",
            blob_name="last_run_time.json"
        )
        
        blob_data = blob_client.download_blob().readall().decode('utf-8')
        data = json.loads(blob_data)
        last_run_iso = data.get("last_run_utc")
        if last_run_iso:
            return datetime.fromisoformat(last_run_iso)
    except Exception as e:
        logging.info("First run or unable to retrieve last run time: %s", str(e))
    
    return None


def save_last_run_time(run_time: datetime) -> bool:
    """
    Save the current run timestamp to Blob Storage.
    Returns True if successful, False otherwise.
    """
    try:
        blob_connection_string = get_env("AzureWebJobsStorage")
        if not blob_connection_string:
            logging.warning("AzureWebJobsStorage not configured. Cannot save run state.")
            return False
        
        blob_client = BlobClient.from_connection_string(
            blob_connection_string,
            container_name="config",
            blob_name="last_run_time.json"
        )
        
        data = {"last_run_utc": run_time.isoformat()}
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        logging.info("Saved run time to blob storage: %s", run_time.isoformat())
        return True
    except Exception as e:
        logging.error("Failed to save run time: %s", str(e))
        return False


def should_include_record(record: Dict[str, Any], cutoff_time: Optional[datetime]) -> bool:
    """
    Determine if a record should be included based on its updated_at or created_at timestamp.
    If cutoff_time is None (full backfill), include all records.
    If timestamps are missing, include the record (safer, no data loss).
    """
    if cutoff_time is None:
        # Full backfill mode - include everything
        return True
    
    attrs = record.get("attributes", {})
    
    # Check updated_at first, then created_at
    timestamp_str = attrs.get("updated_at") or attrs.get("created_at")
    
    if not timestamp_str:
        # No timestamp found - include it to be safe
        logging.debug("Record %s has no timestamp. Including it.", record.get("id"))
        return True
    
    try:
        # Parse ISO 8601 timestamp
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        record_time = datetime.fromisoformat(timestamp_str)
        
        # Include if record was created/updated after cutoff
        return record_time > cutoff_time
    except Exception as e:
        logging.warning("Failed to parse timestamp '%s': %s. Including record to be safe.", timestamp_str, str(e))
        return True


def get_env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name)
    return val if val is not None and val != "" else (default or "")


def mbta_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "mbta-poller-func/1.0"
    }
    api_key = get_env("MBTA_API_KEY")
    if api_key:
        headers["x-api-key"] = api_key
    return headers


def request_get_with_retry(session: requests.Session, url: str, params: Optional[Dict[str, str]], timeout_s: int) -> Dict[str, Any]:
    """
    Simple retry for transient errors (429 / 5xx).
    Keeps it straightforward.
    """
    max_retries = 4
    backoff = 0.7

    for attempt in range(max_retries + 1):
        resp = session.get(url, headers=mbta_headers(), params=params, timeout=timeout_s)

        if resp.status_code < 400:
            return resp.json()

        # retry for transient problems
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else backoff * (2 ** attempt)
            logging.warning("Transient HTTP %s for %s. Retry in %.1fs", resp.status_code, url, sleep_s)
            time.sleep(sleep_s)
            continue

        # non-retryable or out of retries
        resp.raise_for_status()

    raise RuntimeError("Unexpected retry flow")


def fetch_mbta_records(session: requests.Session, endpoint: str, route_filter: Optional[str], page_limit: int, timeout_s: int) -> List[Dict[str, Any]]:
    """
    Fetch all records from MBTA endpoint using the API's JSON:API pagination:
    - uses page[limit] + page[offset]
    - follows links.next (full URL) until it's missing (automatic end detection)
    """
    base_url = get_env("MBTA_BASE_URL", "https://api-v3.mbta.com").rstrip("/")
    url = f"{base_url}/{endpoint.lstrip('/')}"
    params: Dict[str, str] = {"page[limit]": str(page_limit), "page[offset]": "0"}
    if route_filter:
        params["filter[route]"] = route_filter

    all_records: List[Dict[str, Any]] = []
    pages = 0

    while True:
        payload = request_get_with_retry(session, url, params=params, timeout_s=timeout_s)
        pages += 1

        data = payload.get("data") or []
        if isinstance(data, list):
            all_records.extend(data)
        else:
            all_records.append(data)

        next_link = (payload.get("links") or {}).get("next")
        if not next_link:
            break

        # MBTA returns full URL in links.next
        # "https://api-v3.mbta.com/vehicles?page[limit]=1&page[offset]=1"
        url = next_link
        params = None  # next_link already has query params

        # small sleep
        time.sleep(0.10)

    logging.info("Fetched %d records from %s in %d page(s)", len(all_records), endpoint, pages)
    return all_records


def flatten_record(record: Dict[str, Any], endpoint: str) -> Dict[str, Any]:
    """
    Flatten endpoint-specific fields and relationship IDs from the MBTA record.
    Extracts only the fields we care about for each endpoint type, plus foreign keys.
    """
    attrs = record.get("attributes", {})
    relationships = record.get("relationships", {})
    flattened = {}
    
    # Define which fields to extract for each endpoint
    field_mapping = {
        "vehicles": [
            "latitude", "longitude", "bearing", "occupancy_status", 
            "revenue_status", "current_status", "current_stop_sequence", 
            "direction_id", "label", "speed", "updated_at","created_at"
        ],
        "predictions": [
            "arrival_time", "departure_time", "stop_sequence", 
            "direction_id", "revenue_status", "status", "schedule_relationship",
            "updated_at","created_at"
        ],
        "alerts": [
            "effect", "severity", "header", "description", "active_period",
            "service_effect", "cause", "lifecycle", "updated_at","created_at"
        ]
    }
    
    # Define which relationship IDs (foreign keys) to extract for each endpoint
    relationships_mapping = {
        "vehicles": ["trip", "stop", "route"],
        "predictions": ["trip", "stop", "route", "vehicle", "schedule"],
    }
    
    # Extract fields for this endpoint
    fields = field_mapping.get(endpoint, [])
    for field in fields:
        if field in attrs:
            flattened[field] = attrs[field]
    
    # Extract relationship IDs (foreign keys)
    if endpoint == "alerts":
        # Alerts use informed_entity instead of standard relationships
        informed_entity = attrs.get("informed_entity", [])
        if informed_entity:
            route_ids = set()
            stop_ids = set()
            trip_ids = set()
            
            for entity in informed_entity:
                if entity.get("route"):
                    route_ids.add(entity.get("route"))
                if entity.get("stop"):
                    stop_ids.add(entity.get("stop"))
                if entity.get("trip"):
                    trip_ids.add(entity.get("trip"))
            
            if route_ids:
                flattened["route_ids"] = list(route_ids)
            if stop_ids:
                flattened["stop_ids"] = list(stop_ids)
            if trip_ids:
                flattened["trip_ids"] = list(trip_ids)
    else:
        # Standard relationship extraction for vehicles and predictions
        rel_names = relationships_mapping.get(endpoint, [])
        for rel_name in rel_names:
            if rel_name in relationships:
                rel_data = relationships[rel_name].get("data")
                if rel_data:
                    # Handle both single object and array cases
                    if isinstance(rel_data, list):
                        # For array relationships, store as list of IDs
                        flattened[f"{rel_name}_ids"] = [item.get("id") for item in rel_data if item.get("id")]
                    else:
                        # For single object relationships, store as single ID
                        flattened[f"{rel_name}_id"] = rel_data.get("id")
    
    return flattened


@app.function_name(name="mbta_realtime_poll_to_eventhubs")
@app.schedule(schedule="0 * * * * *", arg_name="timer", run_on_startup=True, use_monitor=True)
def mbta_realtime_poll_to_eventhubs(timer: func.TimerRequest) -> None:
    """
    Poll real-time MBTA endpoints and write ONE event per record to Event Hubs.
    Filters records to only include new/updated ones from the last run (incremental mode),
    or all records if MBTA_FULL_BACKFILL is enabled.
    
    Schedule: Every minute (0 * * * * *)
    """
    # Parse route filters from environment (JSON array of strings)
    route_filters = json.loads(get_env("MBTA_FILTER_ROUTE", "[]"))
    
    # If no routes configured, use Red as default
    if not route_filters:
        route_filters = ["Red"]
    
    page_limit = int(get_env("MBTA_PAGE_LIMIT", "100"))
    timeout_s = int(get_env("MBTA_HTTP_TIMEOUT_SECONDS", "20"))
    full_backfill = get_env("MBTA_FULL_BACKFILL", "false").lower() == "true"
    
    # Determine cutoff time for filtering
    last_run_time = None
    if not full_backfill:
        last_run_time = get_last_run_time()
        if last_run_time:
            logging.info("Incremental mode: filtering records modified after %s", last_run_time.isoformat())
        else:
            logging.info("First run or no state found. Running in full backfill mode.")
    else:
        logging.info("Full backfill mode enabled. Processing all records.")
    
    # Current run time for state tracking
    current_run_time = datetime.now(timezone.utc)

    # endpoint -> hub name mapping (configurable)
    endpoints_to_hubs = {
        "vehicles": get_env("EVENTHUB_VEHICLES_NAME", "vehicles"),
        "predictions": get_env("EVENTHUB_PREDICTIONS_NAME", "predictions"),
        "alerts": get_env("EVENTHUB_ALERTS_NAME", "alerts"),
    }

    polled_at = current_run_time.isoformat()
    logging.info("MBTA poll start=%s, routes=%s, backfill=%s", polled_at, route_filters, full_backfill)

    with requests.Session() as session:
        total_sent = 0
        total_filtered = 0

        for endpoint, hub_name in endpoints_to_hubs.items():
            # Only loop through route filters for predictions; alerts and vehicles fetch all without filter
            route_filters_to_use = route_filters if endpoint == "predictions" else [None]
            
            for route_filter in route_filters_to_use:
                try:
                    records = fetch_mbta_records(
                        session=session,
                        endpoint=endpoint,
                        route_filter=route_filter,
                        page_limit=page_limit,
                        timeout_s=timeout_s
                    )

                    # Filter records based on timestamp (if incremental mode)
                    filtered_records = [r for r in records if should_include_record(r, last_run_time)]
                    total_filtered += len(records) - len(filtered_records)

                    # one message per filtered record (Kafka-like)
                    messages: List[str] = []
                    for r in filtered_records:
                        flattened = flatten_record(r, endpoint)
                        envelope = {
                            "source": "mbta-v3",
                            "endpoint": f"/{endpoint}",
                            "record_type": r.get("type"),
                            "record_id": r.get("id"),
                            "filter_route": route_filter,
                            "polled_at_utc": polled_at,
                            **flattened  # Spread flattened fields
                        }
                        messages.append(json.dumps(envelope))

                    sent = _producer_pool.send_events(hub_name, messages)
                    total_sent += sent
                    if route_filter:
                        logging.info("Sent %d/%d events to Event Hub '%s' from endpoint '%s' for route '%s'", sent, len(filtered_records), hub_name, endpoint, route_filter)
                    else:
                        logging.info("Sent %d/%d events to Event Hub '%s' from endpoint '%s' (no filter)", sent, len(filtered_records), hub_name, endpoint)

                except Exception as e:
                    logging.exception("Failed processing endpoint %s (route_filter=%s): %s", endpoint, route_filter, str(e))

        logging.info("MBTA poll done. Total events sent=%d, filtered out=%d", total_sent, total_filtered)
        
        # Save current run time for next incremental poll
        save_last_run_time(current_run_time)
