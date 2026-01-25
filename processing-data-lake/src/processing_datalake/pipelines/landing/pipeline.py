"""Landing pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.landing.nodes.extract_mbta_api import (
    last_ts,
    extract_mbta_endpoint,
    extract_mbta_filter_endpoints,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the landing pipeline"""
    return pipeline(
        [
            node(
                func=last_ts,
                inputs=None,
                outputs="landing_last_execution@json",
                name="extract_last_timestamp",
                tags=["landing", "mbta"],
            ),
            node(
                func=extract_mbta_endpoint,
                inputs=[
                    "params:catalog_info_landing_one_endpoint",
                    "landing_last_execution@json",
                ],
                outputs="endpoints_extraction_true",
                name="extract_mbta_endpoints",
                tags=["landing", "mbta"],
            ),
            node(
                func=extract_mbta_filter_endpoints,
                inputs=[
                    "params:catalog_info_landing_schedules",
                    "landing_last_execution@json",
                ],
                outputs="schedules_extraction_true",
                name="extract_mbta_schedules",
                tags=["landing", "mbta", "filter_endpoint"],
            ),
            node(
                func=extract_mbta_filter_endpoints,
                inputs=[
                    "params:catalog_info_landing_trips",
                    "landing_last_execution@json",
                ],
                outputs="trips_extraction_true",
                name="extract_mbta_trips",
                tags=["landing", "mbta", "filter_endpoint"],
            ),
        ]
    )
