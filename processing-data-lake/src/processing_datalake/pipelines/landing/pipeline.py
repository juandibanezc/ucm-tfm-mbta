"""Landing pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.landing.nodes.extract_mbta_api import (
    last_ts,
    extract_mbta_endpoint,
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
            )
        ]
    )
