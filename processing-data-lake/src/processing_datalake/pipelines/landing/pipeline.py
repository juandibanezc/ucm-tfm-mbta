"""Landing pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.landing.nodes.extract_mbta_api import (
    last_ts
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
        ]
    )
