"""Gold pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.gold.nodes.trips_metrics_process import (
    process_trips_metrics,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the gold pipeline"""
    return pipeline(
        [
            node(
                func=process_trips_metrics,
                inputs=[
                    "silver_schedules@spark",
                    "silver_routes@spark",
                    "silver_trips@spark",
                    "silver_route_patterns@spark",
                    "params:catalog_info_trips_metrics",
                ],
                outputs="gold_trips_metrics_true",
                name="gold_data_transformation_node",
            )
        ]
    )
