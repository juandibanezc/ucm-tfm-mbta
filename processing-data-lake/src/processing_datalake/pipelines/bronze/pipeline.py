"""Bronze pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.bronze.nodes.data_ingestion import (
    ingest_current_load as ingest_dimensions_current_load,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the bronze pipeline"""
    return pipeline(
        [
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "landing_routes@spark",
                    "params:catalog_info_bronze_routes",
                    "bronze_routes@delta",
                ],
                outputs="bronze_routes_true",
                name="routes_bronze",
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "landing_route_patterns@spark",
                    "params:catalog_info_bronze_route_patterns",
                    "bronze_route_patterns@delta",
                ],
                outputs="bronze_route_patterns_true",
                name="route_pattern_bronze",
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "landing_stops@spark",
                    "params:catalog_info_bronze_stops",
                    "bronze_stops@delta",
                ],
                outputs="bronze_stops_true",
                name="stops_bronze",
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "landing_schedules@spark",
                    "params:catalog_info_bronze_schedules",
                    "bronze_schedules@delta",
                ],
                outputs="bronze_schedules_true",
                name="schedules_bronze",
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "landing_trips@spark",
                    "params:catalog_info_bronze_trips",
                    "bronze_trips@delta",
                ],
                outputs="bronze_trips_true",
                name="trips_bronze",
            ),
        ]
    )
