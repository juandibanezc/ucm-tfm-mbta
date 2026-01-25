"""Bronze pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.bronze.nodes.mbta_ingestion import (
    ingest_current_load as ingest_dimensions_current_load,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the bronze pipeline"""
    return pipeline(
        [
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "params:catalog_info_bronze_routes",
                    "landing_last_execution@json",
                    "bronze_routes@delta",
                ],
                outputs="bronze_routes_true",
                name="routes_bronze",
                tags=["bronze", "mbta"],
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "params:catalog_info_bronze_route_patterns",
                    "landing_last_execution@json",
                    "bronze_route_patterns@delta",
                ],
                outputs="bronze_route_patterns_true",
                name="route_pattern_bronze",
                tags=["bronze", "mbta"],
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "params:catalog_info_bronze_stops",
                    "landing_last_execution@json",
                    "bronze_stops@delta",
                ],
                outputs="bronze_stops_true",
                name="stops_bronze",
                tags=["bronze", "mbta"],
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "params:catalog_info_bronze_schedules",
                    "landing_last_execution@json",
                    "bronze_schedules@delta",
                ],
                outputs="bronze_schedules_true",
                name="schedules_bronze",
                tags=["bronze", "mbta"],
            ),
            node(
                func=ingest_dimensions_current_load,
                inputs=[
                    "params:catalog_info_bronze_trips",
                    "landing_last_execution@json",
                    "bronze_trips@delta",
                ],
                outputs="bronze_trips_true",
                name="trips_bronze",
                tags=["bronze", "mbta"],
            ),
        ]
    )
