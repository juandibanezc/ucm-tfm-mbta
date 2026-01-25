"""Bronze pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline

from processing_datalake.pipelines.bronze_first_load.nodes.mbta_ingestion import (
    process_table as ingest_dimensions,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the bronze pipeline"""
    return pipeline(
        [
            node(
                func=ingest_dimensions,
                inputs=[
                    "params:catalog_info_bronze_routes",
                    "landing_last_execution@json",
                ],
                outputs="bronze_routes@spark",
                name="routes_bronze_first_load_node",
                tags=["bronze", "first_load", "mbta"],
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "params:catalog_info_bronze_route_patterns",
                    "landing_last_execution@json",
                ],
                outputs="bronze_route_patterns@spark",
                name="route_pattern_bronze_first_load_node",
                tags=["bronze", "first_load", "mbta"],
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "params:catalog_info_bronze_stops",
                    "landing_last_execution@json",
                ],
                outputs="bronze_stops@spark",
                name="stop_bronze_first_load_node",
                tags=["bronze", "first_load", "mbta"],
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "params:catalog_info_bronze_schedules",
                    "landing_last_execution@json",
                ],
                outputs="bronze_schedules@spark",
                name="schedules_bronze_first_load_node",
                tags=["bronze", "first_load", "mbta"],
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "params:catalog_info_bronze_trips",
                    "landing_last_execution@json",
                ],
                outputs="bronze_trips@spark",
                name="trips_bronze_first_load_node",
                tags=["bronze", "first_load", "mbta"],
            ),
        ]
    )
