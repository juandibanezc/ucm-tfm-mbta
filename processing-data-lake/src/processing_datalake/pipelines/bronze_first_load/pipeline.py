"""Bronze pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline

from processing_datalake.pipelines.bronze_first_load.nodes.data_ingestion import (
    process_table as ingest_dimensions,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the bronze pipeline"""
    return pipeline(
        [
            node(
                func=ingest_dimensions,
                inputs=[
                    "landing_routes@spark",
                    "params:catalog_info_bronze_routes",
                ],
                outputs="bronze_routes@spark",
                name="routes_bronze_first_load_node",
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "landing_route_patterns@spark",
                    "params:catalog_info_bronze_route_patterns",
                ],
                outputs="bronze_route_patterns@spark",
                name="route_pattern_bronze_first_load_node",
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "landing_stops@spark",
                    "params:catalog_info_bronze_stops",
                ],
                outputs="bronze_stops@spark",
                name="stop_bronze_first_load_node",
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "landing_schedules@spark",
                    "params:catalog_info_bronze_schedules",
                ],
                outputs="bronze_schedules@spark",
                name="schedules_bronze_first_load_node",
            ),
            node(
                func=ingest_dimensions,
                inputs=[
                    "landing_trips@spark",
                    "params:catalog_info_bronze_trips",
                ],
                outputs="bronze_trips@spark",
                name="trips_bronze_first_load_node",
            ),
        ]
    )
