"""Silver pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline

from processing_datalake.pipelines.silver_first_load.nodes.clean_mbta_tables import (
    clean_tables as cleaning_mbta_tables,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the silver pipeline"""
    return pipeline(
        [
            node(
                func=cleaning_mbta_tables,
                inputs=[
                    "bronze_schedules@spark",
                    "params:catalog_info_silver_schedules"
                ],
                outputs="silver_schedules@spark",
                name="silver_schedules_first_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=cleaning_mbta_tables,
                inputs=[
                    "bronze_trips@spark",
                    "params:catalog_info_silver_trips"
                ],
                outputs="silver_trips@spark",
                name="silver_trips_first_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=cleaning_mbta_tables,
                inputs=[
                    "bronze_stops@spark",
                    "params:catalog_info_silver_stops"
                ],
                outputs="silver_stops@spark",
                name="silver_stops_first_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=cleaning_mbta_tables,
                inputs=[
                    "bronze_route_patterns@spark",
                    "params:catalog_info_silver_route_patterns"
                ],
                outputs="silver_route_patterns@spark",
                name="silver_route_patterns_first_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=cleaning_mbta_tables,
                inputs=[
                    "bronze_routes@spark",
                    "params:catalog_info_silver_routes"
                ],
                outputs="silver_routes@spark",
                name="silver_routes_first_load",
                tags=["silver", "first_load", "mbta"],
            ),
        ]
    )
