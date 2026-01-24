"""Silver pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline

from processing_datalake.pipelines.silver.nodes.clean_mbta import (
    clean_current_load as clean_current_load_mbta,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the silver pipeline"""
    return pipeline(
        [
            node(
                func=clean_current_load_mbta,
                inputs=[
                    "bronze_schedules@spark",
                    "params:catalog_info_silver_schedules",
                    "landing_last_execution@json",
                    "silver_schedules@delta",
                ],
                outputs="silver_schedules_true",
                name="silver_schedules_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=clean_current_load_mbta,
                inputs=[
                    "bronze_trips@spark",
                    "params:catalog_info_silver_trips",
                    "landing_last_execution@json",
                    "silver_trips@delta",
                ],
                outputs="silver_trips_true",
                name="silver_trips_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=clean_current_load_mbta,
                inputs=[
                    "bronze_stops@spark",
                    "params:catalog_info_silver_stops",
                    "landing_last_execution@json",
                    "silver_stops@delta",
                ],
                outputs="silver_stops_true",
                name="silver_stops_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=clean_current_load_mbta,
                inputs=[
                    "bronze_route_patterns@spark",
                    "params:catalog_info_silver_route_patterns",
                    "landing_last_execution@json",
                    "silver_route_patterns@delta",
                ],
                outputs="silver_route_patterns_true",
                name="silver_route_patterns_load",
                tags=["silver", "first_load", "mbta"],
            ),
            node(
                func=clean_current_load_mbta,
                inputs=[
                    "bronze_routes@spark",
                    "params:catalog_info_silver_routes",
                    "landing_last_execution@json",
                    "silver_routes@delta",
                ],
                outputs="silver_routes_true",
                name="silver_routes_load",
                tags=["silver", "first_load", "mbta"],
            ),
        ]
    )
