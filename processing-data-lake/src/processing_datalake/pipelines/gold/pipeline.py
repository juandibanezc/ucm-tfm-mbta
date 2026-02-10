"""Gold pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.gold.nodes.trips_metrics_process import (
    process_trips_metrics,
)
from processing_datalake.pipelines.gold.nodes.routes_forecast_process import (
    process_route_forecast_metrics,
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
                    "landing_last_execution@json",
                    "params:catalog_info_gold_trips_metrics",
                ],
                outputs="gold_trips_metrics_true",
                name="gold_trips_metrics_transformation_node",
                tags=["gold"],
            ),
            node(
                func=process_route_forecast_metrics,
                inputs=[
                    "silver_grids@spark",
                    "silver_points@spark",
                    "silver_schedules@spark",
                    "landing_last_execution@json",
                    "params:catalog_info_gold_routes_forecast",
                ],
                outputs="gold_routes_forecast_true",
                name="gold_routes_forecast_transformation_node",
                tags=["gold"],
            ),
        ]
    )
