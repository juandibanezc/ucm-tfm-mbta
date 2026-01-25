"""Gold pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline
from processing_datalake.pipelines.gold_first_load.nodes.trips_metrics import create_trips_metrics_table


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the gold pipeline"""
    return pipeline(
        [
            node(
                func=create_trips_metrics_table,
                inputs=[
                    "silver_schedules@spark",
                    "silver_routes@spark",
                    "silver_trips@spark",
                    "silver_route_patterns@spark"
                ],
                outputs="gold_trips_metrics@spark",
                name="gold_trips_metrics_transformation_first_load",
                tags=["gold", "first_load"],
            ),
        ]
    )
