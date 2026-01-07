"""Gold pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the silver pipeline"""
    return pipeline(
        [
            node(
                func=lambda x: x,
                inputs="bronze_data",
                outputs="silver_data",
                name="silver_data_transformation_node",
            )
        ]
    )
