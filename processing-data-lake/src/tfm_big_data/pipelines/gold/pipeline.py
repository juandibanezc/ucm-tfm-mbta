"""Gold pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the gold pipeline"""
    return pipeline(
        [
            node(
                func=lambda x: x,
                inputs="silver_data",
                outputs="gold_data",
                name="gold_data_transformation_node",
            )
        ]
    )
