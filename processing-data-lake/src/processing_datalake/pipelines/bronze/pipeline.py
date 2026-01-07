"""Bronze pipeline definition."""

from kedro.pipeline import Pipeline, node, pipeline


def create_pipeline(**kwargs) -> Pipeline:
    """Creates the bronze pipeline"""
    return pipeline(
        [
            node(
                func=lambda x: x,
                inputs="raw_data",
                outputs="bronze_data",
                name="bronze_data_ingestion_node",
            )
        ]
    )
