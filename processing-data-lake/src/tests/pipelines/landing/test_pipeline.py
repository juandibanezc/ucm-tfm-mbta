"""Tests for the landing pipeline"""
from kedro.pipeline import Pipeline
from processing_datalake.pipelines.landing.pipeline import create_pipeline


def test_pipeline_creation():
    """Test that the landing pipeline is created correctly."""
    pipeline = create_pipeline()
    assert isinstance(pipeline, Pipeline)


def test_pipeline_nodes():
    """Test that the landing pipeline has the correct number of nodes."""
    pipeline = create_pipeline()
    assert len(pipeline.nodes) == 6
