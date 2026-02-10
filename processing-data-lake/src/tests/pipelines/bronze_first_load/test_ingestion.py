"""Tests for bronze_first_load ingestion nodes and pipeline."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.bronze_first_load.nodes import ingestion
from processing_datalake.pipelines.bronze_first_load import pipeline


def _build_mock_df(name: str = "df"):
    """Return a simple DataFrame-like mock that chains operations."""

    df = MagicMock(name=name)
    df.withColumn.return_value = df
    df.selectExpr.return_value = MagicMock(name=f"{name}_selected")
    df.filter.return_value = MagicMock(name=f"{name}_filtered")
    return df


@patch.object(ingestion, "get_dataset")
@patch.object(ingestion.F, "explode", side_effect=lambda col: f"explode({col})")
@patch.object(ingestion.F, "col", side_effect=lambda name: f"col({name})")
def test_single_table_processing_formats_path_and_selects(mock_col, mock_explode, mock_get_dataset):
    """single_table_processing builds filepath, explodes data, and selects columns."""

    catalog = MagicMock()
    catalog._filepath = "/tmp/{last_ts}/{year}/{month}/{day}/file.json"
    table_df = _build_mock_df("raw_df")
    catalog.load.return_value = table_df
    mock_get_dataset.return_value = catalog

    params = {
        "catalog_dataset": "test_catalog",
        "columns": ["col1", "col2"],
    }
    last_exec = {"last_ts": "20240102030405"}

    result = ingestion.single_table_processing(params, last_exec)

    expected_path = "/tmp/20240102030405/2024/01/02/file.json"
    assert str(catalog._filepath) == expected_path
    mock_get_dataset.assert_called_once_with("test_catalog")
    catalog.load.assert_called_once()
    table_df.withColumn.assert_called_once()
    table_df.selectExpr.assert_called_once_with(*params["columns"])
    assert result == table_df.selectExpr.return_value


@patch.object(ingestion, "get_dataset")
def test_single_table_processing_skips_explode_when_disabled(mock_get_dataset):
    """Explode is skipped when explode_column is False."""

    catalog = MagicMock()
    catalog._filepath = "/tmp/{last_ts}/{year}/{month}/{day}/file.json"
    table_df = _build_mock_df("raw_df_no_explode")
    catalog.load.return_value = table_df
    mock_get_dataset.return_value = catalog

    params = {
        "catalog_dataset": "test_catalog",
        "columns": ["col1"],
        "explode_column": False,
    }
    last_exec = {"last_ts": "20240102030405"}

    result = ingestion.single_table_processing(params, last_exec)

    table_df.withColumn.assert_not_called()
    table_df.selectExpr.assert_called_once_with(*params["columns"])
    assert result == table_df.selectExpr.return_value


def _mock_col_with_contains(name: str):
    col_mock = MagicMock(name=f"col({name})")
    col_mock.contains.return_value = f"contains({name})"
    return col_mock


@patch.object(ingestion, "audit_cols", return_value=_build_mock_df("audited"))
@patch.object(ingestion, "add_filename_column", side_effect=lambda df: df)
@patch.object(ingestion, "single_table_processing", return_value=_build_mock_df("processed"))
@patch.object(ingestion.F, "col", side_effect=_mock_col_with_contains)
def test_process_table_applies_audit_and_filter(mock_col, mock_single, mock_add_filename, mock_audit_cols):
    """process_table enriches and filters using last_ts."""

    params = {"dummy": "value"}
    last_exec = {"last_ts": "20240102030405"}

    result = ingestion.process_table(params, last_exec)

    mock_single.assert_called_once_with(params, last_exec)
    mock_add_filename.assert_called_once()
    assert mock_audit_cols.call_count == 1
    assert mock_audit_cols.call_args.kwargs.get("scd_key") is True
    mock_audit_cols.return_value.filter.assert_called_once()
    assert result == mock_audit_cols.return_value.filter.return_value


def test_create_pipeline_defines_all_nodes():
    """Pipeline includes expected bronze first-load nodes."""

    bronze_pipeline = pipeline.create_pipeline()
    node_names = [n.name for n in bronze_pipeline.nodes]

    expected = {
        "routes_bronze_first_load_node",
        "route_pattern_bronze_first_load_node",
        "stop_bronze_first_load_node",
        "schedules_bronze_first_load_node",
        "trips_bronze_first_load_node",
        "points_bronze_first_load_node",
        "grids_bronze_first_load_node",
    }

    assert expected == set(node_names)
    assert len(bronze_pipeline.nodes) == len(expected)
