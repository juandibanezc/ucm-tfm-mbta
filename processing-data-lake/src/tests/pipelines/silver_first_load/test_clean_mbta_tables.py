"""Tests for silver_first_load MBTA cleaning nodes and pipeline."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.silver_first_load.nodes import clean_mbta_tables
from processing_datalake.pipelines.silver_first_load import pipeline


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.select.return_value = df
    df.withColumns.return_value = df
    df.distinct.return_value = df
    return df


def test_build_schema_casts_columns():
    """_build_schema returns column cast mapping."""

    with patch.object(clean_mbta_tables.F, "col", side_effect=lambda k: MagicMock(name=f"col({k})")) as mock_col:  # noqa: E501
        schema = {"a": "int", "b": "string"}
        result = clean_mbta_tables._build_schema(schema)

    assert set(result.keys()) == {"a", "b"}
    assert all(hasattr(col, "cast") for col in result.values())
    assert mock_col.call_count == 2


@patch.object(clean_mbta_tables, "audit_cols", return_value=MagicMock(name="audited"))
@patch.object(clean_mbta_tables, "_build_schema", return_value={"c1": "casted", "c2": "casted"})
@patch.object(clean_mbta_tables.F, "col", side_effect=lambda k: MagicMock(name=f"col({k})"))
def test_transformation_route_and_duplicate_handling(mock_col, mock_build_schema, mock_audit_cols):
    """transformation handles route_table explode, duplicate_table, and casts."""

    df = _df_mock("raw")
    df.columns = [
        "direction_names",
        "direction_destinations",
        "x",
        "y",
        "created_ts",
        "updated_ts",
        "source_file",
        "scd_key",
    ]

    # Ensure distinct is tracked on the object returned after drop()
    with_columns_df = MagicMock(name="with_columns_df")
    with_columns_df.drop.return_value = with_columns_df
    with_columns_df.distinct.return_value = with_columns_df
    with_columns_df.withColumns.return_value = with_columns_df
    df.withColumns.return_value = with_columns_df

    params = {
        "schema": {"c1": "int"},
        "route_table": True,
        "duplicate_table": True,
    }

    result = clean_mbta_tables.transformation(df, params)

    df.select.assert_called_once()
    df.withColumns.assert_called_once()
    with_columns_df.drop.assert_called_once()
    with_columns_df.distinct.assert_called_once()
    mock_build_schema.assert_called_once()
    assert result == with_columns_df.withColumns.return_value


@patch.object(clean_mbta_tables, "transformation", return_value=MagicMock(name="transformed"))
@patch.object(clean_mbta_tables, "audit_cols", return_value=MagicMock(name="audited"))
def test_clean_tables_runs_transformation_and_audit(mock_audit_cols, mock_transformation):
    """clean_tables applies transformation then audit_cols."""

    df = MagicMock(name="input_df")
    params = {"schema": {}}

    result = clean_mbta_tables.clean_tables(df, params)

    mock_transformation.assert_called_once_with(df, params)
    mock_audit_cols.assert_called_once_with(mock_transformation.return_value, scd_key=True)
    assert result == mock_audit_cols.return_value


def test_silver_pipeline_contains_expected_nodes():
    """Pipeline defines all silver first-load nodes."""

    p = pipeline.create_pipeline()
    node_names = {n.name for n in p.nodes}

    expected = {
        "silver_schedules_first_load",
        "silver_trips_first_load",
        "silver_stops_first_load",
        "silver_route_patterns_first_load",
        "silver_routes_first_load",
        "silver_grids_first_load",
        "silver_points_first_load",
    }

    assert node_names == expected
    assert len(p.nodes) == len(expected)
