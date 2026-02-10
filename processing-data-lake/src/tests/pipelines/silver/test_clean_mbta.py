"""Tests for silver current-load MBTA cleaning pipeline."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.silver import pipeline
from processing_datalake.pipelines.silver.nodes import clean_mbta


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.filter.return_value = df
    return df


def test_clean_current_load_mbta_runs_transformation_and_scd():
    """clean_current_load should filter, transform, audit, and merge."""

    params = {"keys": ["id"]}
    last_ts = {"last_ts": "20240102030405"}

    df = _df_mock()
    with (
        patch.object(clean_mbta, "transformation", return_value=df) as mock_transform,
        patch.object(clean_mbta, "audit_cols", side_effect=lambda d, scd_key: d) as mock_audit,
        patch.object(clean_mbta.F, "col", side_effect=lambda name: MagicMock(name=f"col({name})")),
        patch.object(clean_mbta, "scd1_merge_delta_write") as mock_scd,
    ):

        target = MagicMock(name="delta_table")
        result = clean_mbta.clean_current_load(df, params, last_ts, target)

    df.filter.assert_called_once()
    mock_transform.assert_called_once_with(df, params)
    mock_audit.assert_called_once_with(df, scd_key=True)
    mock_scd.assert_called_once_with(source=df, target=target, keys=["id"])
    assert result is True


def test_silver_pipeline_nodes_mbta():
    """Pipeline defines expected silver MBTA/NWS current-load nodes."""

    p = pipeline.create_pipeline()
    node_names = {n.name for n in p.nodes}

    expected = {
        "silver_schedules_load",
        "silver_trips_load",
        "silver_stops_load",
        "silver_route_patterns_load",
        "silver_routes_load",
        "silver_grids_load",
        "silver_points_load",
    }

    assert node_names == expected
    assert len(p.nodes) == len(expected)
