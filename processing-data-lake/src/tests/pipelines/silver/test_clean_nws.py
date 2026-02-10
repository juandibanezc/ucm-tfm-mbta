"""Tests for silver current-load NWS cleaning pipeline."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.silver.nodes import clean_nws


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.filter.return_value = df
    return df


def test_clean_current_load_nws_uses_transform_and_scd_grids():
    """When grids=True, uses transform_grids_table then audit and merge."""

    params = {"keys": ["id"], "grids": True}
    last_ts = {"last_ts": "20240102030405"}

    df = _df_mock()
    with (
        patch.object(clean_nws, "transform_grids_table", return_value=df) as mock_transform,
        patch.object(clean_nws, "transform_point_table") as mock_point,
        patch.object(clean_nws, "audit_cols", side_effect=lambda d, scd_key: d) as mock_audit,
        patch.object(clean_nws.F, "col", side_effect=lambda name: MagicMock(name=f"col({name})")),
        patch.object(clean_nws, "scd1_merge_delta_write") as mock_scd,
    ):

        target = MagicMock(name="delta_table")
        result = clean_nws.clean_current_load(df, params, last_ts, target)

    mock_point.assert_not_called()
    mock_transform.assert_called_once_with(df)
    df.filter.assert_called_once()
    mock_audit.assert_called_once_with(df, scd_key=True)
    mock_scd.assert_called_once_with(source=df, target=target, keys=["id"])
    assert result is True


def test_clean_current_load_nws_uses_transform_points_when_not_grids():
    """When grids flag missing/false, uses transform_point_table."""

    params = {"keys": ["id"], "grids": False}
    last_ts = {"last_ts": "20240102030405"}

    df = _df_mock("points")
    with (
        patch.object(clean_nws, "transform_grids_table") as mock_grids,
        patch.object(clean_nws, "transform_point_table", return_value=df) as mock_point,
        patch.object(clean_nws, "audit_cols", side_effect=lambda d, scd_key: d) as mock_audit,
        patch.object(clean_nws.F, "col", side_effect=lambda name: MagicMock(name=f"col({name})")),
        patch.object(clean_nws, "scd1_merge_delta_write") as mock_scd,
    ):

        target = MagicMock(name="delta_table")
        result = clean_nws.clean_current_load(df, params, last_ts, target)

    mock_grids.assert_not_called()
    mock_point.assert_called_once_with(df)
    df.filter.assert_called_once()
    mock_audit.assert_called_once_with(df, scd_key=True)
    mock_scd.assert_called_once_with(source=df, target=target, keys=["id"])
    assert result is True
