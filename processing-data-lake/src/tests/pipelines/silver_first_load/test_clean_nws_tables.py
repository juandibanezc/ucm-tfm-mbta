"""Tests for silver_first_load NWS cleaning nodes."""
from unittest.mock import MagicMock, patch
from processing_datalake.pipelines.silver_first_load.nodes import clean_nws_tables


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.select.return_value = MagicMock(name=f"{name}_selected")
    return df


@patch.object(clean_nws_tables.F, "col", side_effect=lambda k: MagicMock(name=f"col({k})"))
def test_transform_grids_table_selects_and_casts(mock_col):
    """transform_grids_table selects expected columns."""

    df = _df_mock("grids")

    result = clean_nws_tables.transform_grids_table(df)

    df.select.assert_called_once()
    assert result == df.select.return_value


@patch.object(clean_nws_tables.F, "col", side_effect=lambda k: MagicMock(name=f"col({k})"))
@patch.object(clean_nws_tables.F, "concat_ws", side_effect=lambda *_args, **_kwargs: MagicMock(name="concat_ws"))  # noqa: E501
def test_transform_point_table_selects_and_casts(mock_concat, mock_col):
    """transform_point_table selects expected columns."""

    df = _df_mock("points")

    result = clean_nws_tables.transform_point_table(df)

    df.select.assert_called_once()
    assert result == df.select.return_value


@patch.object(clean_nws_tables, "transform_grids_table", return_value=_df_mock("grids_t"))
@patch.object(clean_nws_tables, "audit_cols", return_value=MagicMock(name="audited"))
def test_clean_grids_table_calls_audit(mock_audit, mock_transform):
    """clean_grids_table transforms then audits."""

    df = MagicMock(name="input_grids")

    result = clean_nws_tables.clean_grids_table(df)

    mock_transform.assert_called_once_with(df)
    mock_audit.assert_called_once_with(mock_transform.return_value, scd_key=True)
    assert result == mock_audit.return_value


@patch.object(clean_nws_tables, "transform_point_table", return_value=_df_mock("points_t"))
@patch.object(clean_nws_tables, "audit_cols", return_value=MagicMock(name="audited"))
def test_clean_points_table_calls_audit(mock_audit, mock_transform):
    """clean_points_table transforms then audits."""

    df = MagicMock(name="input_points")

    result = clean_nws_tables.clean_points_table(df)

    mock_transform.assert_called_once_with(df)
    mock_audit.assert_called_once_with(mock_transform.return_value, scd_key=True)
    assert result == mock_audit.return_value
