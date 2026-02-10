"""Tests for gold_first_load routes forecast node."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.gold_first_load.nodes import routes_forecast


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.alias.return_value = df
    df.join.return_value = df
    df.groupBy.return_value = df
    df.agg.return_value = df
    df.filter.return_value = df
    df.withColumn.return_value = df

    # select returns an object that supports withColumn/filter
    select_df = MagicMock(name=f"{name}_selected")
    select_df.withColumn.return_value = select_df
    select_df.filter.return_value = select_df
    select_df.select.return_value = select_df
    select_df.alias.return_value = select_df
    select_df.join.return_value = select_df
    select_df.groupBy.return_value = select_df
    select_df.agg.return_value = select_df
    df.select.return_value = select_df

    # ensure base df alias/join also chainable
    df.groupBy.return_value = df
    df.agg.return_value = df

    return df


def _fake_functions_module():
    """Create a lightweight fake pyspark.sql.functions namespace."""

    def _col(name):
        col = MagicMock(name=f"col({name})")
        col.__getitem__.return_value = MagicMock(name=f"col({name})getitem")
        col.cast.return_value = MagicMock(name=f"cast({name})")
        return col

    return MagicMock(
        col=_col,
        explode=lambda *_: MagicMock(name="explode"),
        row_number=lambda *_: MagicMock(name="row_number"),
        to_date=lambda *_: MagicMock(name="to_date"),
        date_format=lambda *_: MagicMock(name="date_format"),
        count=lambda *_: MagicMock(name="count"),
    )


class _FakeWindow:
    @staticmethod
    def partitionBy(*_args, **_kwargs):
        w = MagicMock(name="Window")
        w.orderBy.return_value = MagicMock(name="window_order")
        return w


@patch.object(routes_forecast, "F", new_callable=_fake_functions_module)
@patch.object(routes_forecast, "W", _FakeWindow)
def test_process_routes_forecast_chains_operations(fake_f):
    """process_routes_forecast should chain select/join/group/filters."""

    grids = _df_mock("grids")
    points = _df_mock("points")
    schedules = _df_mock("schedules")

    result = routes_forecast.process_routes_forecast(grids, points, schedules)

    grids.select.assert_called()
    grids.select.return_value.withColumn.assert_called()
    grids.select.return_value.filter.assert_called()
    schedules.select.assert_called()
    assert any([
        schedules.select.return_value.join.called,
        schedules.join.called,
        schedules.alias.return_value.join.called,
    ]), "Expected at least one join call"
    schedules.select.return_value.groupBy.assert_called()
    schedules.select.return_value.agg.assert_called()
    schedules.select.assert_called()
    assert result is not None


@patch.object(routes_forecast, "audit_cols", return_value=MagicMock(name="audited"))
@patch.object(routes_forecast, "process_routes_forecast", return_value=MagicMock(name="processed"))
def test_create_routes_forecast_calls_audit(mock_process, mock_audit):
    """create_routes_forecast runs process then audit_cols."""

    grids = MagicMock(name="grids")
    points = MagicMock(name="points")
    schedules = MagicMock(name="schedules")

    result = routes_forecast.create_routes_forecast(grids, points, schedules)

    mock_process.assert_called_once_with(grids=grids, points=points, schedules=schedules)
    mock_audit.assert_called_once_with(mock_process.return_value)
    assert result == mock_audit.return_value
