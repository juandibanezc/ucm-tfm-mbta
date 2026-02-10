"""Tests for gold_first_load trips metrics nodes and pipeline."""
from unittest.mock import MagicMock, patch

from processing_datalake.pipelines.gold_first_load.nodes import trips_metrics
from processing_datalake.pipelines.gold_first_load import pipeline


def _df_mock(name="df"):
    df = MagicMock(name=name)
    df.groupBy.return_value = df
    df.agg.return_value = df
    df.withColumn.return_value = df
    df.alias.return_value = df
    df.join.return_value = df
    df.select.return_value = MagicMock(name=f"{name}_selected")
    return df


def _fake_functions_module():
    """Create a lightweight fake pyspark.sql.functions namespace."""

    def _col(name):
        col = MagicMock(name=f"col({name})")
        col.__getitem__.return_value = MagicMock(name=f"col({name})getitem")
        return col

    return MagicMock(
        min=lambda *_: MagicMock(name="min"),
        max=lambda *_: MagicMock(name="max"),
        count=lambda *_: MagicMock(name="count"),
        unix_timestamp=lambda *_: MagicMock(name="unix_timestamp"),
        to_date=lambda *_: MagicMock(name="to_date"),
        date_format=lambda *_: MagicMock(name="date_format"),
        when=lambda *_1, **_2: MagicMock(name="when"),
        col=_col,
    )


@patch.object(trips_metrics, "F", new_callable=_fake_functions_module)
def test_create_trips_metrics_table_builds_select_chain(fake_f):
    """create_trips_metrics_table performs expected operations chain."""

    schedules = _df_mock("schedules")
    routes = _df_mock("routes")
    trips = _df_mock("trips")
    route_patterns = _df_mock("route_patterns")

    result = trips_metrics.create_trips_metrics_table(schedules, routes, trips, route_patterns)

    schedules.groupBy.assert_called_once()
    schedules.agg.assert_called_once()
    schedules.withColumn.assert_called_once()
    schedules.alias.assert_called_once()
    schedules.join.assert_called()  # at least once
    schedules.select.assert_called_once()
    assert result == schedules.select.return_value


@patch.object(trips_metrics, "audit_cols", return_value=MagicMock(name="audited"))
@patch.object(trips_metrics, "create_trips_metrics_table", return_value=MagicMock(name="created"))
def test_process_trips_metrics_applies_audit(mock_create, mock_audit):
    """process_trips_metrics runs create then audit_cols."""

    schedules = MagicMock(name="schedules")
    routes = MagicMock(name="routes")
    trips = MagicMock(name="trips")
    route_patterns = MagicMock(name="route_patterns")

    result = trips_metrics.process_trips_metrics(schedules, routes, trips, route_patterns)

    mock_create.assert_called_once_with(schedules, routes, trips, route_patterns)
    mock_audit.assert_called_once_with(mock_create.return_value)
    assert result == mock_audit.return_value


def test_gold_pipeline_contains_expected_nodes():
    """Pipeline defines all gold first-load nodes."""

    p = pipeline.create_pipeline()
    node_names = {n.name for n in p.nodes}

    expected = {
        "gold_trips_metrics_transformation_first_load",
        "gold_routes_forecast_first_load",
    }

    assert node_names == expected
    assert len(p.nodes) == len(expected)
