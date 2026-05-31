from unittest.mock import MagicMock

from dbt.contracts.graph.unparsed import (
    UnparsedMetric,
    UnparsedMetricInputMeasure,
    UnparsedMetricTypeParams,
    UnparsedMetricV2,
)
from dbt.parser.schema_yaml_readers import MetricParser


def _metric_parser() -> MetricParser:
    return MetricParser(MagicMock(), MagicMock())


def test_get_metric_type_params_v1_fill_nulls_with():
    unparsed = UnparsedMetric(
        name="orders",
        label="Orders",
        type="simple",
        type_params=UnparsedMetricTypeParams(
            measure=UnparsedMetricInputMeasure(name="order_count", fill_nulls_with=0),
        ),
    )
    type_params = _metric_parser()._get_metric_type_params(unparsed)
    assert type_params.measure.fill_nulls_with == 0
    assert type_params.fill_nulls_with == 0


def test_get_metric_type_params_v2_fill_nulls_with():
    unparsed = UnparsedMetricV2(
        name="orders",
        label="Orders",
        type="simple",
        agg="count",
        fill_nulls_with=0,
        join_to_timespine=True,
    )
    type_params = _metric_parser()._get_metric_type_params(
        unparsed,
        generated_from="fct_orders",
        default_agg_time_dimension="order_date",
    )
    assert type_params.fill_nulls_with == 0
    assert type_params.join_to_timespine is True
