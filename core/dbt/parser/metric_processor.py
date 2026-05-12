from typing import Any, List

from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.type_enums import MetricType

import dbt.exceptions
from dbt.artifacts.resources import MetricInput
from dbt.contracts.graph.manifest import Disabled, Manifest
from dbt.contracts.graph.nodes import Metric
from dbt.node_types import NodeType


def _add_depends_on_metrics_to_v2_metric(
    metric: Metric,
    input_metrics: List[MetricInput],
    manifest: Manifest,
    current_project: str,
) -> None:
    """Set the depends_on property for a v2 metric that depends on other metrics"""
    for input_metric in input_metrics:
        target_metric = manifest.resolve_metric(
            target_metric_name=input_metric.name,
            target_metric_package=None,
            current_project=current_project,
            node_package=metric.package_name,
        )

        if target_metric is None:
            raise dbt.exceptions.ParsingError(
                f"The metric `{input_metric.name}` does not exist but was referenced.",
                node=metric,
            )
        elif isinstance(target_metric, Disabled):
            raise dbt.exceptions.ParsingError(
                f"The metric `{input_metric.name}` is disabled and thus cannot be referenced.",
                node=metric,
            )

        _process_metric_node(
            manifest=manifest,
            current_project=current_project,
            metric=target_metric,
        )
        metric.depends_on.add_node(target_metric.unique_id)


def _process_metric_depends_on_semantic_models_for_measures(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
) -> None:
    """For a given v1 metric, set the `depends_on` property"""

    assert (
        len(metric.type_params.input_measures) > 0
        or metric.type_params.metric_aggregation_params is not None
    ), f"{metric} should have a measure or agg type defined, but it does not."
    for input_measure in metric.type_params.input_measures:
        target_semantic_model = manifest.resolve_semantic_model_for_measure(
            target_measure_name=input_measure.name,
            current_project=current_project,
            node_package=metric.package_name,
        )
        if target_semantic_model is None:
            raise dbt.exceptions.ParsingError(
                f"A semantic model having a measure `{input_measure.name}` does not exist but was referenced.",
                node=metric,
            )
        if target_semantic_model.config.enabled is False:
            raise dbt.exceptions.ParsingError(
                f"The measure `{input_measure.name}` is referenced on disabled semantic model `{target_semantic_model.name}`.",
                node=metric,
            )

        metric.depends_on.add_node(target_semantic_model.unique_id)


def _process_multiple_metric_inputs(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
    metric_inputs: List[MetricInput],
) -> None:
    for input_metric in metric_inputs:
        target_metric = manifest.resolve_metric(
            target_metric_name=input_metric.name,
            target_metric_package=None,
            current_project=current_project,
            node_package=metric.package_name,
        )

        if target_metric is None:
            raise dbt.exceptions.ParsingError(
                f"The metric `{input_metric.name}` does not exist but was referenced.",
                node=metric,
            )
        elif isinstance(target_metric, Disabled):
            raise dbt.exceptions.ParsingError(
                f"The metric `{input_metric.name}` is disabled and thus cannot be referenced.",
                node=metric,
            )

        _process_metric_node(
            manifest=manifest, current_project=current_project, metric=target_metric
        )
        for input_measure in target_metric.type_params.input_measures:
            metric.add_input_measure(input_measure)
        metric.depends_on.add_node(target_metric.unique_id)


def _process_v2_cumulative_metric(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
) -> None:
    cumulative_type_params = metric.type_params.cumulative_type_params
    input_metric = cumulative_type_params.metric if cumulative_type_params is not None else None
    if input_metric is None:
        raise dbt.exceptions.ParsingError(
            f"Cumulative metric {metric} should have a measure or input_metric defined, but it does not.",
            node=metric,
        )
    _add_depends_on_metrics_to_v2_metric(
        metric,
        input_metrics=[input_metric],
        manifest=manifest,
        current_project=current_project,
    )


def _process_cumulative_metric(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
) -> None:
    if metric.type_params.measure is not None:
        # v1: measure-based
        metric.add_input_measure(metric.type_params.measure)
        _process_metric_depends_on_semantic_models_for_measures(
            manifest=manifest, current_project=current_project, metric=metric
        )
    else:
        _process_v2_cumulative_metric(
            manifest=manifest, current_project=current_project, metric=metric
        )


def _process_v1_conversion_metric(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
    conversion_type_params: Any,
) -> None:
    base_measure = conversion_type_params.base_measure
    conversion_measure = conversion_type_params.conversion_measure
    if conversion_measure is None:
        raise dbt.exceptions.ParsingError(
            f"Conversion metric `{metric.name}` cannot have only one of base measure "
            + "and conversion measure defined.",
            node=metric,
        )
    metric.add_input_measure(base_measure)
    metric.add_input_measure(conversion_measure)
    _process_metric_depends_on_semantic_models_for_measures(
        manifest=manifest,
        current_project=current_project,
        metric=metric,
    )


def _process_v2_conversion_metric(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
    conversion_type_params: Any,
) -> None:
    base_metric = conversion_type_params.base_metric
    conversion_metric = conversion_type_params.conversion_metric
    if conversion_metric is None:
        raise dbt.exceptions.ParsingError(
            f"Conversion metric `{metric.name}` cannot have only one of base metric "
            + "and conversion metric defined.",
            node=metric,
        )
    _add_depends_on_metrics_to_v2_metric(
        metric,
        input_metrics=[base_metric, conversion_metric],
        manifest=manifest,
        current_project=current_project,
    )


def _process_conversion_metric(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
) -> None:
    conversion_type_params = metric.type_params.conversion_type_params
    if conversion_type_params is None:
        raise dbt.exceptions.ParsingError(
            f"{metric.name} is a conversion metric and must have conversion_type_params defined.",
            node=metric,
        )
    if conversion_type_params.base_measure is not None:
        _process_v1_conversion_metric(manifest, current_project, metric, conversion_type_params)
    elif conversion_type_params.base_metric is not None:
        _process_v2_conversion_metric(manifest, current_project, metric, conversion_type_params)
    else:
        raise dbt.exceptions.ParsingError(
            f"Depending the version of YAML being used, conversion metric `{metric.name}` "
            + "must have base and conversion measures or base and conversion metrics defined.",
            node=metric,
        )


def _process_simple_metric(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
) -> None:
    if metric.type_params.measure is None and metric.type_params.metric_aggregation_params is None:
        raise dbt.exceptions.ParsingError(
            f"Simple metric {metric} should have a measure or agg type defined, but it does not.",
            node=metric,
        )
    if metric.type_params.measure is not None:
        # v1: measure-based
        metric.add_input_measure(metric.type_params.measure)
        _process_metric_depends_on_semantic_models_for_measures(
            manifest=manifest, current_project=current_project, metric=metric
        )
    else:
        # v2: semantic model-based
        semantic_model_dependency = metric.type_params.get_semantic_model_name()
        if semantic_model_dependency is None:
            raise dbt.exceptions.ParsingError(
                f"Simple metric `{metric.name}` must be attached to a semantic model.",
                node=metric,
            )
        unique_id = f"{NodeType.SemanticModel}.{current_project}.{semantic_model_dependency}"
        metric.depends_on.add_node(unique_id)


def _process_metric_node(
    manifest: Manifest,
    current_project: str,
    metric: Metric,
) -> None:
    """Sets a metric's `input_measures` and `depends_on` properties"""
    # This ensures that if this metrics input_measures have already been set
    # we skip the work. This could happen either due to recursion or if multiple
    # metrics derive from another given metric.
    # NOTE: This does not protect against infinite loops
    if len(metric.type_params.input_measures) > 0:
        return
        # TODO DI-4613: we need a v2 equivalent to avoid unnecessary work!  (This will
        # probably require passing through / maintaining a "processed" set of metric names)

    if metric.type is MetricType.SIMPLE:
        _process_simple_metric(manifest=manifest, current_project=current_project, metric=metric)
    elif metric.type is MetricType.CUMULATIVE:
        _process_cumulative_metric(
            manifest=manifest, current_project=current_project, metric=metric
        )
    elif metric.type is MetricType.CONVERSION:
        _process_conversion_metric(
            manifest=manifest, current_project=current_project, metric=metric
        )
    elif metric.type is MetricType.DERIVED or metric.type is MetricType.RATIO:
        input_metrics = metric.input_metrics
        if metric.type is MetricType.RATIO:
            if metric.type_params.numerator is None or metric.type_params.denominator is None:
                raise dbt.exceptions.ParsingError(
                    "Invalid ratio metric. Both a numerator and denominator must be specified",
                    node=metric,
                )
            input_metrics = [metric.type_params.numerator, metric.type_params.denominator]

        for input_metric in input_metrics:
            target_metric = manifest.resolve_metric(
                target_metric_name=input_metric.name,
                target_metric_package=None,
                current_project=current_project,
                node_package=metric.package_name,
            )
            if target_metric is None:
                raise dbt.exceptions.ParsingError(
                    f"The metric `{input_metric.name}` does not exist but was referenced by metric `{metric.name}`.",
                    node=metric,
                )
            elif isinstance(target_metric, Disabled):
                raise dbt.exceptions.ParsingError(
                    f"The metric `{input_metric.name}` is disabled and thus cannot be referenced.",
                    node=metric,
                )
            _process_metric_node(
                manifest=manifest, current_project=current_project, metric=target_metric
            )
            for input_measure in target_metric.type_params.input_measures:
                metric.add_input_measure(input_measure)
            metric.depends_on.add_node(target_metric.unique_id)
    else:
        assert_values_exhausted(metric.type)
