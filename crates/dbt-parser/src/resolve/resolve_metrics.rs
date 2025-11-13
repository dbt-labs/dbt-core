use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::utils::{get_node_fqn, get_original_file_path, get_unique_id};
use dbt_common::io_args::{StaticAnalysisKind, StaticAnalysisOffReason};
use dbt_common::tracing::emit::{emit_error_log_from_fs_error, emit_error_log_message};
use dbt_common::{ErrorCode, FsResult};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::serde::into_typed_with_error;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::common::{DbtChecksum, NodeDependsOn};
use dbt_schemas::schemas::project::{DefaultTo, MetricConfig};
use dbt_schemas::schemas::properties::metrics_properties::{MetricType, PercentileType};
use dbt_schemas::schemas::properties::{MetricsProperties, ModelProperties};
use dbt_schemas::schemas::{CommonAttributes, NodeBaseAttributes};
use dbt_schemas::state::DbtPackage;
use minijinja::value::Value as MinijinjaValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;
use super::validate_metrics::validate_metric;

use dbt_schemas::schemas::manifest::metric::{
    DbtMetric, DbtMetricAttr, MeasureAggregationParameters, MetricAggregationParameters,
    MetricTypeParams, NonAdditiveDimension,
};

type ResolveMetricsResult = FsResult<(
    HashMap<String, Arc<DbtMetric>>,
    HashMap<String, Arc<DbtMetric>>,
)>;

/// Helper to compute the effective config for a given metric
fn get_effective_metric_config(
    metric_fqn: &[String],
    root_project_configs: &RootProjectConfigs,
    resource_config: &MetricConfig,
    metric_props: &MetricsProperties,
) -> MetricConfig {
    let mut project_config = root_project_configs
        .metrics
        .get_config_for_fqn(metric_fqn)
        .clone();
    project_config.default_to(resource_config);

    if let Some(config) = &metric_props.config {
        let mut final_config = config.clone();
        final_config.default_to(&project_config);
        final_config
    } else {
        project_config
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_metrics(
    arg: &ResolveArgs,
    package: &DbtPackage,
    root_project_configs: &RootProjectConfigs,
    minimal_model_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    minimal_metric_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    typed_models_properties: &BTreeMap<String, ModelProperties>,
    package_name: &str,
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
) -> ResolveMetricsResult {
    let mut metrics: HashMap<String, Arc<DbtMetric>> = HashMap::new();
    let mut disabled_metrics: HashMap<String, Arc<DbtMetric>> = HashMap::new();
    let mut seen_metric_names = HashSet::new();

    let (nested_metrics, nested_disabled_metrics) = resolve_nested_model_metrics(
        arg,
        package,
        root_project_configs,
        minimal_model_properties,
        typed_models_properties,
        package_name,
        env,
        base_ctx,
        &mut seen_metric_names,
    )?;
    metrics.extend(nested_metrics);
    disabled_metrics.extend(nested_disabled_metrics);

    let (top_level_metrics, top_level_disabled_metrics) = resolve_top_level_metrics(
        arg,
        package,
        root_project_configs,
        minimal_metric_properties,
        package_name,
        env,
        base_ctx,
        &mut seen_metric_names,
    )?;
    metrics.extend(top_level_metrics);
    disabled_metrics.extend(top_level_disabled_metrics);

    Ok((metrics, disabled_metrics))
}

#[allow(clippy::too_many_arguments)]
pub fn resolve_nested_model_metrics(
    arg: &ResolveArgs,
    package: &DbtPackage,
    root_project_configs: &RootProjectConfigs,
    minimal_model_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    typed_models_properties: &BTreeMap<String, ModelProperties>,
    package_name: &str,
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    seen_metric_names: &mut HashSet<String>,
) -> ResolveMetricsResult {
    let mut metrics = HashMap::new();
    let mut disabled_metrics = HashMap::new();

    if minimal_model_properties.is_empty() {
        return Ok((metrics, disabled_metrics));
    }

    // TODO: what is the difference between 'package_name' and 'dependency_package_name'?
    let dependency_package_name = dependency_package_name_from_ctx(env, base_ctx);
    let local_project_config = init_project_config(
        &arg.io,
        &package.dbt_project.metrics,
        MetricConfig {
            enabled: Some(true),
            ..Default::default()
        },
        dependency_package_name,
    )?;

    for (model_name, model_props) in typed_models_properties.iter() {
        if model_props.metrics.is_none() {
            continue;
        }

        let mpe = minimal_model_properties
            .get(model_name)
            .unwrap_or_else(|| panic!("ModelPropertiesEntry must exist for model '{model_name}'"));

        let mut semantic_model_name = model_props.name.clone();
        if let Some(semantic_model) = &model_props.semantic_model
            && let Some(name) = &semantic_model.name
        {
            semantic_model_name = name.clone();
        }
        let semantic_model_unique_id =
            get_unique_id(&semantic_model_name, package_name, None, "semantic_model");

        if let Some(model_metrics_props) = model_props.metrics.clone().as_ref() {
            for metric_props in model_metrics_props {
                let metric_name = metric_props.name.clone();

                // Validate metric (name and window)
                if let Err(e) = validate_metric(metric_props) {
                    emit_error_log_from_fs_error(&e, arg.io.status_reporter.as_ref());

                    continue;
                }

                // Check for duplicate metric names
                if !seen_metric_names.insert(metric_name.clone()) {
                    emit_error_log_message(
                        ErrorCode::SchemaError,
                        format!(
                            "Duplicate metric name '{}' found in package '{}'",
                            metric_name, package_name
                        ),
                        arg.io.status_reporter.as_ref(),
                    );
                    continue;
                }

                let metric_unique_id = get_unique_id(&metric_name, package_name, None, "metric");
                let metric_fqn = get_node_fqn(
                    package_name,
                    mpe.relative_path.clone(),
                    vec![metric_name.to_owned()],
                    &package.dbt_project.all_source_paths(),
                );

                // Get combined config from project config and metric config
                let resource_config = local_project_config.get_config_for_fqn(&metric_fqn);
                let metric_config = get_effective_metric_config(
                    &metric_fqn,
                    root_project_configs,
                    resource_config,
                    metric_props,
                );

                let mut type_params: MetricTypeParams = metric_props.clone().into();
                type_params.metric_aggregation_params =
                    model_metrics_props_to_metric_aggregation_params(
                        &semantic_model_name,
                        model_props,
                        metric_props,
                    );

                let dbt_metric = DbtMetric {
                    __common_attr__: CommonAttributes {
                        name: metric_name.clone(),
                        package_name: package_name.to_string(),
                        path: mpe.relative_path.clone(),
                        original_file_path: get_original_file_path(
                            &package.package_root_path,
                            &arg.io.in_dir,
                            &mpe.relative_path,
                        ),
                        name_span: dbt_common::Span::from_serde_span(
                            mpe.name_span.clone(),
                            mpe.relative_path.clone(),
                        ),
                        patch_path: Some(mpe.relative_path.clone()),
                        unique_id: metric_unique_id.clone(),
                        fqn: metric_fqn.clone(),
                        description: metric_props.description.clone(),
                        checksum: DbtChecksum::default(),
                        raw_code: None,
                        language: None,
                        tags: metric_config
                            .tags
                            .clone()
                            .map(|tags| tags.into())
                            .unwrap_or_default(),
                        meta: metric_config.meta.clone().unwrap_or_default(),
                    },
                    __base_attr__: NodeBaseAttributes {
                        database: "".to_string(),
                        schema: "".to_string(),
                        alias: "".to_string(),
                        relation_name: None,
                        quoting: Default::default(),
                        materialized: Default::default(),
                        static_analysis: StaticAnalysisKind::Off.into(),
                        static_analysis_off_reason: Some(
                            StaticAnalysisOffReason::UnableToFetchSchema,
                        ),
                        enabled: true,
                        extended_model: false,
                        persist_docs: None,
                        columns: Default::default(),
                        refs: vec![],
                        sources: vec![],
                        functions: vec![],
                        metrics: vec![],
                        depends_on: NodeDependsOn {
                            macros: vec![],
                            nodes: vec![semantic_model_unique_id.clone()],
                            nodes_with_ref_location: vec![],
                        },
                        quoting_ignore_case: false,
                    },
                    __metric_attr__: DbtMetricAttr {
                        unrendered_config: BTreeMap::new(), // TODO: do we need to hydrate?
                        group: metric_config.group.clone(),
                        created_at: chrono::Utc::now().timestamp() as f64,
                        metadata: None,
                        label: metric_props.label.clone(),
                        metric_type: metric_props.type_.clone().unwrap_or_default(),
                        type_params,
                        filter: metric_props.filter.clone().map(|f| vec![f].into()),
                        time_granularity: metric_props.time_granularity.clone(),
                        metrics: vec![], // always empty, hydrated in type_params.metrics
                    },
                    deprecated_config: MetricConfig {
                        enabled: metric_config.enabled,
                        tags: metric_config.tags.clone(),
                        meta: metric_config.meta.clone(),
                        group: metric_config.group.clone(),
                    },
                    __other__: BTreeMap::new(),
                };

                // Check if metric is enabled (following exposures pattern)
                if metric_config.enabled.unwrap_or(true) {
                    metrics.insert(metric_unique_id, Arc::new(dbt_metric));
                } else {
                    disabled_metrics.insert(metric_unique_id, Arc::new(dbt_metric));
                }
            }
        }
    }

    Ok((metrics, disabled_metrics))
}

#[allow(clippy::too_many_arguments)]
pub fn resolve_top_level_metrics(
    arg: &ResolveArgs,
    package: &DbtPackage,
    root_project_configs: &RootProjectConfigs,
    minimal_metric_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    package_name: &str,
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    seen_metric_names: &mut HashSet<String>,
) -> ResolveMetricsResult {
    let mut metrics = HashMap::new();
    let mut disabled_metrics = HashMap::new();

    if minimal_metric_properties.is_empty() {
        return Ok((metrics, disabled_metrics));
    }

    let dependency_package_name = dependency_package_name_from_ctx(env, base_ctx);
    let local_project_config = init_project_config(
        &arg.io,
        &package.dbt_project.metrics,
        MetricConfig {
            enabled: Some(true),
            ..Default::default()
        },
        dependency_package_name,
    )?;

    for (metric_name, mpe) in minimal_metric_properties.iter_mut() {
        if mpe.schema_value.is_null() {
            continue;
        }

        // Parse the metric properties from YAML
        let metric_props: MetricsProperties = into_typed_with_error(
            &arg.io,
            mpe.schema_value.clone(),
            // Set show_errors_or_warnings to false for legacy top-level metrics to avoid strict validation errors, since these metrics use a different specification format than the current semantic layer spec.
            false,
            None,
            None,
        )?;

        let metric_fqn = get_node_fqn(
            package_name,
            mpe.relative_path.clone(),
            vec![metric_name.to_owned()],
            &package.dbt_project.all_source_paths(),
        );

        // Get combined config from project config and metric config
        let resource_config = local_project_config.get_config_for_fqn(&metric_fqn);
        let metric_metric_config = get_effective_metric_config(
            &metric_fqn,
            root_project_configs,
            resource_config,
            &metric_props,
        );

        let metric_name = metric_props.name.clone();

        // Validate metric (name and window)
        if let Err(e) = validate_metric(&metric_props) {
            emit_error_log_from_fs_error(&e, arg.io.status_reporter.as_ref());

            continue;
        }

        // Check for duplicate metric names
        if !seen_metric_names.insert(metric_name.clone()) {
            emit_error_log_message(
                ErrorCode::SchemaError,
                format!(
                    "Duplicate metric name '{}' found in package '{}'",
                    metric_name, package_name
                ),
                arg.io.status_reporter.as_ref(),
            );
            continue;
        }

        let metric_unique_id = get_unique_id(&metric_name, package_name, None, "metric");
        let metric_fqn = get_node_fqn(
            package_name,
            mpe.relative_path.clone(),
            vec![metric_name.to_owned()],
            &package.dbt_project.all_source_paths(),
        );

        let type_params: MetricTypeParams = metric_props.clone().into();
        let metric_type: MetricType = metric_props.type_.clone().unwrap_or_default();

        // in contrast to model nested metrics depending on the underlying semantic model
        // top-level metrics depend on other metrics from other semantic models
        let names_of_nodes_depends_on: Vec<String> = match &metric_type {
            MetricType::Ratio => {
                let maybe_numerator = type_params.numerator.clone();
                let maybe_denominator = type_params.denominator.clone();
                if maybe_numerator.is_none() || maybe_denominator.is_none() {
                    vec![]
                } else {
                    vec![
                        maybe_numerator
                            .expect("Numerator must be specified for ratio metric")
                            .name,
                        maybe_denominator
                            .expect("Denominator must be specified for ratio metric")
                            .name,
                    ]
                }
            }
            MetricType::Derived => {
                let mut metric_names: Vec<String> = type_params
                    .metrics
                    .clone()
                    .unwrap_or_default()
                    .iter()
                    .map(|metric| metric.name.clone())
                    .collect();
                metric_names.dedup();
                metric_names
            }
            MetricType::Cumulative => {
                let maybe_cumulative_metric = type_params
                    .cumulative_type_params
                    .clone()
                    .unwrap_or_default()
                    .metric;

                if maybe_cumulative_metric.is_none() {
                    vec![]
                } else {
                    vec![
                        maybe_cumulative_metric
                            .expect(
                                "cumulative_type_params.metric must exist for cumulative metric",
                            )
                            .name,
                    ]
                }
            }
            MetricType::Conversion => {
                if type_params.conversion_type_params.is_none() {
                    vec![]
                } else {
                    let conversion_type_params = type_params
                        .conversion_type_params
                        .clone()
                        .expect("conversion_type_params must exist for conversion metric");
                    let base_metric_name =
                        conversion_type_params.base_metric.unwrap_or_default().name;
                    let conversion_metric_name = conversion_type_params
                        .conversion_metric
                        .unwrap_or_default()
                        .name;

                    if base_metric_name.is_empty() || conversion_metric_name.is_empty() {
                        vec![]
                    } else {
                        vec![base_metric_name, conversion_metric_name]
                    }
                }
            }
            _ => vec![],
        };

        let unique_ids_of_nodes_depends_on: Vec<String> = names_of_nodes_depends_on
            .iter()
            .map(|name| get_unique_id(name, package_name, None, "metric"))
            .collect();

        let depends_on = NodeDependsOn {
            macros: vec![],
            nodes: unique_ids_of_nodes_depends_on,
            nodes_with_ref_location: vec![],
        };

        let dbt_metric = DbtMetric {
            __common_attr__: CommonAttributes {
                name: metric_name.clone(),
                package_name: package_name.to_string(),
                path: mpe.relative_path.clone(),
                original_file_path: get_original_file_path(
                    &package.package_root_path,
                    &arg.io.in_dir,
                    &mpe.relative_path,
                ),
                name_span: dbt_common::Span::from_serde_span(
                    mpe.name_span.clone(),
                    mpe.relative_path.clone(),
                ),
                patch_path: Some(mpe.relative_path.clone()),
                unique_id: metric_unique_id.clone(),
                fqn: metric_fqn.clone(),
                description: metric_props.description.clone(),
                checksum: DbtChecksum::default(),
                raw_code: None,
                language: None,
                tags: metric_metric_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: metric_metric_config.meta.clone().unwrap_or_default(),
            },
            __base_attr__: NodeBaseAttributes {
                database: "".to_string(),
                schema: "".to_string(),
                alias: "".to_string(),
                relation_name: None,
                quoting: Default::default(),
                materialized: Default::default(),
                static_analysis: StaticAnalysisKind::Off.into(),
                static_analysis_off_reason: Some(StaticAnalysisOffReason::UnableToFetchSchema),
                enabled: true,
                extended_model: false,
                persist_docs: None,
                columns: Default::default(),
                refs: vec![],
                sources: vec![],
                functions: vec![],
                metrics: vec![],
                depends_on,
                quoting_ignore_case: false,
            },
            __metric_attr__: DbtMetricAttr {
                unrendered_config: BTreeMap::new(), // TODO: do we need to hydrate?
                group: metric_metric_config.group.clone(),
                created_at: chrono::Utc::now().timestamp() as f64,
                metadata: None,
                label: metric_props.label.clone(),
                metric_type,
                type_params,
                filter: metric_props.filter.clone().map(|f| vec![f].into()),
                time_granularity: metric_props.time_granularity.clone(),
                metrics: vec![],
            },
            deprecated_config: MetricConfig {
                enabled: metric_metric_config.enabled,
                tags: metric_metric_config.tags.clone(),
                meta: metric_metric_config.meta.clone(),
                group: metric_metric_config.group.clone(),
            },
            __other__: BTreeMap::new(),
        };

        // Check if metric is enabled (following exposures pattern)
        if metric_metric_config.enabled.unwrap_or(true) {
            metrics.insert(metric_unique_id, Arc::new(dbt_metric));
        } else {
            disabled_metrics.insert(metric_unique_id, Arc::new(dbt_metric));
        }
    }

    Ok((metrics, disabled_metrics))
}

pub fn model_metrics_props_to_metric_aggregation_params(
    semantic_model: &str,
    model: &ModelProperties,
    metric: &MetricsProperties,
) -> Option<MetricAggregationParameters> {
    // agg_params hydrated only for percentile
    let mut agg_params: Option<MeasureAggregationParameters> = None;
    if let Some(percentile) = metric.percentile {
        let use_discrete_percentile =
            matches!(metric.percentile_type, Some(PercentileType::Discrete));
        let use_approximate_percentile =
            matches!(metric.percentile_type, Some(PercentileType::Continuous));
        agg_params = Some(MeasureAggregationParameters {
            percentile: Some(percentile),
            use_discrete_percentile: Some(use_discrete_percentile),
            use_approximate_percentile: Some(use_approximate_percentile),
        });
    }

    let mut agg_time_dimension = metric.agg_time_dimension.clone();
    if agg_time_dimension.is_none() {
        agg_time_dimension = model.agg_time_dimension.clone();
    }

    metric.agg.clone().map(|agg| MetricAggregationParameters {
        semantic_model: semantic_model.to_string(),
        agg: Some(agg),
        agg_params,
        agg_time_dimension,
        non_additive_dimension: metric
            .non_additive_dimension
            .clone()
            .map(NonAdditiveDimension::from),
        expr: metric.expr.clone().map(String::from),
    })
}
