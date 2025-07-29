use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::utils::get_node_fqn;
use dbt_common::error::AbstractLocation;
use dbt_common::{ErrorCode, FsResult, err, fs_err, show_error};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::parse::build_resolve_model_context;
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_jinja_utils::utils::render_extract_ref_or_source_expr;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::nodes::{
    CommonAttributes, DbtExposure, DbtExposureAttr, NodeBaseAttributes,
};
use dbt_schemas::schemas::project::{DbtProject, DefaultTo, ExposureConfig};
use dbt_schemas::schemas::properties::ExposureProperties;
use dbt_schemas::schemas::ref_and_source::{DbtRef, DbtSourceWrapper};
use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
use dbt_schemas::state::{DbtPackage, DbtRuntimeConfig};
use minijinja::value::Value as MinijinjaValue;
use regex::Regex;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use super::resolve_properties::MinimalPropertiesEntry;

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub async fn resolve_exposures(
    args: &ResolveArgs,
    exposure_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    package: &DbtPackage,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    database: &str,
    schema: &str,
    adapter_type: &str,
    package_name: &str,
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
) -> FsResult<(
    HashMap<String, Arc<DbtExposure>>,
    HashMap<String, Arc<DbtExposure>>,
)> {
    let mut exposures: HashMap<String, Arc<DbtExposure>> = HashMap::new();
    let mut disabled_exposures: HashMap<String, Arc<DbtExposure>> = HashMap::new();
    let local_project_config = init_project_config(
        &args.io,
        &package.dbt_project.exposures,
        ExposureConfig {
            enabled: Some(true),
            ..Default::default()
        },
    )?;

    // Retrieve exposures from yaml
    let exposure_name_re = Regex::new(r"[\w-]+$").unwrap();
    for (exposure_name, mpe) in exposure_properties.iter_mut() {
        if !mpe.schema_value.is_null() {
            // validate dbt_exposure name
            if !exposure_name_re.is_match(exposure_name) {
                let e = fs_err!(
                    code => ErrorCode::InvalidConfig,
                    loc => mpe.relative_path.clone(),
                    "Exposure name '{}' can only contain letters, numbers, and underscores.",
                    exposure_name
                );
                show_error!(&args.io, e);
            }

            let unique_id = format!("exposure.{}.{}", &package_name, exposure_name);
            let fqn = get_node_fqn(
                package_name,
                mpe.relative_path.clone(),
                vec![exposure_name.to_owned()],
            );

            let schema_value =
                std::mem::replace(&mut mpe.schema_value, dbt_serde_yaml::Value::null());
            // ExposureProperties is for the yaml schema
            let exposure: ExposureProperties =
                into_typed_with_jinja(Some(&args.io), schema_value, false, env, base_ctx, &[])?;

            // Get combined properties
            let global_config = local_project_config.get_config_for_fqn(&fqn);
            let mut project_config = root_project_configs
                .exposures
                .get_config_for_fqn(&fqn)
                .clone();
            project_config.default_to(global_config);

            let exposure_properties_config = if let Some(properties) = &exposure.config {
                let mut properties_config: ExposureConfig = properties.clone();
                properties_config.default_to(&project_config);
                properties_config
            } else {
                project_config
            };

            //      depends_on:
            //        - ref('model')
            //        - source('test_source', 'test_table')
            //        - metric('metric')

            // Extract refs, sources & metrics from yaml depends_on
            let (refs, sources, metrics) = if let Some(depends_on) = &exposure.depends_on {
                resolve_yaml_depends_on(
                    depends_on,
                    env,
                    base_ctx,
                    &exposure_properties_config,
                    database,
                    schema,
                    adapter_type,
                    package_name,
                    &root_project.name,
                    fqn.clone(),
                    &mpe.relative_path.to_string_lossy(),
                )?
            } else {
                (vec![], vec![], vec![])
            };

            let dbt_exposure = DbtExposure {
                common_attr: CommonAttributes {
                    name: exposure_name.to_string(),
                    package_name: package_name.to_string(),
                    path: mpe.relative_path.clone(),
                    original_file_path: mpe.relative_path.clone(),
                    unique_id: unique_id.clone(),
                    fqn,
                    description: Some(exposure.description.unwrap_or_default()),
                    patch_path: None,
                    checksum: Default::default(),
                    language: None,
                    raw_code: None,
                    tags: exposure_properties_config
                        .tags
                        .clone()
                        .map(|tags| tags.into())
                        .unwrap_or_default(),
                    meta: exposure_properties_config.meta.clone().unwrap_or_default(),
                },
                base_attr: NodeBaseAttributes {
                    database: "".to_string(),
                    schema: "".to_string(),
                    alias: "".to_string(),
                    relation_name: None,
                    quoting: Default::default(),
                    materialized: Default::default(),
                    static_analysis: Default::default(),
                    enabled: true,
                    extended_model: false,
                    columns: BTreeMap::new(),
                    refs,
                    sources,
                    metrics,
                    depends_on: NodeDependsOn::default(),
                    quoting_ignore_case: false,
                },
                exposure_attr: DbtExposureAttr {
                    owner: exposure.owner,
                    label: exposure.label.to_owned(),
                    maturity: exposure.maturity.to_owned(),
                    type_: exposure.type_.to_owned(),
                    url: exposure.url,
                    unrendered_config: BTreeMap::new(),
                    created_at: Default::default(),
                },
                deprecated_config: exposure_properties_config.clone(),
            };

            // Check if exposure is enabled, add to appropriate collection
            if exposure_properties_config.enabled.unwrap_or(true) {
                exposures.insert(unique_id, Arc::new(dbt_exposure));
            } else {
                disabled_exposures.insert(unique_id, Arc::new(dbt_exposure));
            }
        }
    }

    Ok((exposures, disabled_exposures))
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub fn resolve_yaml_depends_on(
    depends_on: &[String],
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    exposure_config: &ExposureConfig,
    database: &str,
    schema: &str,
    adapter_type: &str,
    package_name: &str,
    root_project_name: &str,
    fqn: Vec<String>,
    relative_path: &str,
) -> FsResult<(Vec<DbtRef>, Vec<DbtSourceWrapper>, Vec<Vec<String>>)> {
    let mut dependent_refs = vec![];
    let mut dependent_sources = vec![];
    let mut dependent_metrics = vec![];

    // Process each dependency in the depends_on list
    for dependency in depends_on {
        let sql_resources: Arc<Mutex<Vec<SqlResource<ExposureConfig>>>> =
            Arc::new(Mutex::new(Vec::new()));

        let mut resolve_model_context = base_ctx.clone();
        resolve_model_context.extend(build_resolve_model_context(
            exposure_config,
            adapter_type,
            database,
            schema,
            &fqn.join("."),
            fqn.clone(),
            package_name,
            root_project_name,
            DEFAULT_DBT_QUOTING,                   // package_quoting
            Arc::new(DbtRuntimeConfig::default()), // runtime_config
            sql_resources.clone(),
            Arc::new(AtomicBool::new(false)),
        ));

        let sql_resource = render_extract_ref_or_source_expr(
            env,
            &resolve_model_context,
            sql_resources.clone(),
            dependency,
        )?;

        match sql_resource {
            SqlResource::Ref(ref_info) => {
                dependent_refs.push(DbtRef {
                    name: ref_info.0,
                    package: ref_info.1,
                    version: ref_info.2.map(|v| v.into()),
                    location: Some(ref_info.3.with_file(relative_path)),
                });
            }
            SqlResource::Source(source_info) => {
                dependent_sources.push(DbtSourceWrapper {
                    source: vec![source_info.0, source_info.1],
                    location: Some(source_info.2.with_file(relative_path)),
                });
            }
            SqlResource::Metric(metric_info) => {
                dependent_metrics.push(vec![metric_info.0]);
            }
            _ => {
                return err!(
                    ErrorCode::Unexpected,
                    "Invalid dependency input: {}",
                    dependency
                );
            }
        }
    }

    Ok((dependent_refs, dependent_sources, dependent_metrics))
}
