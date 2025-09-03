use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::utils::{get_node_fqn, get_original_file_path, get_unique_id};

use dbt_common::{ErrorCode, FsResult, fs_err, show_error};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::CommonAttributes;
use dbt_schemas::schemas::common::{DbtChecksum, NodeDependsOn};
use dbt_schemas::schemas::manifest::saved_query::{
    self, DbtSavedQuery, DbtSavedQueryAttr, SavedQueryExportConfig, SavedQueryParams,
};
use dbt_schemas::schemas::project::{DefaultTo, SavedQueryConfig};
use dbt_schemas::schemas::properties::SavedQueriesProperties;
use dbt_schemas::state::DbtPackage;
use minijinja::value::Value as MinijinjaValue;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;

#[allow(clippy::too_many_arguments)]
pub async fn resolve_saved_queries(
    arg: &ResolveArgs,
    package: &DbtPackage,
    _root_package_name: &str,
    root_project_configs: &RootProjectConfigs,
    saved_query_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    database: &str,
    schema: &str,
    package_name: &str,
    env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
) -> FsResult<(
    HashMap<String, Arc<DbtSavedQuery>>,
    HashMap<String, Arc<DbtSavedQuery>>,
)> {
    let mut saved_queries: HashMap<String, Arc<DbtSavedQuery>> = HashMap::new();
    let mut disabled_saved_queries: HashMap<String, Arc<DbtSavedQuery>> = HashMap::new();

    // Return early if no saved queries to process
    if saved_query_properties.is_empty() {
        return Ok((saved_queries, disabled_saved_queries));
    }

    let dependency_package_name = dependency_package_name_from_ctx(&env, base_ctx);
    let local_project_config = init_project_config(
        &arg.io,
        &package.dbt_project.saved_queries,
        SavedQueryConfig {
            enabled: Some(true),
            ..Default::default()
        },
        dependency_package_name,
    )?;

    // Validate saved query names with regex (similar to exposures)
    let saved_query_name_re = Regex::new(r"[\w-]+$").unwrap();

    for (saved_query_name, mpe) in saved_query_properties.iter_mut() {
        if !mpe.schema_value.is_null() {
            // Validate saved query name
            if !saved_query_name_re.is_match(saved_query_name) {
                let e = fs_err!(
                    code => ErrorCode::InvalidConfig,
                    loc => mpe.relative_path.clone(),
                    "Saved query name '{}' can only contain letters, numbers, and underscores.",
                    saved_query_name
                );
                show_error!(&arg.io, e);
                continue;
            }

            let unique_id = get_unique_id(saved_query_name, package_name, None, "saved_query");
            let fqn = get_node_fqn(
                package_name,
                mpe.relative_path.clone(),
                vec![saved_query_name.to_owned()],
                &package.dbt_project.all_source_paths(),
            );

            let schema_value =
                std::mem::replace(&mut mpe.schema_value, dbt_serde_yaml::Value::null());

            // Parse the saved query properties from YAML
            let saved_query_props: SavedQueriesProperties = into_typed_with_jinja(
                &arg.io,
                schema_value,
                false,
                &env,
                base_ctx,
                &[],
                dependency_package_name,
            )?;

            // Get combined config from project config and saved query config
            let global_config = local_project_config.get_config_for_fqn(&fqn);
            let mut project_config = root_project_configs
                .saved_queries
                .get_config_for_fqn(&fqn)
                .clone();
            project_config.default_to(global_config);

            let saved_query_config = if let Some(config) = &saved_query_props.config {
                let mut final_config = config.clone();
                final_config.default_to(&project_config);
                final_config
            } else {
                project_config.clone()
            };

            let props_query_params = &saved_query_props.query_params;

            // Create default query params and exports since we're doing minimal implementation
            let query_params = SavedQueryParams {
                metrics: props_query_params.metrics.clone().unwrap_or_default(),
                group_by: props_query_params.group_by.clone().unwrap_or_default(),
                where_: props_query_params
                    .where_
                    .clone()
                    .map(|where_clause| where_clause.into()),
                order_by: vec![],
                limit: None, // TODO: populate from saved_query_props when implementing full functionality
            };

            let exports = saved_query_props
                .exports
                .unwrap_or_default()
                .iter()
                .map(|export| {
                    let config = export.config.clone().unwrap_or_default();

                    saved_query::SavedQueryExport {
                        name: export.name.clone(),
                        config: SavedQueryExportConfig {
                            export_as: config.export_as.unwrap_or_default(),
                            schema_name: Some(config.schema.unwrap_or(schema.to_string())), // TODO: verify
                            alias: Some(config.alias.unwrap_or(export.name.clone())),
                            database: Some(database.to_string()), // TODO: verify
                        },
                        unrendered_config: BTreeMap::new(), // TODO
                    }
                })
                .collect::<Vec<saved_query::SavedQueryExport>>();

            let dbt_saved_query = DbtSavedQuery {
                __common_attr__: CommonAttributes {
                    name: saved_query_name.clone(),
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
                    unique_id: unique_id.clone(),
                    fqn,
                    description: saved_query_props.description,
                    checksum: DbtChecksum::default(),
                    raw_code: None,
                    language: None,
                    tags: saved_query_config
                        .tags
                        .clone()
                        .map(|tags| tags.into())
                        .unwrap_or_default(),
                    meta: saved_query_config.meta.clone().unwrap_or_default(),
                },
                __saved_query_attr__: DbtSavedQueryAttr {
                    query_params,
                    exports,
                    label: saved_query_props.label,
                    metadata: None, // TODO: populate when implementing full functionality
                    unrendered_config: BTreeMap::new(),
                    depends_on: NodeDependsOn::default(),
                    // TODO: Set refs when implementing full functionality
                    refs: vec![], // TODO
                    group: saved_query_config.group.clone(),
                    created_at: chrono::Utc::now().timestamp() as f64,
                },
                deprecated_config: saved_query_config.clone(),
                __other__: BTreeMap::new(),
            };

            // Check if saved query is enabled (following exposures pattern)
            if saved_query_config.enabled.unwrap_or(true) {
                saved_queries.insert(unique_id, Arc::new(dbt_saved_query));
            } else {
                disabled_saved_queries.insert(unique_id, Arc::new(dbt_saved_query));
            }
        }
    }

    Ok((saved_queries, disabled_saved_queries))
}
