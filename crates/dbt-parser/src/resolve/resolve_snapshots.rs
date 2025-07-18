use dbt_common::constants::DBT_SNAPSHOTS_DIR_NAME;
use dbt_common::error::AbstractLocation;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::{ErrorCode, FsResult, fs_err, show_error, show_warning, stdfs, unexpected_fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::refs_and_sources::RefsAndSources;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_schemas::schemas::common::{DbtMaterialization, DbtQuoting, NodeDependsOn};
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::macros::DbtMacro;
use dbt_schemas::schemas::project::{DbtProject, SnapshotConfig};
use dbt_schemas::schemas::properties::SnapshotProperties;
use dbt_schemas::schemas::ref_and_source::{DbtRef, DbtSourceWrapper};
use dbt_schemas::schemas::{CommonAttributes, DbtSnapshot, DbtSnapshotAttr, NodeBaseAttributes};
use dbt_schemas::state::{
    DbtAsset, DbtPackage, DbtRuntimeConfig, ModelStatus, RefsAndSourcesTracker,
};
use minijinja::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::renderer::{
    RenderCtx, RenderCtxInner, SqlFileRenderResult, render_unresolved_sql_files,
};
use crate::utils::{RelationComponents, update_node_relation_components};

use super::resolve_properties::MinimalPropertiesEntry;

#[allow(clippy::too_many_arguments)]
pub async fn resolve_snapshots(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    mut snapshot_properties: BTreeMap<String, MinimalPropertiesEntry>,
    macros: &BTreeMap<String, DbtMacro>,
    database: &str,
    schema: &str,
    adapter_type: &str,
    jinja_env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    runtime_config: Arc<DbtRuntimeConfig>,
    refs_and_sources: &mut RefsAndSources,
) -> FsResult<(
    HashMap<String, Arc<DbtSnapshot>>,
    HashMap<String, Arc<DbtSnapshot>>,
)> {
    let mut snapshots: HashMap<String, Arc<DbtSnapshot>> = HashMap::new();
    let mut disabled_snapshots: HashMap<String, Arc<DbtSnapshot>> = HashMap::new();

    let local_project_config = init_project_config(
        &arg.io,
        &package.dbt_project.snapshots,
        SnapshotConfig {
            enabled: Some(true),
            quoting: Some(package_quoting),
            ..Default::default()
        },
    )?;
    let package_name = package.dbt_project.name.to_owned();

    // Create the `snapshots` directory
    let snapshots_dir = arg.io.out_dir.join(DBT_SNAPSHOTS_DIR_NAME);
    if !snapshots_dir.exists() {
        stdfs::create_dir_all(&snapshots_dir)?;
    }

    // Save snapshots to the `snapshots` directory
    let mut snapshot_files = Vec::new();
    for (macro_uid, macro_node) in macros {
        if macro_node.package_name == package_name && macro_uid.starts_with("snapshot.") {
            // Write the macro call to the `snapshots` directory
            let macro_call = format!("{{{{ {}() }}}}", macro_node.name);
            let macro_name = macro_node.name.clone();
            let snapshot_name = macro_name
                .strip_prefix("snapshot_")
                .expect("All snapshot macros should start with 'snapshot_'")
                .to_string();
            let target_path =
                PathBuf::from(DBT_SNAPSHOTS_DIR_NAME).join(format!("{snapshot_name}.sql"));
            let snapshot_path = arg.io.out_dir.join(&target_path);
            stdfs::write(snapshot_path, macro_call)?;
            snapshot_files.push(DbtAsset {
                path: target_path,
                package_name: package_name.clone(),
                base_path: arg.io.out_dir.clone(),
            });
        }
    }
    // Save snapshot from yml to the `snapshots` directory
    for (snapshot_name, mpe) in snapshot_properties.iter_mut() {
        // if mpe.schema_value
        if !mpe.schema_value.is_null() {
            let schema_value =
                std::mem::replace(&mut mpe.schema_value, dbt_serde_yaml::Value::null());
            let snapshot: SnapshotProperties = into_typed_with_jinja(
                Some(&arg.io),
                schema_value,
                false,
                &jinja_env,
                base_ctx,
                &[],
            )?;

            if let Some(relation) = &snapshot.relation {
                // check if the relation matches the pattern of ref(...)
                let relation = if relation.starts_with("ref(") || relation.starts_with("source(") {
                    format!("{{{{ {relation} }}}}")
                } else {
                    relation.to_owned()
                };
                // Write SQL for relation to the `snapshots` directory
                let sql = format!("select * from {relation}");

                let target_path =
                    PathBuf::from(DBT_SNAPSHOTS_DIR_NAME).join(format!("{snapshot_name}.sql"));
                let snapshot_path = arg.io.out_dir.join(&target_path);
                stdfs::write(&snapshot_path, &sql)?;
                let asset = DbtAsset {
                    path: target_path.clone(),
                    package_name: package_name.clone(),
                    base_path: arg.io.out_dir.clone(),
                };
                snapshot_files.push(asset.to_owned());
            }
            // Put snapshot back in as it is unused
            let _ = std::mem::replace(
                &mut mpe.schema_value,
                dbt_serde_yaml::to_value(snapshot).map_err(|e| {
                    unexpected_fs_err!("Failed to serialize snapshot properties: {e}")
                })?,
            );
        }
    }

    let render_ctx = RenderCtx {
        inner: Arc::new(RenderCtxInner {
            args: arg.clone(),
            root_project_name: root_project.name.clone(),
            root_project_config: root_project_configs.snapshots.clone(),
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type: adapter_type.to_string(),
            database: database.to_string(),
            schema: schema.to_string(),
            local_project_config,
            resource_paths: package
                .dbt_project
                .snapshot_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        }),
        jinja_env: jinja_env.clone(),
        runtime_config: runtime_config.clone(),
    };

    // Render the snapshots
    let mut snapshot_sql_resources_map = render_unresolved_sql_files::<
        SnapshotConfig,
        SnapshotProperties,
    >(
        &render_ctx, &snapshot_files, &mut snapshot_properties
    )
    .await?;

    // make deterministic
    snapshot_sql_resources_map.sort_by(|a, b| {
        a.asset
            .path
            .file_name()
            .cmp(&b.asset.path.file_name())
            .then(a.asset.path.cmp(&b.asset.path))
    });

    for SqlFileRenderResult {
        asset: dbt_asset,
        sql_file_info,
        macro_spans: _macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
        ..
    } in snapshot_sql_resources_map.into_iter()
    {
        {
            let mut final_config = *sql_file_info.config;
            let database = final_config.database.clone().unwrap_or(database.to_owned());
            let schema = final_config.schema.clone().unwrap_or(schema.to_owned());
            let snapshot_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();

            let properties = if let Some(properties) = maybe_properties {
                properties
            } else {
                SnapshotProperties::empty(snapshot_name.to_owned())
            };

            let unique_id = format!("snapshot.{package_name}.{snapshot_name}");

            final_config.enabled = Some(!(status == ModelStatus::Disabled));

            let columns = process_columns(
                properties.columns.as_ref(),
                final_config.meta.clone(),
                final_config.tags.clone().map(|tags| tags.into()),
            )?;

            if final_config.materialized.is_none() {
                final_config.materialized = Some(DbtMaterialization::Table);
            }

            // Create initial snapshot with default values
            let mut dbt_snapshot = DbtSnapshot {
                common_attr: CommonAttributes {
                    name: snapshot_name.to_string(),
                    package_name: package_name.clone(),
                    path: dbt_asset.path.clone(),
                    raw_code: Some("--placeholder--".to_string()), // TODO: This is only so that dbt-evaluator returns truthy
                    // The path to the YML file, if it is specified
                    original_file_path: dbt_asset.path.clone(),
                    unique_id: unique_id.clone(),
                    fqn: vec![package_name.to_owned(), snapshot_name.to_owned()],
                    description: properties.description.to_owned(),
                    patch_path,
                    checksum: sql_file_info.checksum,
                    language: Some("sql".to_string()),
                    tags: final_config
                        .tags
                        .clone()
                        .map(|tags| tags.into())
                        .unwrap_or_default(),
                    meta: final_config.meta.clone().unwrap_or_default(),
                },
                base_attr: NodeBaseAttributes {
                    database: database.to_owned(), // will be updated below
                    schema: schema.to_owned(),     // will be updated below
                    alias: "".to_owned(),          // will be updated below
                    relation_name: None,           // will be updated below
                    columns,
                    depends_on: NodeDependsOn::default(),
                    enabled: final_config.enabled.unwrap_or(true),
                    extended_model: false,
                    materialized: final_config
                        .materialized
                        .clone()
                        .expect("materialized is required"),
                    quoting: final_config
                        .quoting
                        .expect("quoting is required")
                        .try_into()
                        .expect("quoting is required"),
                    static_analysis: final_config
                        .static_analysis
                        .unwrap_or(StaticAnalysisKind::On),
                    refs: sql_file_info
                        .refs
                        .iter()
                        .map(|(model, project, version, location)| DbtRef {
                            name: model.to_owned(),
                            package: project.to_owned(),
                            version: version.clone().map(|v| v.into()),
                            location: Some(location.with_file(&dbt_asset.path)),
                        })
                        .collect(),
                    sources: sql_file_info
                        .sources
                        .iter()
                        .map(|(source, table, location)| DbtSourceWrapper {
                            source: vec![source.to_owned(), table.to_owned()],
                            location: Some(location.with_file(&dbt_asset.path)),
                        })
                        .collect(),
                    metrics: vec![],
                },
                snapshot_attr: DbtSnapshotAttr {
                    snapshot_meta_column_names: final_config
                        .snapshot_meta_column_names
                        .clone()
                        .unwrap_or_default(),
                },
                deprecated_config: final_config.clone(),
                compiled: None,
                compiled_code: None,
                other: BTreeMap::new(),
            };

            let components = RelationComponents {
                database: final_config.database.clone(),
                schema: final_config.schema.clone(),
                alias: final_config.alias.clone(),
                store_failures: None,
            };

            // Update with relation components
            update_node_relation_components(
                &mut dbt_snapshot,
                &jinja_env,
                &root_project.name,
                &package_name,
                base_ctx,
                &components,
                adapter_type,
            )?;
            match refs_and_sources.insert_ref(&dbt_snapshot, adapter_type, status, false) {
                Ok(_) => (),
                Err(e) => {
                    show_error!(&arg.io, e.with_location(dbt_asset.path.clone()));
                }
            }

            match status {
                ModelStatus::Enabled => {
                    snapshots.insert(unique_id, Arc::new(dbt_snapshot));
                }
                ModelStatus::Disabled => {
                    disabled_snapshots.insert(unique_id, Arc::new(dbt_snapshot));
                }
                ModelStatus::ParsingFailed => {}
            }
        }
    }
    for (snapshot_name, mpe) in snapshot_properties.iter() {
        // Skip until we support better error messages for versioned models
        if mpe.version_info.is_some() {
            continue;
        }
        if !mpe.schema_value.is_null() {
            // Validate that the model is not latest and flattened
            let err = fs_err!(
                code => ErrorCode::InvalidConfig,
                loc => mpe.relative_path.clone(),
                "Unused schema.yml entry for snapshot '{}'",
                snapshot_name,
            );
            show_warning!(&arg.io, err);
        }
    }

    Ok((snapshots, disabled_snapshots))
}
