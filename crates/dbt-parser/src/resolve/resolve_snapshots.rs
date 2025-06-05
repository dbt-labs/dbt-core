use dbt_common::constants::DBT_SNAPSHOTS_DIR_NAME;
use dbt_common::error::AbstractLocation;
use dbt_common::{fs_err, show_error, show_warning, stdfs, ErrorCode, FsResult};
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_jinja_utils::refs_and_sources::RefsAndSources;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_schemas::project_configs::ProjectConfigs;
use dbt_schemas::schemas::common::{DbtContract, DbtQuoting, NodeDependsOn};
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::macros::DbtMacro;
use dbt_schemas::schemas::manifest::{CommonAttributes, DbtSnapshot, NodeBaseAttributes};
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::properties::SnapshotProperties;
use dbt_schemas::schemas::ref_and_source::{DbtRef, DbtSourceWrapper};
use dbt_schemas::state::{
    DbtAsset, DbtPackage, DbtRuntimeConfig, ModelStatus, RefsAndSourcesTracker,
};
use minijinja::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::args::ResolveArgs;
use crate::dbt_project_config::{init_project_config, RootProjectConfigs};
use crate::renderer::{render_unresolved_sql_files, SqlFileRenderResult};
use crate::utils::update_node_relation_components;

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
    jinja_env: &JinjaEnvironment<'static>,
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
        package_quoting,
        &package
            .dbt_project
            .snapshots
            .as_ref()
            .map(ProjectConfigs::SnapshotConfigs),
        jinja_env,
        base_ctx,
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
        if macro_uid.starts_with(&format!("snapshot.{}", package_name)) {
            // Write the macro call to the `snapshots` directory
            let macro_call = format!("{{{{ {}() }}}}", macro_node.name);
            let macro_name = macro_node.name.clone();
            let snapshot_name = macro_name
                .strip_prefix("snapshot_")
                .expect("All snapshot macros should start with 'snapshot_'")
                .to_string();
            let target_path =
                PathBuf::from(DBT_SNAPSHOTS_DIR_NAME).join(format!("{}.sql", snapshot_name));
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
                jinja_env,
                base_ctx,
                None,
            )?;

            if let Some(relation) = &snapshot.relation {
                // check if the relation matches the pattern of ref(...)
                let relation = if relation.starts_with("ref(") || relation.starts_with("source(") {
                    format!("{{{{ {} }}}}", relation)
                } else {
                    relation.to_owned()
                };
                // Write SQL for relation to the `snapshots` directory
                let sql = format!("select * from {}", relation);

                let target_path =
                    PathBuf::from(DBT_SNAPSHOTS_DIR_NAME).join(format!("{}.sql", snapshot_name));
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
            let _ = std::mem::replace(&mut mpe.schema_value, dbt_serde_yaml::to_value(snapshot)?);
        }
    }

    // Render the snapshots
    let mut snapshot_sql_resources_map = render_unresolved_sql_files::<SnapshotProperties>(
        arg,
        &snapshot_files,
        &package_name,
        package_quoting,
        adapter_type,
        database,
        schema,
        jinja_env,
        base_ctx,
        &mut snapshot_properties,
        root_project.name.as_str(),
        &root_project_configs.snapshots,
        &local_project_config,
        runtime_config.clone(),
        &package
            .dbt_project
            .snapshot_paths
            .as_ref()
            .unwrap_or(&vec![])
            .clone(),
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

            let unique_id = format!("snapshot.{}.{}", package_name, snapshot_name);

            final_config.enabled = Some(!(status == ModelStatus::Disabled));

            // Create initial snapshot with default values
            let mut dbt_snapshot = DbtSnapshot {
                common_attr: CommonAttributes {
                    database: database.to_owned(), // will be updated below
                    schema: schema.to_owned(),     // will be updated below
                    name: snapshot_name.to_string(),
                    package_name: package_name.clone(),
                    path: dbt_asset.path.clone(),
                    // The path to the YML file, if it is specified
                    original_file_path: dbt_asset.path.clone(),
                    unique_id: unique_id.clone(),
                    fqn: vec![package_name.to_owned(), snapshot_name.to_owned()],
                    description: properties.description.to_owned(),
                    patch_path,
                },
                base_attr: NodeBaseAttributes {
                    alias: "".to_owned(), // will be updated below
                    checksum: sql_file_info.checksum,
                    relation_name: None, // will be updated below
                    build_path: None,
                    unrendered_config: BTreeMap::new(),
                    created_at: None,
                    raw_code: Some("--placeholder--".to_string()), // TODO: This is only so that dbt-evaluator returns truthy
                    columns: properties
                        .columns
                        .as_ref()
                        .map(|c| {
                            c.iter()
                                .map(|cp| cp.clone().try_into())
                                .collect::<Result<Vec<DbtColumn>, _>>()
                        })
                        .transpose()?
                        .map(|c| {
                            c.into_iter()
                                .map(|c| (c.name.clone(), c))
                                .collect::<BTreeMap<_, _>>()
                        })
                        .unwrap_or_default(),
                    depends_on: NodeDependsOn::default(),
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
                    doc_blocks: None,
                    language: Some("sql".to_string()),
                    compiled: None,
                    compiled_path: None,
                    compiled_code: None,
                    extra_ctes_injected: None,
                    extra_ctes: None,
                    contract: DbtContract::default(),
                },
                config: final_config.clone(),
                other: BTreeMap::new(),
            };

            // Update with relation components
            update_node_relation_components(
                &mut dbt_snapshot,
                jinja_env,
                &root_project.name,
                &package_name,
                base_ctx,
                &final_config,
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
