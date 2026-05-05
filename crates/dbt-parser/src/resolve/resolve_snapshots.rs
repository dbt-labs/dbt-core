use super::resolve_properties::MinimalPropertiesEntry;
use crate::args::ResolveArgs;
use crate::dbt_project_config::{
    ProjectConfigResolver, RootProjectConfigs, init_project_config,
    strip_resource_paths_from_ref_path,
};
use crate::renderer::{
    RenderCtx, RenderCtxInner, SqlFileRenderResult, collect_adapter_identifiers_detect_unsafe,
    render_unresolved_sql_files,
};
use crate::resolve::resolve_tests::persist_generic_data_tests::TestableNodeTrait;
use crate::resolve::resolve_utils::err_resource_name_has_spaces;
use crate::resolve::yaml_field_utils;
use crate::sql_file_info::SqlFileInfo;
use crate::utils::{
    RelationComponents, get_node_fqn, get_original_file_path, update_node_relation_components,
};
use crate::validation::check_node_static_analysis;
use dbt_adapter_core::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::constants::DBT_SNAPSHOTS_DIR_NAME;
use dbt_common::error::AbstractLocation;
use dbt_common::io_args::{StaticAnalysisKind, StaticAnalysisOffReason};
use dbt_common::tokiofs;
use dbt_common::tracing::emit::{emit_error_log_from_fs_error, emit_warn_log_from_fs_error};
use dbt_common::{ErrorCode, FsResult, fs_err, stdfs, unexpected_fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
use dbt_jinja_utils::node_resolver::NodeResolver;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_schemas::schemas::common::{
    DbtChecksum, DbtQuoting, NodeDependsOn, conform_normalized_snapshot_raw_code_to_mantle_format,
    normalize_sql,
};
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::macros::DbtMacro;
use dbt_schemas::schemas::nodes::AdapterAttr;
use dbt_schemas::schemas::project::{DbtProject, SnapshotConfig};
use dbt_schemas::schemas::properties::SnapshotProperties;
use dbt_schemas::schemas::ref_and_source::{DbtRef, DbtSourceWrapper};
use dbt_schemas::schemas::{
    CommonAttributes, DbtSnapshot, DbtSnapshotAttr, IntrospectionKind, NodeBaseAttributes,
};
use dbt_schemas::state::{
    DbtAsset, DbtPackage, DbtRuntimeConfig, GenericTestAsset, ModelStatus, NodeResolverTracker,
};
use minijinja::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[allow(clippy::too_many_arguments, clippy::cognitive_complexity)]
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
    adapter_type: AdapterType,
    jinja_env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    runtime_config: Arc<DbtRuntimeConfig>,
    node_resolver: &mut NodeResolver,
    collected_generic_tests: &mut Vec<GenericTestAsset>,
    test_name_truncations: &mut HashMap<String, String>,
    token: &CancellationToken,
) -> FsResult<(
    HashMap<String, Arc<DbtSnapshot>>,
    HashMap<String, Arc<DbtSnapshot>>,
)> {
    let mut snapshots: HashMap<String, Arc<DbtSnapshot>> = HashMap::new();
    let mut disabled_snapshots: HashMap<String, Arc<DbtSnapshot>> = HashMap::new();
    let jinja_type_checking_event_listener_factory =
        Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default());
    let mut snapshots_with_execute: HashMap<String, DbtSnapshot> = HashMap::new();

    let dependency_package_name = if package.dbt_project.name != root_project.name {
        Some(package.dbt_project.name.as_str())
    } else {
        None
    };

    let package_name = package.dbt_project.name.to_owned();

    // Create the `snapshots` directory
    let snapshots_dir = arg.io.out_dir.join(DBT_SNAPSHOTS_DIR_NAME);
    if !snapshots_dir.exists() {
        stdfs::create_dir_all(&snapshots_dir)?;
    }

    // Save snapshots to the `snapshots` directory
    let mut snapshot_files = Vec::new();
    let mut sql_defined_snapshots = Vec::new();
    // Map target path to original macro path for checksum recalculation
    let mut snapshot_original_paths: HashMap<PathBuf, PathBuf> = HashMap::new();
    let default_snapshots_path = vec![DBT_SNAPSHOTS_DIR_NAME.to_string()];
    for (macro_uid, macro_node) in macros {
        if macro_node.package_name == package_name && macro_uid.starts_with("snapshot.") {
            // Write the macro call to the `snapshots` directory
            let macro_call = format!("{{{{ {}() }}}}", macro_node.name);
            let macro_name = macro_node.name.clone();
            let snapshot_name = macro_name
                .strip_prefix("snapshot_")
                .expect("All snapshot macros should start with 'snapshot_'")
                .to_string();

            // Preserve file layout for proper fqn generation
            let original_relative_path = strip_resource_paths_from_ref_path(
                &macro_node.path,
                package
                    .dbt_project
                    .snapshot_paths
                    .as_ref()
                    .unwrap_or(&default_snapshots_path),
            );

            let target_path = PathBuf::from(DBT_SNAPSHOTS_DIR_NAME)
                .join(original_relative_path.with_file_name(format!("{snapshot_name}.sql")));
            let snapshot_path = arg.io.out_dir.join(&target_path);
            if let Some(parent) = snapshot_path.parent() {
                stdfs::create_dir_all(parent)?;
            }
            stdfs::write(snapshot_path, macro_call)?;

            // Track original path for checksum recalculation
            snapshot_original_paths
                .insert(target_path.clone(), macro_node.original_file_path.clone());

            snapshot_files.push(DbtAsset {
                path: target_path.clone(),
                original_path: macro_node.original_file_path.clone(),
                package_name: package_name.clone(),
                base_path: arg.io.out_dir.clone(),
            });
            sql_defined_snapshots.push(target_path);
        }
    }

    // Save snapshot from yml to the `snapshots` directory
    for (snapshot_name, mpe) in snapshot_properties.iter_mut() {
        // if mpe.schema_value
        if !mpe.schema_value.is_null() {
            let mut schema_value =
                std::mem::replace(&mut mpe.schema_value, dbt_yaml::Value::null());
            // `description:` is documentation text and may contain Jinja-like
            // doc snippets the renderer cannot evaluate at parse time
            // (e.g. `{% raw %}{{ ref('x') }}{% endraw %}`). dbt-core never
            // renders descriptions; detach before `into_typed_with_jinja`
            // so the renderer doesn't see the string, then reattach the raw
            // value to the typed struct. Other fields (config, meta, …)
            // still go through Jinja as before.
            let raw_description =
                yaml_field_utils::detach_field_from_mapping(&mut schema_value, "description");
            let mut snapshot: SnapshotProperties = into_typed_with_jinja(
                &arg.io,
                schema_value,
                false,
                &jinja_env,
                base_ctx,
                &[],
                dependency_package_name,
                true,
            )?;
            if let Some(desc_value) = raw_description
                && let Some(s) = desc_value.as_str()
            {
                snapshot.description = Some(s.to_string());
            }

            if let Some(relation) = &snapshot.relation {
                // check if the relation matches the pattern of ref(...)
                let relation = if relation.starts_with("ref(") || relation.starts_with("source(") {
                    format!("{{{{ {relation} }}}}")
                } else {
                    relation.to_owned()
                };
                // Write SQL for relation to the `snapshots` directory
                let sql = format!("select * from {relation}");

                // Preserve directory structure from the properties file path
                // This ensures FQN includes directory components for proper selector matching
                let default_snapshots_path = vec![DBT_SNAPSHOTS_DIR_NAME.to_string()];
                let original_relative_path = strip_resource_paths_from_ref_path(
                    &mpe.relative_path,
                    package
                        .dbt_project
                        .snapshot_paths
                        .as_ref()
                        .unwrap_or(&default_snapshots_path),
                );
                let target_path = PathBuf::from(DBT_SNAPSHOTS_DIR_NAME)
                    .join(
                        original_relative_path
                            .parent()
                            .unwrap_or_else(|| std::path::Path::new("")),
                    )
                    .join(format!("{snapshot_name}.sql"));
                let snapshot_path = arg.io.out_dir.join(&target_path);
                if let Some(parent) = snapshot_path.parent() {
                    stdfs::create_dir_all(parent)?;
                }
                stdfs::write(&snapshot_path, &sql)?;
                // Compute the original file path relative to in_dir
                // For package YAML snapshots, this includes the package path
                let original_path = get_original_file_path(
                    &package.package_root_path,
                    &arg.io.in_dir,
                    &mpe.relative_path,
                );
                let asset = DbtAsset {
                    path: target_path.clone(),
                    original_path,
                    package_name: package_name.clone(),
                    base_path: arg.io.out_dir.clone(),
                };
                snapshot_files.push(asset.to_owned());
            }
            // Put snapshot back in as it is unused
            let _ = std::mem::replace(
                &mut mpe.schema_value,
                dbt_yaml::to_value(snapshot).map_err(|e| {
                    unexpected_fs_err!("Failed to serialize snapshot properties: {e}")
                })?,
            );
        }
    }

    let config_resolver = ProjectConfigResolver::build(
        root_project_configs.snapshots.clone(),
        dependency_package_name.is_some(),
        || {
            init_project_config(
                &arg.io,
                &package.dbt_project.snapshots,
                package_quoting,
                dependency_package_name,
            )
        },
    )?
    .with_resolve_defaults((
        arg.static_analysis.unwrap_or_default(),
        root_project.sync.clone(),
    ));

    let render_ctx = RenderCtx {
        inner: Arc::new(RenderCtxInner {
            args: arg.clone(),
            root_project_name: root_project.name.clone(),
            config_resolver,
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type,
            database: database.to_string(),
            schema: schema.to_string(),
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
    let mut snapshot_sql_resources_map =
        render_unresolved_sql_files::<SnapshotConfig, SnapshotProperties>(
            &render_ctx,
            &snapshot_files,
            &mut snapshot_properties,
            token,
            jinja_type_checking_event_listener_factory.clone(),
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

    let all_depends_on = jinja_type_checking_event_listener_factory
        .depends_on()
        .clone();

    for SqlFileRenderResult {
        asset: dbt_asset,
        sql_file_info,
        config: snapshot_config,
        macro_spans: _macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
        ..
    } in snapshot_sql_resources_map.into_iter()
    {
        {
            let snapshot_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
            if snapshot_name.contains(' ') {
                return Err(err_resource_name_has_spaces(snapshot_name, &dbt_asset.path));
            }

            // Recalculate checksum from original snapshot file.
            // Without doing this, the checksum will be different from the one from mantle since fusion
            // creates a new file for the snapshot that only contains a function call
            // to the original macro instead of the original macro itself.
            let recalculated_checksum =
                if let Some(original_path) = snapshot_original_paths.get(&dbt_asset.path) {
                    recalculate_snapshot_checksum(arg, original_path, &sql_file_info).await
                } else {
                    // Not a macro-based snapshot, use the checksum from sql_file_info
                    sql_file_info.checksum.clone()
                };

            let properties = if let Some(properties) = maybe_properties {
                properties
            } else {
                SnapshotProperties::empty(snapshot_name.to_owned())
            };

            let unique_id = format!("snapshot.{package_name}.{snapshot_name}");

            let columns = process_columns(
                properties.columns.as_ref(),
                snapshot_config.meta.clone(),
                snapshot_config.tags.clone().map(|tags| tags.into()),
            )?;

            let fqn = get_node_fqn(
                &package_name,
                dbt_asset.path.clone(),
                vec![snapshot_name.to_string()],
                package
                    .dbt_project
                    .snapshot_paths
                    .as_ref()
                    .unwrap_or(&vec![]),
            );

            let static_analysis = snapshot_config.static_analysis.clone();
            check_node_static_analysis(
                &snapshot_config,
                arg.static_analysis,
                &unique_id,
                dependency_package_name,
                arg.io.status_reporter.as_ref(),
            );

            let macro_depends_on = all_depends_on
                .get(&format!("{package_name}.{snapshot_name}"))
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();

            // Create initial snapshot with default values
            let mut dbt_snapshot = DbtSnapshot {
                __common_attr__: CommonAttributes {
                    name: snapshot_name.to_string(),
                    package_name: package_name.clone(),
                    path: dbt_asset.path.clone(),
                    name_span: dbt_common::Span::default(),
                    // NOTE: raw_code has to be this value for dbt-evaluator to return truthy
                    // hydrating it with get_original_file_contents would actually break dbt-evaluator
                    raw_code: Some("--placeholder--".to_string()),
                    // The original file path where the snapshot was defined
                    // For package snapshots, this includes the package path (e.g., dbt_packages/my_pkg/snapshots/foo.sql)
                    original_file_path: dbt_asset.original_path.clone(),
                    unique_id: unique_id.clone(),
                    fqn,
                    description: properties.description.to_owned(),
                    patch_path: patch_path.clone(),
                    checksum: recalculated_checksum,
                    language: Some("sql".to_string()),
                    tags: snapshot_config
                        .tags
                        .clone()
                        .map(|tags| tags.into())
                        .unwrap_or_default(),
                    meta: snapshot_config.meta.clone().unwrap_or_default(),
                },
                __base_attr__: NodeBaseAttributes {
                    database: "".to_owned(), // will be updated below
                    schema: "".to_owned(),   // will be updated below
                    alias: "".to_owned(),    // will be updated below
                    relation_name: None,     // will be updated below
                    columns,
                    depends_on: NodeDependsOn {
                        macros: macro_depends_on,
                        nodes: vec![],
                        nodes_with_ref_location: vec![],
                    },
                    enabled: snapshot_config.enabled,
                    extended_model: false,
                    persist_docs: snapshot_config.persist_docs.clone(),
                    materialized: snapshot_config.materialized.clone(),
                    quoting: snapshot_config
                        .quoting
                        .try_into()
                        .expect("DbtQuoting -> ResolvedQuoting conversion"),
                    quoting_ignore_case: snapshot_config
                        .quoting
                        .snowflake_ignore_case
                        .unwrap_or(false),
                    static_analysis_off_reason: (*static_analysis == StaticAnalysisKind::Off)
                        .then_some(StaticAnalysisOffReason::ConfiguredOff),
                    static_analysis,
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
                    unrendered_config: Default::default(),
                    functions: sql_file_info
                        .functions
                        .iter()
                        .map(|(function_name, package, location)| DbtRef {
                            name: function_name.to_owned(),
                            package: package.to_owned(),
                            version: None, // Functions don't have versions
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
                __snapshot_attr__: DbtSnapshotAttr {
                    snapshot_meta_column_names: snapshot_config
                        .snapshot_meta_column_names
                        .clone()
                        .unwrap_or_default(),
                    introspection: if sql_file_info.this {
                        IntrospectionKind::This
                    } else {
                        IntrospectionKind::None
                    },
                    sync: snapshot_config.sync.clone(),
                },
                __adapter_attr__: AdapterAttr::from_config_and_dialect(
                    &snapshot_config.__warehouse_specific_config__,
                    adapter_type,
                ),
                deprecated_config: snapshot_config.clone().into(),
                compiled: None,
                compiled_code: None,
                __other__: BTreeMap::new(),
            };

            let components = RelationComponents {
                // For backwards compatibility with target_schema and target_database configs
                database: if snapshot_config.target_database.is_some() {
                    snapshot_config.target_database.clone()
                } else {
                    snapshot_config.database.clone()
                },
                schema: if snapshot_config.target_schema.is_some() {
                    snapshot_config.target_schema.clone()
                } else {
                    snapshot_config.schema.clone()
                },
                alias: snapshot_config.alias.clone(),
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

            match node_resolver.insert_ref(&dbt_snapshot, adapter_type, status, false) {
                Ok(_) => (),
                Err(e) => {
                    let err_with_loc = e.with_location(dbt_asset.path.clone());
                    emit_error_log_from_fs_error(&err_with_loc, arg.io.status_reporter.as_ref());
                }
            }

            match status {
                ModelStatus::Enabled => {
                    if snapshot_config.unique_key.is_none() || snapshot_config.strategy.is_none() {
                        let e = fs_err!(
                            code => ErrorCode::InvalidConfig,
                            loc => dbt_asset.path.clone(),
                            "Snapshot '{}' must be configured with a 'strategy' and 'unique_key'",
                            snapshot_name
                        );
                        emit_error_log_from_fs_error(&e, arg.io.status_reporter.as_ref());
                    }
                    if sql_file_info.execute && sql_defined_snapshots.contains(&dbt_asset.path) {
                        snapshots_with_execute.insert(unique_id.to_owned(), dbt_snapshot);
                    } else {
                        snapshots.insert(unique_id, Arc::new(dbt_snapshot));
                    }

                    if !arg.skip_creating_generic_tests {
                        properties.as_testable().persist(
                            package_name.as_str(),
                            &root_project.name,
                            collected_generic_tests,
                            test_name_truncations,
                            adapter_type,
                            &arg.io,
                            patch_path.as_ref().unwrap_or(&dbt_asset.path),
                        )?;
                    }
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
                code => ErrorCode::NoNodeForYamlKey,
                loc => mpe.relative_path.clone(),
                "Unused schema.yml entry for snapshot '{}'",
                snapshot_name,
            );
            emit_warn_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
        }
    }
    // Second pass to capture all identifiers with the appropriate context
    // `models_with_execute` should never have overlapping Arc pointers with `models` and `disabled_models`
    // otherwise make_mut will clone the inner model, and the modifications inside this function call will be lost
    let snapshots_rest = collect_adapter_identifiers_detect_unsafe(
        arg,
        snapshots_with_execute,
        node_resolver,
        jinja_env,
        adapter_type,
        package.dbt_project.name.as_str(),
        &root_project.name,
        runtime_config,
        token,
    )
    .await?;

    snapshots.extend(
        snapshots_rest
            .into_iter()
            .map(|(k, _)| (k.__common_attr__.unique_id.clone(), Arc::new(k))),
    );

    Ok((snapshots, disabled_snapshots))
}

async fn recalculate_snapshot_checksum(
    arg: &ResolveArgs,
    original_path: &PathBuf,
    sql_file_info: &SqlFileInfo<SnapshotConfig>,
) -> DbtChecksum {
    // Read original snapshot file
    let original_absolute_path = arg.io.in_dir.join(original_path);
    match tokiofs::read_to_string(&original_absolute_path).await {
        Ok(original_sql) => {
            // First normalize: remove all whitespace and lowercase
            let normalized_full = normalize_sql(&original_sql);

            let normalized_sql =
                conform_normalized_snapshot_raw_code_to_mantle_format(&normalized_full);
            DbtChecksum::hash(normalized_sql.as_bytes())
        }
        Err(e) => {
            // Fallback to sql_file_info checksum if original file can't be read
            emit_warn_log_from_fs_error(&e, arg.io.status_reporter.as_ref());
            sql_file_info.checksum.clone()
        }
    }
}
