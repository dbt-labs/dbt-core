//! Utility functions for the resolver
use crate::dbt_project_config::DbtProjectConfig;
use crate::resolve::resolve_properties::MinimalPropertiesEntry;
use crate::sql_file_info::SqlFileInfo;
use crate::utils::{get_node_fqn, register_duplicate_resource, trigger_duplicate_errors};
use dbt_common::constants::PARSING;
use dbt_common::tokiofs::read_to_string;
use dbt_common::{
    fs_err, show_error, show_progress, show_warning_soon_to_be_error, ErrorCode, FsError, FsResult,
};
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_jinja_utils::phases::build_compile_and_run_base_context;
use dbt_jinja_utils::phases::compile::build_compile_node_context;
use dbt_jinja_utils::phases::parse::build_resolve_model_context;
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_jinja_utils::refs_and_sources::RefsAndSources;
use dbt_jinja_utils::serde::into_typed_with_jinja_error;
use dbt_jinja_utils::silence_base_context;
use dbt_jinja_utils::utils::render_sql;
use dbt_schemas::schemas::common::{DbtChecksum, DbtQuoting};
use dbt_schemas::schemas::manifest::{
    DbtConfig, DbtModel, InternalDbtNode, IntrospectionKind, ManifestModelConfig, Nodes,
};
use dbt_schemas::schemas::properties::TryBuildConfig;
use dbt_schemas::state::{DbtAsset, DbtRuntimeConfig, ModelStatus};
use minijinja::constants::{TARGET_PACKAGE_NAME, TARGET_UNIQUE_ID};
use minijinja::{ErrorKind as MinijinjaErrorKind, MacroSpans, Value as MinijinjaValue};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex};

use crate::args::ResolveArgs;
use dbt_common::fsinfo;

/// Represents the result of rendering a single SQL file
#[derive(Debug)]
pub struct SqlFileRenderResult<T> {
    /// The asset that was rendered
    pub asset: DbtAsset,
    /// The status of the model
    pub status: ModelStatus,
    /// The file info for the rendered SQL file
    pub sql_file_info: SqlFileInfo,
    /// The rendered SQL
    pub rendered_sql: String,
    /// The macro spans for the rendered SQL
    pub macro_spans: MacroSpans,
    /// The properties for the model
    pub properties: Option<T>,
    /// The path to the properties file that defines this model
    pub patch_path: Option<PathBuf>,
}

/// Extracts model and version configuration from node properties
fn extract_model_and_version_config<T: TryBuildConfig + DeserializeOwned + std::fmt::Debug>(
    ref_name: &str,
    mpe: &mut MinimalPropertiesEntry,
    duplicate_errors: &mut Vec<FsError>,
    arg: &ResolveArgs,
    jinja_env: &JinjaEnvironment<'static>,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
) -> FsResult<(Option<T>, Option<DbtConfig>)> {
    if !mpe.duplicate_paths.is_empty() {
        register_duplicate_resource(mpe, ref_name, "model", duplicate_errors);
        return Ok((None, None));
    }
    // Can occur if a model asset is duplicated, but does not have duplicate property.yml definitions.
    if mpe.schema_value.is_null() {
        return Ok((None, None));
    }

    // Swap the schema value for Null - we are doing this so that we don't have to clone
    let schema_value = std::mem::replace(&mut mpe.schema_value, dbt_serde_yaml::Value::null());

    let (maybe_model, errors) =
        into_typed_with_jinja_error::<T, _>(schema_value, false, jinja_env, base_ctx, None)?;

    for error in errors {
        let context = format!(
            "While parsing {} config: {}",
            T::resource_name(),
            error.context
        );
        let error = error.with_context(context);
        show_warning_soon_to_be_error!(arg.io, error);
    }
    let maybe_version_config = if let Some(version_info) = mpe.version_info.as_ref() {
        if let Some(version_config) = version_info.version_config.as_ref() {
            let (version_config, errors) = into_typed_with_jinja_error::<DbtConfig, _>(
                version_config.clone(),
                false,
                jinja_env,
                base_ctx,
                None,
            )?;

            for error in errors {
                let context = format!(
                    "While parsing {} version config: {}",
                    T::resource_name(),
                    error.context
                );
                let error = error.with_context(context);
                show_warning_soon_to_be_error!(arg.io, error);
            }

            Some(version_config)
        } else {
            None
        }
    } else {
        None
    };
    Ok((Some(maybe_model), maybe_version_config))
}

/// Render the SQL files and return the SQL resources found while rendering the files
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub async fn render_unresolved_sql_files_sequentially<
    T: TryBuildConfig + DeserializeOwned + Send + Clone + 'static + std::fmt::Debug,
>(
    arg: &ResolveArgs,
    model_sql_files: &[DbtAsset],
    package_name: &str,
    package_quoting: DbtQuoting,
    adapter_type: &str,
    database: &str,
    schema: &str,
    jinja_env: &JinjaEnvironment<'static>,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    node_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    root_project_name: &str,
    root_project_config: &DbtProjectConfig,
    local_project_config: &DbtProjectConfig,
    runtime_config: Arc<DbtRuntimeConfig>,
    resource_paths: &[String],
) -> FsResult<Vec<SqlFileRenderResult<T>>> {
    let mut model_sql_resources_map = Vec::new();
    let mut duplicate_errors = Vec::new();

    if model_sql_files.is_empty() {
        return Ok(Vec::new());
    }

    for dbt_asset in model_sql_files {
        dbt_common::check_cancellation!(arg.io.should_cancel_compilation).map_err(|e| *e)?;

        let ref_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
        let (maybe_model, maybe_version_config) = {
            if let Some(mpe) = node_properties.get_mut(ref_name) {
                extract_model_and_version_config::<T>(
                    ref_name,
                    mpe,
                    &mut duplicate_errors,
                    arg,
                    jinja_env,
                    base_ctx,
                )
                .map_err(|e| *e)?
            } else {
                (None::<T>, None::<DbtConfig>)
            }
        };

        if maybe_model.is_none() && maybe_version_config.is_none() && !duplicate_errors.is_empty() {
            continue;
        }

        let project_config =
            local_project_config.get_config_for_path(&dbt_asset.path, package_name, resource_paths);
        let properties_config: DbtConfig = if let Some(model) = &maybe_model {
            if let Some(mut properties_config) = model.try_build_config().map_err(|e| *e)? {
                properties_config.default_to(project_config);
                properties_config
            } else {
                project_config.clone()
            }
        } else {
            project_config.clone()
        };
        let properties_config: DbtConfig = if let Some(mut version_config) = maybe_version_config {
            version_config.default_to(&properties_config);
            version_config
        } else {
            properties_config
        };
        let model_name = dbt_asset
            .path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let absolute_path = dbt_asset.base_path.join(&dbt_asset.path);
        let sql = read_to_string(&absolute_path).await.map_err(|e| *e)?;

        let sql_resources = Arc::new(Mutex::new(Vec::new()));
        let execute_exists = Arc::new(AtomicBool::new(false));

        let mut resolve_model_context = base_ctx.clone();
        resolve_model_context.extend(build_resolve_model_context(
            &properties_config,
            adapter_type,
            database,
            schema,
            &model_name,
            get_node_fqn(
                package_name,
                dbt_asset.path.clone(),
                vec![model_name.clone()],
            ),
            package_name,
            root_project_name,
            package_quoting,
            runtime_config.clone(),
            sql_resources.clone(),
            execute_exists.clone(),
        ));
        let display_path = if dbt_asset.base_path == arg.io.out_dir {
            PathBuf::from("target").join(dbt_asset.to_display_path(&arg.io.out_dir))
        } else {
            dbt_asset.to_display_path(&arg.io.in_dir)
        };
        show_progress!(
            arg.io,
            fsinfo!(PARSING.into(), display_path.display().to_string())
        );
        match render_sql(&sql, jinja_env, resolve_model_context, None, &display_path) {
            Ok((rendered_sql_except_refs_and_sources, macro_spans)) => {
                let sql_resources_cloned = sql_resources.clone();

                if root_project_name != package_name {
                    let root_config: DbtConfig = root_project_config
                        .get_config_for_path(&dbt_asset.path, package_name, resource_paths)
                        .clone();
                    sql_resources_cloned
                        .lock()
                        .unwrap()
                        .push(SqlResource::Config(Box::new(root_config.clone())));
                }

                let sql_resources_locked = sql_resources_cloned.lock().unwrap().clone();
                let sql_file_info = SqlFileInfo::from_sql_resources(
                    sql_resources_locked.clone(),
                    DbtChecksum::hash(sql.as_bytes()),
                    execute_exists.load(atomic::Ordering::Relaxed),
                );
                let model_config = ManifestModelConfig::from(*sql_file_info.config.clone());
                let status = if model_config
                    .enabled
                    .expect("model config should be set by now")
                {
                    ModelStatus::Enabled
                } else {
                    ModelStatus::Disabled
                };

                model_sql_resources_map.push(SqlFileRenderResult {
                    asset: dbt_asset.clone(),
                    sql_file_info,
                    rendered_sql: rendered_sql_except_refs_and_sources,
                    macro_spans,
                    properties: maybe_model,
                    status,
                    patch_path: node_properties
                        .get(ref_name)
                        .map(|mpe| mpe.relative_path.clone()),
                });
            }
            Err(err) => {
                let status;
                let sql_resources_cloned = sql_resources.clone();
                let sql_resources_locked = sql_resources_cloned.lock().unwrap().clone();
                let sql_file_info = SqlFileInfo::from_sql_resources(
                    sql_resources_locked.clone(),
                    DbtChecksum::hash(sql.as_bytes()),
                    execute_exists.load(atomic::Ordering::Relaxed),
                );
                match err.kind() {
                    MinijinjaErrorKind::DisabledModel => {
                        status = ModelStatus::Disabled;
                    }
                    MinijinjaErrorKind::SyntaxError => {
                        status = ModelStatus::ParsingFailed;
                        show_error!(
                            arg.io,
                            FsError::from(err).with_location(dbt_asset.path.clone())
                        );
                    }
                    _ => {
                        let model_config = ManifestModelConfig::from(*sql_file_info.config.clone());
                        if model_config
                            .enabled
                            .expect("model config should be set by now")
                        {
                            status = ModelStatus::ParsingFailed;
                            show_error!(
                                arg.io,
                                FsError::from(err).with_location(dbt_asset.path.clone())
                            );
                        } else {
                            status = ModelStatus::Disabled;
                        }
                    }
                }
                model_sql_resources_map.push(SqlFileRenderResult {
                    asset: dbt_asset.clone(),
                    sql_file_info,
                    rendered_sql: "".to_string(),
                    macro_spans: MacroSpans::default(),
                    properties: maybe_model,
                    status,
                    patch_path: node_properties
                        .get(ref_name)
                        .map(|mpe| mpe.relative_path.clone()),
                });
                continue;
            }
        }
    }

    trigger_duplicate_errors(&arg.io, &mut duplicate_errors)?;

    Ok(model_sql_resources_map)
}
/// iterate over all the sql files passed in, generate the local config, initailize the sql render env, and render the sql
/// and return the sql resources (deps) found while rendering the files
#[allow(clippy::too_many_arguments)]
pub async fn render_unresolved_sql_files<
    T: TryBuildConfig + DeserializeOwned + Send + Clone + 'static + std::fmt::Debug,
>(
    arg: &ResolveArgs,
    model_sql_files: &[DbtAsset],
    package_name: &str,
    package_quoting: DbtQuoting,
    adapter_type: &str,
    database: &str,
    schema: &str,
    jinja_env: &JinjaEnvironment<'static>,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    node_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    root_project_name: &str,
    root_project_config: &DbtProjectConfig,
    local_project_config: &DbtProjectConfig,
    runtime_config: Arc<DbtRuntimeConfig>,
    resource_paths: &[String],
) -> FsResult<Vec<SqlFileRenderResult<T>>> {
    let mut model_sql_resources_map = Vec::new();
    let mut duplicate_errors = Vec::new();

    if model_sql_files.is_empty() {
        return Ok(Vec::new());
    }
    if model_sql_files.len() < 50 || arg.num_threads == Some(1) {
        // if the number of files is less than 50 or the user has specified to use a single thread, use a single thread
        return render_unresolved_sql_files_sequentially(
            arg,
            model_sql_files,
            package_name,
            package_quoting,
            adapter_type,
            database,
            schema,
            jinja_env,
            base_ctx,
            node_properties,
            root_project_name,
            root_project_config,
            local_project_config,
            runtime_config,
            resource_paths,
        )
        .await;
    }
    // this code is io bound. So split the work between min(4, max_cpus) threads
    let max_concurrency = arg
        .num_threads
        .unwrap_or(4)
        .min(16)
        .min(model_sql_files.len())
        .max(1);

    let chunk_size = model_sql_files.len().div_ceil(max_concurrency);
    // Partition the workload and node_properties into chunks
    let mut tasks = Vec::new();
    let mut chunked_files: Vec<Vec<DbtAsset>> = Vec::new();
    let mut chunked_node_props: Vec<BTreeMap<String, MinimalPropertiesEntry>> = Vec::new();
    for chunk in model_sql_files.chunks(chunk_size) {
        let chunk_vec = chunk.to_vec();
        let mut chunk_props = BTreeMap::new();
        for dbt_asset in &chunk_vec {
            let ref_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
            if let Some(entry) = node_properties.get(ref_name) {
                chunk_props.insert(ref_name.to_string(), entry.clone());
            }
        }
        chunked_files.push(chunk_vec);
        chunked_node_props.push(chunk_props);
    }

    for (chunk, mut chunk_node_properties) in chunked_files.into_iter().zip(chunked_node_props) {
        let arg = arg.clone();
        let package_name = package_name.to_string();
        let adapter_type = adapter_type.to_string();
        let database = database.to_string();
        let schema = schema.to_string();
        let jinja_env = jinja_env.clone();
        let base_ctx = base_ctx.clone();
        let local_project_config = local_project_config.clone();
        let runtime_config = runtime_config.clone();
        let resource_paths = resource_paths.to_vec();

        let root_project_name = root_project_name.to_owned();
        // TODO: a potentially expensive clone. Arc this
        let root_project_config = root_project_config.to_owned();
        tasks.push(tokio::spawn(async move {
            let mut local_results: Vec<SqlFileRenderResult<T>> = Vec::new();
            let mut local_duplicate_errors: Vec<FsError> = Vec::new();

            for dbt_asset in chunk {
                dbt_common::check_cancellation!(arg.io.should_cancel_compilation)
                    .map_err(|e| *e)?;

                let ref_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
                let (maybe_model, maybe_version_config) = {
                    if let Some(mpe) = chunk_node_properties.get_mut(ref_name) {
                        extract_model_and_version_config::<T>(
                            ref_name,
                            mpe,
                            &mut local_duplicate_errors,
                            &arg,
                            &jinja_env,
                            &base_ctx,
                        )
                        .map_err(|e| *e)?
                    } else {
                        (None::<T>, None::<DbtConfig>)
                    }
                };

                if maybe_model.is_none()
                    && maybe_version_config.is_none()
                    && !local_duplicate_errors.is_empty()
                {
                    continue;
                }

                let project_config = local_project_config.get_config_for_path(
                    &dbt_asset.path,
                    &package_name,
                    &resource_paths,
                );
                let properties_config: DbtConfig = if let Some(model) = &maybe_model {
                    if let Some(mut properties_config) = model.try_build_config().map_err(|e| *e)? {
                        properties_config.default_to(project_config);
                        properties_config
                    } else {
                        project_config.clone()
                    }
                } else {
                    project_config.clone()
                };
                let properties_config: DbtConfig =
                    if let Some(mut version_config) = maybe_version_config {
                        version_config.default_to(&properties_config);
                        version_config
                    } else {
                        properties_config
                    };
                let model_name = dbt_asset
                    .path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();

                let absolute_path = dbt_asset.base_path.join(&dbt_asset.path);
                let sql = read_to_string(&absolute_path).await.map_err(|e| *e)?;
                let sql_resources = Arc::new(Mutex::new(Vec::new()));
                // when `load`, using `Ordering::Relax` is enough since no threads should be writing to it
                // as long as the read is after `render_sql` is done within this scope
                let execute_exists = Arc::new(AtomicBool::new(false));

                let mut resolve_model_context = base_ctx.clone();
                resolve_model_context.extend(build_resolve_model_context(
                    &properties_config,
                    &adapter_type,
                    &database,
                    &schema,
                    &model_name,
                    get_node_fqn(
                        &package_name,
                        dbt_asset.path.clone(),
                        vec![model_name.clone()],
                    ),
                    &package_name,
                    &root_project_name,
                    package_quoting,
                    runtime_config.clone(),
                    sql_resources.clone(),
                    execute_exists.clone(),
                ));
                let display_path = if dbt_asset.base_path == arg.io.out_dir {
                    PathBuf::from("target").join(dbt_asset.to_display_path(&arg.io.out_dir))
                } else {
                    dbt_asset.to_display_path(&arg.io.in_dir)
                };
                show_progress!(
                    arg.io,
                    fsinfo!(PARSING.into(), display_path.display().to_string())
                );
                match render_sql(&sql, &jinja_env, resolve_model_context, None, &display_path) {
                    Ok((rendered_sql_except_refs_and_sources, macro_spans)) => {
                        let sql_resources_cloned = sql_resources.clone();

                        if root_project_name != package_name {
                            let root_config: DbtConfig = root_project_config
                                .get_config_for_path(
                                    &dbt_asset.path,
                                    &package_name,
                                    &resource_paths,
                                )
                                .clone();
                            sql_resources_cloned
                                .lock()
                                .unwrap()
                                .insert(0, SqlResource::Config(Box::new(root_config.clone())));
                        }

                        let sql_resources_locked = sql_resources_cloned.lock().unwrap().clone();
                        let sql_file_info = SqlFileInfo::from_sql_resources(
                            sql_resources_locked.clone(),
                            DbtChecksum::hash(sql.as_bytes()),
                            execute_exists.load(atomic::Ordering::Relaxed),
                        );
                        // check the model config to see if it is enabled
                        let model_config = ManifestModelConfig::from(*sql_file_info.config.clone());
                        let status = if model_config
                            .enabled
                            .expect("model config should be set by now")
                        {
                            ModelStatus::Enabled
                        } else {
                            ModelStatus::Disabled
                        };

                        local_results.push(SqlFileRenderResult {
                            asset: dbt_asset.clone(),
                            sql_file_info,
                            rendered_sql: rendered_sql_except_refs_and_sources,
                            macro_spans,
                            properties: maybe_model,
                            status,
                            patch_path: chunk_node_properties
                                .get(ref_name)
                                .map(|mpe| mpe.relative_path.clone()),
                        });
                    }
                    Err(err) => {
                        let status;
                        let sql_resources_cloned = sql_resources.clone();
                        let sql_resources_locked = sql_resources_cloned.lock().unwrap().clone();
                        let sql_file_info = SqlFileInfo::from_sql_resources(
                            sql_resources_locked.clone(),
                            DbtChecksum::hash(sql.as_bytes()),
                            execute_exists.load(atomic::Ordering::Relaxed),
                        );
                        match err.kind() {
                            // Model is disabled and template compiles
                            MinijinjaErrorKind::DisabledModel => {
                                status = ModelStatus::Disabled;
                            }
                            // Template is invalid
                            MinijinjaErrorKind::SyntaxError => {
                                status = ModelStatus::ParsingFailed;
                                show_error!(
                                    arg.io,
                                    FsError::from(err).with_location(dbt_asset.path.clone())
                                );
                            }
                            _ => {
                                let model_config =
                                    ManifestModelConfig::from(*sql_file_info.config.clone());
                                if model_config
                                    .enabled
                                    .expect("model config should be set by now")
                                {
                                    status = ModelStatus::ParsingFailed;
                                    show_error!(
                                        arg.io,
                                        FsError::from(err).with_location(dbt_asset.path.clone())
                                    );
                                } else {
                                    // Model is disabled and template fails to compile for a non-syntax/non-disabled error
                                    status = ModelStatus::Disabled;
                                }
                            }
                        }
                        local_results.push(SqlFileRenderResult {
                            asset: dbt_asset.clone(),
                            sql_file_info,
                            rendered_sql: "".to_string(),
                            macro_spans: MacroSpans::default(),
                            properties: maybe_model,
                            status,
                            patch_path: chunk_node_properties
                                .get(ref_name)
                                .map(|mpe| mpe.relative_path.clone()),
                        });
                        continue;
                    }
                }
            }

            Ok::<_, _>((local_results, local_duplicate_errors, chunk_node_properties))
        }));
    }

    // Collect results from all tasks
    let mut merged_node_properties: BTreeMap<String, MinimalPropertiesEntry> = BTreeMap::new();
    for task in tasks {
        match task.await {
            Ok(Ok((results, errors, chunk_node_properties))) => {
                model_sql_resources_map.extend(results);
                duplicate_errors.extend(errors);
                merged_node_properties.extend(chunk_node_properties);
            }
            Ok(Err(err)) => {
                show_error!(arg.io, err);
                continue;
            }
            Err(err) => {
                show_error!(
                    arg.io,
                    fs_err!(ErrorCode::Unexpected, "{}", err.to_string())
                );
                continue;
            }
        }
    }
    trigger_duplicate_errors(&arg.io, &mut duplicate_errors)?;

    // Merge back node_properties
    *node_properties = merged_node_properties;

    Ok(model_sql_resources_map)
}

/// Collect the adapter identifiers for the given nodes and check if they are detected as unsafe
#[allow(clippy::too_many_arguments)]
pub async fn collect_adapter_identifiers_detect_unsafe(
    arg: &ResolveArgs,
    models: &mut HashMap<String, Arc<DbtModel>>,
    refs_and_sources: &RefsAndSources,
    jinja_env: &JinjaEnvironment<'static>,
    adapter_type: &str,
    package_name: &str,
    root_project_name: &str,
    runtime_config: Arc<DbtRuntimeConfig>,
) -> FsResult<()> {
    if models.is_empty() {
        return Ok(());
    }
    // Prepare chunking
    let model_vec: Vec<(String, Arc<DbtModel>)> = models
        .iter()
        .map(|(k, v)| (k.clone(), Arc::clone(v)))
        .collect();

    let max_concurrency = arg
        .num_threads
        .unwrap_or(4)
        .min(16)
        .min(model_vec.len())
        .max(1);
    let chunk_size = model_vec.len().div_ceil(max_concurrency);

    let parse_adapter = jinja_env
        .get_parse_adapter()
        .expect("Adapter should be parse");

    // Use sequential processing if num_threads is 1, otherwise use parallel processing
    let all_unsafe_ids = if max_concurrency == 1 {
        collect_adapter_identifiers_sequential(
            arg,
            model_vec,
            refs_and_sources,
            jinja_env,
            adapter_type,
            package_name,
            root_project_name,
            runtime_config,
            parse_adapter,
            chunk_size,
        )
        .await?
    } else {
        collect_adapter_identifiers_parallel(
            arg,
            model_vec,
            refs_and_sources,
            jinja_env,
            adapter_type,
            package_name,
            root_project_name,
            runtime_config,
            parse_adapter,
            chunk_size,
        )
        .await?
    };

    // Now, update the models in the main thread
    for unsafe_id in all_unsafe_ids {
        if let Some(arc_model) = models.get_mut(&unsafe_id) {
            let model = Arc::make_mut(arc_model);
            model.set_detected_introspection(Some(IntrospectionKind::Execute));
        }
    }

    Ok(())
}

/// Processes a chunk of models to detect unsafe identifiers
#[allow(clippy::too_many_arguments)]
async fn process_model_chunk_for_unsafe_detection(
    chunk: Vec<(String, Arc<DbtModel>)>,
    arg: ResolveArgs,
    refs_and_sources: RefsAndSources,
    jinja_env: JinjaEnvironment<'static>,
    adapter_type: String,
    package_name: String,
    root_project_name: String,
    runtime_config: Arc<DbtRuntimeConfig>,
    parse_adapter: Arc<dbt_fusion_adapter::ParseAdapter>,
) -> FsResult<Vec<String>> {
    let mut unsafe_ids = Vec::new();
    let mut render_base_context = build_compile_and_run_base_context(
        Arc::new(refs_and_sources.clone()),
        &package_name,
        &Nodes::default(),
        runtime_config.clone(),
    );
    silence_base_context(&mut render_base_context);

    for (_key, arc_model) in chunk {
        let model = (*arc_model).clone();
        dbt_common::check_cancellation!(arg.io.should_cancel_compilation)?;

        let absolute_path = arg.io.in_dir.join(&model.common().original_file_path);
        let sql = read_to_string(&absolute_path).await?;

        let config = model.get_dbt_config();
        render_base_context.insert(
            TARGET_PACKAGE_NAME.to_string(),
            MinijinjaValue::from(model.common().package_name.clone()),
        );
        render_base_context.insert(
            TARGET_UNIQUE_ID.to_string(),
            MinijinjaValue::from(model.common().unique_id.clone()),
        );

        let quoting = match config.quoting.unwrap().try_into() {
            Ok(q) => q,
            Err(_) => {
                show_error!(
                    arg.io,
                    fs_err!(ErrorCode::Generic, "")
                        .with_location(model.common().original_file_path.clone())
                );
                return Ok(unsafe_ids);
            }
        };

        let (render_resolved_context, _, _) = build_compile_node_context(
            &MinijinjaValue::from_serialize(model.serialize()),
            model.common(),
            model.base().alias.as_str(),
            &config,
            quoting,
            &adapter_type,
            &render_base_context,
            &root_project_name,
            runtime_config.dependencies.keys().cloned().collect(),
        );
        let display_path = if arg
            .io
            .out_dir
            .join(&model.common().original_file_path)
            .exists()
        {
            PathBuf::from("target").join(&model.common().original_file_path)
        } else {
            arg.io.in_dir.join(&model.common().original_file_path)
        };
        let _res = render_sql(
            &sql,
            &jinja_env,
            render_resolved_context,
            None,
            &display_path,
        );
        if parse_adapter
            .unsafe_nodes()
            .contains(&model.common().unique_id)
        {
            unsafe_ids.push(model.common().unique_id.clone());
        }
    }
    Ok(unsafe_ids)
}

/// Collect adapter identifiers sequentially (single-threaded)
#[allow(clippy::too_many_arguments)]
async fn collect_adapter_identifiers_sequential(
    arg: &ResolveArgs,
    model_vec: Vec<(String, Arc<DbtModel>)>,
    refs_and_sources: &RefsAndSources,
    jinja_env: &JinjaEnvironment<'static>,
    adapter_type: &str,
    package_name: &str,
    root_project_name: &str,
    runtime_config: Arc<DbtRuntimeConfig>,
    parse_adapter: Arc<dbt_fusion_adapter::ParseAdapter>,
    chunk_size: usize,
) -> FsResult<Vec<String>> {
    let mut all_unsafe_ids = Vec::new();

    for chunk in model_vec.chunks(chunk_size) {
        let chunk = chunk.to_vec();
        let unsafe_ids = process_model_chunk_for_unsafe_detection(
            chunk,
            arg.clone(),
            refs_and_sources.clone(),
            jinja_env.clone(),
            adapter_type.to_string(),
            package_name.to_string(),
            root_project_name.to_string(),
            runtime_config.clone(),
            parse_adapter.clone(),
        )
        .await?;
        all_unsafe_ids.extend(unsafe_ids);
    }

    Ok(all_unsafe_ids)
}

/// Collect adapter identifiers in parallel using tokio::spawn
#[allow(clippy::too_many_arguments)]
async fn collect_adapter_identifiers_parallel(
    arg: &ResolveArgs,
    model_vec: Vec<(String, Arc<DbtModel>)>,
    refs_and_sources: &RefsAndSources,
    jinja_env: &JinjaEnvironment<'static>,
    adapter_type: &str,
    package_name: &str,
    root_project_name: &str,
    runtime_config: Arc<DbtRuntimeConfig>,
    parse_adapter: Arc<dbt_fusion_adapter::ParseAdapter>,
    chunk_size: usize,
) -> FsResult<Vec<String>> {
    let mut tasks = Vec::new();

    for chunk in model_vec.chunks(chunk_size) {
        let chunk = chunk.to_vec();
        let arg = arg.clone();
        let refs_and_sources = refs_and_sources.clone();
        let jinja_env = jinja_env.clone();
        let adapter_type = adapter_type.to_string();
        let package_name = package_name.to_string();
        let root_project_name = root_project_name.to_string();
        let runtime_config = runtime_config.clone();
        let parse_adapter = parse_adapter.clone();

        tasks.push(tokio::spawn(async move {
            process_model_chunk_for_unsafe_detection(
                chunk,
                arg,
                refs_and_sources,
                jinja_env,
                adapter_type,
                package_name,
                root_project_name,
                runtime_config,
                parse_adapter,
            )
            .await
            .map_err(|e| *e)
        }));
    }

    // Collect all unsafe IDs from all threads
    let mut all_unsafe_ids = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(ids)) => {
                all_unsafe_ids.extend(ids);
            }
            Ok(Err(e)) => return Err(Box::new(e)),
            Err(e) => return Err(fs_err!(ErrorCode::Unexpected, "{}", e)),
        }
    }

    Ok(all_unsafe_ids)
}
