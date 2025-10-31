use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use crate::utils::get_original_file_path;
use dbt_common::{
    CodeLocation, ErrorCode, FsResult, fs_err,
    io_args::{IoArgs, StaticAnalysisKind},
};
use dbt_common::{adapter::AdapterType, tracing::emit::emit_warn_log_from_fs_error};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv,
    listener::DefaultRenderingEventListenerFactory,
    phases::parse::{build_resolve_model_context, sql_resource::SqlResource},
    utils::render_sql,
};
use dbt_schemas::{
    schemas::{
        CommonAttributes,
        common::{DbtChecksum, DbtQuoting},
        manifest::DbtOperation,
        project::DbtProject,
        ref_and_source::{DbtRef, DbtSourceWrapper},
    },
    state::DbtRuntimeConfig,
};
use dbt_serde_yaml::Spanned;

/// Empty config type used for operations (they don't have traditional config)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct OperationEmptyConfig;

impl dbt_schemas::schemas::project::DefaultTo<OperationEmptyConfig> for OperationEmptyConfig {
    fn default_to(&mut self, _other: &OperationEmptyConfig) {}
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn resolve_operations(
    dbt_project: &DbtProject,
    package_base_path: &Path,
    project_root: &Path,
    jinja_env: &Arc<JinjaEnv>,
    io: &IoArgs,
    adapter_type: AdapterType,
    database: &str,
    schema: &str,
    root_project_quoting: DbtQuoting,
    root_runtime_config: Arc<DbtRuntimeConfig>,
) -> FsResult<(Vec<Spanned<DbtOperation>>, Vec<Spanned<DbtOperation>>)> {
    let mut on_run_start = Vec::new();
    let mut on_run_end = Vec::new();

    for start in dbt_project.on_run_start.iter() {
        let operations: Vec<Spanned<String>> = start.clone().into();
        on_run_start.extend(new_operation(
            "on_run_start",
            &operations,
            dbt_project,
            package_base_path,
            project_root,
            jinja_env,
            io,
            adapter_type,
            database,
            schema,
            &root_project_quoting,
            &root_runtime_config,
        )?);
    }

    for end in dbt_project.on_run_end.iter() {
        let operations: Vec<Spanned<String>> = end.clone().into();
        on_run_end.extend(new_operation(
            "on_run_end",
            &operations,
            dbt_project,
            package_base_path,
            project_root,
            jinja_env,
            io,
            adapter_type,
            database,
            schema,
            &root_project_quoting,
            &root_runtime_config,
        )?);
    }

    Ok((on_run_start, on_run_end))
}

#[allow(clippy::too_many_arguments)]
fn new_operation(
    operation_type: &str,
    operations: &[Spanned<String>],
    dbt_project: &DbtProject,
    package_base_path: &Path,
    project_root: &Path,
    jinja_env: &Arc<JinjaEnv>,
    io: &IoArgs,
    adapter_type: AdapterType,
    database: &str,
    schema: &str,
    root_project_quoting: &DbtQuoting,
    root_runtime_config: &Arc<DbtRuntimeConfig>,
) -> FsResult<Vec<Spanned<DbtOperation>>> {
    let project_name = &dbt_project.name;
    // Calculate the original file path for dbt_project.yml
    let dbt_project_yml_path = PathBuf::from("dbt_project.yml");
    let original_file_path =
        get_original_file_path(package_base_path, project_root, &dbt_project_yml_path);

    // Map with index
    let mut resolved_operations = Vec::new();

    for (index, operation_sql_spanned) in operations.iter().enumerate() {
        let name = format!("{project_name}-{operation_type}-{index}");
        let unique_id = format!("operation.{project_name}.{name}");
        let operation_sql = operation_sql_spanned.as_ref();

        // Create the base operation
        let mut operation = DbtOperation {
            __common_attr__: CommonAttributes {
                name: name.clone(),
                package_name: project_name.to_string(),
                path: PathBuf::from("hooks").join(&name),
                original_file_path: original_file_path.clone(),
                unique_id,
                fqn: vec![project_name.to_string(), "hooks".to_string(), name],
                checksum: DbtChecksum::hash(operation_sql.trim().as_bytes()),
                raw_code: Some(operation_sql.to_string()),
                language: Some("sql".to_string()),
                ..Default::default()
            },
            __base_attr__: Default::default(),
            __other__: BTreeMap::new(),
        };

        // Skip empty operations
        if !operation_sql.trim().is_empty() {
            // Render and extract dependencies
            let sql_resources: Arc<Mutex<Vec<SqlResource<OperationEmptyConfig>>>> =
                Arc::new(Mutex::new(Vec::new()));
            let execute_exists = Arc::new(AtomicBool::new(false));

            // Build operation context with tracking functions
            let mut operation_ctx = BTreeMap::new();
            operation_ctx.extend(build_resolve_model_context(
                &OperationEmptyConfig,
                adapter_type,
                database,
                schema,
                &operation.__common_attr__.name,
                vec![
                    root_runtime_config.inner.project_name.clone(),
                    "hooks".to_string(),
                    operation.__common_attr__.name.clone(),
                ],
                &operation.__common_attr__.package_name,
                &root_runtime_config.inner.project_name,
                *root_project_quoting,
                root_runtime_config.clone(),
                sql_resources.clone(),
                execute_exists,
                &operation.__common_attr__.original_file_path,
                io,
            ));

            // Set TARGET_PACKAGE_NAME for var lookups
            operation_ctx.insert(
                "TARGET_PACKAGE_NAME".to_string(),
                minijinja::Value::from(operation.__common_attr__.package_name.clone()),
            );

            // Wrap operation SQL with reset_span() to provide proper span context for error messages
            let instruction_with_span = format!(
                "{{% do reset_span('{}', {}, {}, {}, {}, {}, {}) %}}\n{}",
                operation
                    .__common_attr__
                    .original_file_path
                    .to_string_lossy(),
                operation_sql_spanned.span().start.line as u32,
                operation_sql_spanned.span().start.column as u32,
                operation_sql_spanned.span().start.index as u32,
                operation_sql_spanned.span().end.line as u32,
                operation_sql_spanned.span().end.column as u32,
                operation_sql_spanned.span().end.index as u32,
                operation_sql,
            );

            // Render the operation SQL
            let listener_factory = DefaultRenderingEventListenerFactory::default();
            match render_sql(
                &instruction_with_span,
                jinja_env.as_ref(),
                &operation_ctx,
                &listener_factory,
                &operation.__common_attr__.original_file_path,
            ) {
                Ok(_) => {
                    // Extract refs and sources from sql_resources
                    let resources = sql_resources.lock().unwrap().clone();
                    for resource in resources {
                        match resource {
                            SqlResource::Ref((name, package, version, location)) => {
                                operation.__base_attr__.refs.push(DbtRef {
                                    name,
                                    package,
                                    version: version.map(|v| v.into()),
                                    location: Some(CodeLocation::new(
                                        location.line,
                                        location.col,
                                        location.index,
                                        operation.__common_attr__.original_file_path.clone(),
                                    )),
                                });
                            }
                            SqlResource::Source((source_name, table_name, location)) => {
                                operation.__base_attr__.sources.push(DbtSourceWrapper {
                                    source: vec![source_name, table_name],
                                    location: Some(CodeLocation::new(
                                        location.line,
                                        location.col,
                                        location.index,
                                        operation.__common_attr__.original_file_path.clone(),
                                    )),
                                });
                            }
                            _ => {
                                // Ignore other resource types
                            }
                        }
                    }

                    // Mark operation with static_analysis: Unsafe so it will always defer
                    operation.__base_attr__.static_analysis = StaticAnalysisKind::Unsafe.into();
                }
                Err(err) => {
                    // Log rendering error but don't fail the build
                    let err = fs_err!(
                        ErrorCode::Generic,
                        "Operation '{}' failed to render: {}",
                        operation.__common_attr__.name,
                        err.to_string()
                    )
                    .with_location(operation.__common_attr__.original_file_path.clone());
                    emit_warn_log_from_fs_error(&err, io.status_reporter.as_ref());
                }
            }
        }

        // Add the operation (with or without rendering)
        resolved_operations.push(operation_sql_spanned.clone().map(|_| operation));
    }

    Ok(resolved_operations)
}
