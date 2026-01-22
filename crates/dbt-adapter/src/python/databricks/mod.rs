use crate::{AdapterResponse, TypedBaseAdapter};

use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult, ErrorCode};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};
use serde_json::json;

mod api_client;
use api_client::DatabricksApiClient;

pub fn submit_python_job(
    adapter: &dyn TypedBaseAdapter,
    _ctx: &QueryCtx,
    _conn: &'_ mut dyn Connection,
    _state: &State,
    model: &Value,
    compiled_code: &str,
) -> AdapterResult<AdapterResponse> {
    let config = model
        .get_attr("config")
        .map_err(|e| AdapterError::new(AdapterErrorKind::Internal, e.to_string()))?;

    let catalog = model
        .get_attr("database")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "hive_metastore".to_string());

    let schema = model
        .get_attr("schema")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .ok_or_else(|| AdapterError::new(AdapterErrorKind::Internal, "schema is required"))?;

    let identifier = model
        .get_attr("alias")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .ok_or_else(|| AdapterError::new(AdapterErrorKind::Internal, "alias is required"))?;

    let submission_method = config
        .get_attr("submission_method")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "all_purpose_cluster".to_string());

    match submission_method.as_str() {
        "all_purpose_cluster" => submit_all_purpose_cluster(
            adapter,
            &config,
            &catalog,
            &schema,
            &identifier,
            compiled_code,
        ),
        "job_cluster" => submit_job_cluster(
            adapter,
            &config,
            &catalog,
            &schema,
            &identifier,
            compiled_code,
        ),
        "serverless_cluster" => submit_serverless_cluster(
            adapter,
            &config,
            &catalog,
            &schema,
            &identifier,
            compiled_code,
        ),
        "workflow_job" => submit_workflow_job(
            adapter,
            &config,
            &catalog,
            &schema,
            &identifier,
            compiled_code,
        ),
        _ => Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            format!(
                "Unsupported submission_method: '{}'. Supported methods: all_purpose_cluster, job_cluster, serverless_cluster, workflow_job",
                submission_method
            ),
        )),
    }
}

/// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/python_models/python_submissions.py#L412
fn submit_all_purpose_cluster(
    adapter: &dyn TypedBaseAdapter,
    config: &Value,
    catalog: &str,
    schema: &str,
    identifier: &str,
    compiled_code: &str,
) -> AdapterResult<AdapterResponse> {
    let cluster_id = resolve_cluster_id(adapter, config)?;

    let create_notebook = config
        .get_attr("create_notebook")
        .ok()
        .map(|v| v.is_true())
        .unwrap_or(false);

    if create_notebook {
        // Extract library configuration (packages, index_url, additional_libs)
        let packages = extract_packages(config);
        let index_url = config
            .get_attr("index_url")
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()));

        let additional_libs = config
            .get_attr("additional_libs")
            .ok()
            .and_then(|v| v.try_iter().ok())
            .map(|iter| {
                iter.filter_map(|v| serde_json::to_value(&v).ok())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let libraries = build_libraries(&packages, index_url.as_deref(), &additional_libs);

        let mut task_settings = json!({
            "existing_cluster_id": cluster_id
        });

        // Attach libraries if present
        if !libraries.is_empty() {
            task_settings["libraries"] = json!(libraries);
        }

        submit_via_notebook(
            adapter,
            config,
            catalog,
            schema,
            identifier,
            compiled_code,
            task_settings,
        )
    } else {
        submit_via_command_api(adapter, config, compiled_code, &cluster_id)
    }
}

/// https://github.com/databricks/dbt-databricks/blob/87954785bc43167b7bb4a404b793c34d36140dc9/dbt/adapters/databricks/python_models/python_submissions.py#L461
fn submit_serverless_cluster(
    adapter: &dyn TypedBaseAdapter,
    config: &Value,
    catalog: &str,
    schema: &str,
    identifier: &str,
    compiled_code: &str,
) -> AdapterResult<AdapterResponse> {
    let task_settings = json!({});

    submit_via_notebook(
        adapter,
        config,
        catalog,
        schema,
        identifier,
        compiled_code,
        task_settings,
    )
}

/// https://github.com/databricks/dbt-databricks/blob/955743ab67543ef1fad3c4f7c13cc8b4a0ab8c06/dbt/adapters/databricks/python_models/python_submissions.py#L465
fn submit_workflow_job(
    adapter: &dyn TypedBaseAdapter,
    config: &Value,
    catalog: &str,
    schema: &str,
    identifier: &str,
    compiled_code: &str,
) -> AdapterResult<AdapterResponse> {
    let python_job_config = config.get_attr("python_job_config").ok().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            "'python_job_config' is required for workflow_job submission method",
        )
    })?;

    let timeout = extract_timeout(config);

    let use_user_folder_for_python = config
        .get_attr("user_folder_for_python")
        .ok()
        .map(|v| v.is_true())
        .unwrap_or(false);

    let api_client = DatabricksApiClient::new(adapter, use_user_folder_for_python)?;

    let notebook_path = upload_notebook(
        &api_client,
        catalog,
        schema,
        identifier,
        compiled_code,
        true,
    )?;

    let task_settings = build_cluster_settings(config)?;

    let (workflow_spec, existing_job_id) = build_workflow_spec(
        &python_job_config,
        catalog,
        schema,
        identifier,
        &notebook_path,
        task_settings,
    )?;

    let job_id = create_or_update_workflow(&api_client, workflow_spec, existing_job_id)?;

    // todo: Implement permission building from grants and access_control_list configs

    let run_id = api_client.run_workflow(job_id, true)?;

    poll_job_completion(&api_client, &run_id.to_string(), timeout)?;

    // Only convert to string at the boundary (AdapterResponse)
    Ok(AdapterResponse {
        message: format!(
            "Python model executed successfully via workflow_job. Job ID: {}, Run ID: {}",
            job_id, run_id
        ),
        code: "OK".to_string(),
        rows_affected: 0,
        query_id: Some(run_id.to_string()),
    })
}

/// https://github.com/databricks/dbt-databricks/blob/955743ab67543ef1fad3c4f7c13cc8b4a0ab8c06/dbt/adapters/databricks/python_models/python_submissions.py#L392
fn submit_job_cluster(
    adapter: &dyn TypedBaseAdapter,
    config: &Value,
    catalog: &str,
    schema: &str,
    identifier: &str,
    compiled_code: &str,
) -> AdapterResult<AdapterResponse> {
    let job_cluster_config = config.get_attr("job_cluster_config").ok().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            "'job_cluster_config' is required for job_cluster submission method",
        )
    })?;

    validate_job_cluster_config(&job_cluster_config)?;

    let packages = extract_packages(config);
    let index_url = config
        .get_attr("index_url")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()));

    let additional_libs = config
        .get_attr("additional_libs")
        .ok()
        .and_then(|v| v.try_iter().ok())
        .map(|iter| {
            iter.filter_map(|v| serde_json::to_value(&v).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let cluster_config_json = serde_json::to_value(&job_cluster_config).map_err(|e| {
        AdapterError::new(
            AdapterErrorKind::Internal,
            format!("Failed to serialize job_cluster_config: {}", e),
        )
    })?;

    let libraries = build_libraries(&packages, index_url.as_deref(), &additional_libs);

    let mut task_settings = json!({
        "new_cluster": cluster_config_json
    });

    if !libraries.is_empty() {
        task_settings["libraries"] = json!(libraries);
    }

    submit_via_notebook(
        adapter,
        config,
        catalog,
        schema,
        identifier,
        compiled_code,
        task_settings,
    )
}

/// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/python_models/python_submissions.py#L61
fn submit_via_command_api(
    adapter: &dyn TypedBaseAdapter,
    config: &Value,
    compiled_code: &str,
    cluster_id: &str,
) -> AdapterResult<AdapterResponse> {
    let timeout = extract_timeout(config);

    let use_user_folder_for_python = config
        .get_attr("user_folder_for_python")
        .ok()
        .map(|v| v.is_true())
        .unwrap_or(false);

    let api_client = DatabricksApiClient::new(adapter, use_user_folder_for_python)?;

    let context_id = api_client.create_context(cluster_id)?;

    let _command_id_opt: Option<String> = None;
    let result = (|| -> AdapterResult<String> {
        let command_id = api_client.execute_command(cluster_id, &context_id, compiled_code)?;
        api_client.poll_command_completion(cluster_id, &context_id, &command_id, timeout)?;

        Ok(command_id)
    })();

    let cleanup_result = api_client.destroy_context(cluster_id, &context_id);
    match (result, cleanup_result) {
        (Ok(command_id), Ok(())) => Ok(AdapterResponse {
            message: format!(
                "Python model executed successfully using Command API on cluster {}",
                cluster_id
            ),
            code: "OK".to_string(),
            rows_affected: 0,
            query_id: Some(command_id),
        }),
        (Err(e), _) => Err(e),
        (Ok(_), Err(cleanup_err)) => Err(cleanup_err),
    }
}

fn submit_via_notebook(
    adapter: &dyn TypedBaseAdapter,
    config: &Value,
    catalog: &str,
    schema: &str,
    identifier: &str,
    compiled_code: &str,
    task_settings: serde_json::Value,
) -> AdapterResult<AdapterResponse> {
    let timeout = extract_timeout(config);

    let use_user_folder_for_python = config
        .get_attr("user_folder_for_python")
        .ok()
        .map(|v| v.is_true())
        .unwrap_or(false);

    let api_client = DatabricksApiClient::new(adapter, use_user_folder_for_python)?;

    let notebook_path = upload_notebook(
        &api_client,
        catalog,
        schema,
        identifier,
        compiled_code,
        true,
    )?;

    let run_name = format!(
        "{}-{}-{}-{}",
        catalog,
        schema,
        identifier,
        uuid::Uuid::new_v4()
    );

    let submission_type = if task_settings.get("new_cluster").is_some() {
        "job_cluster"
    } else if task_settings.get("existing_cluster_id").is_some() {
        "all_purpose_cluster"
    } else {
        "serverless_cluster"
    };

    let run_id = submit_notebook_job(
        &api_client,
        &run_name,
        &notebook_path,
        task_settings,
        timeout,
    )?;

    poll_job_completion(&api_client, &run_id, timeout)?;

    Ok(AdapterResponse {
        message: format!(
            "Python model executed successfully via notebook using {}. \
             Run ID: {}, Notebook: {}",
            submission_type, run_id, notebook_path
        ),
        code: "OK".to_string(),
        rows_affected: 0,
        query_id: Some(run_id),
    })
}

fn resolve_cluster_id(adapter: &dyn TypedBaseAdapter, config: &Value) -> AdapterResult<String> {
    // Precedence order: cluster_id -> http_path -> http_path from profile

    if let Some(cluster_id) = config
        .get_attr("cluster_id")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
    {
        return Ok(cluster_id);
    }

    if let Some(http_path) = config
        .get_attr("http_path")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
    {
        return extract_cluster_id_from_http_path(&http_path);
    }

    let http_path = adapter
        .engine()
        .get_config()
        .get_string("http_path")
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "Databricks `http_path` or `cluster_id` of an all-purpose cluster is required \
                 for the `all_purpose_cluster` submission method.",
            )
        })?;

    extract_cluster_id_from_http_path(&http_path)
}

fn extract_cluster_id_from_http_path(http_path: &str) -> AdapterResult<String> {
    // Check if this is a warehouse path (not a cluster)
    if http_path.contains("/warehouses/") {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "http_path points to a SQL Warehouse, not an all-purpose cluster. \
             Use 'job_cluster' submission method for SQL Warehouses.",
        ));
    }

    http_path
        .split('/')
        .next_back()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("Could not extract cluster_id from http_path: {}", http_path),
            )
        })
}

fn validate_job_cluster_config(config: &Value) -> AdapterResult<()> {
    let required_fields = vec!["spark_version", "node_type_id"];

    for field in required_fields {
        if config.get_attr(field).is_err() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "'{}' is required in job_cluster_config. Example:\n\
                    job_cluster_config:\n\
                      spark_version: '15.4.x-scala2.12'\n\
                      node_type_id: 'Standard_D4s_v5'\n\
                      num_workers: 1",
                    field
                ),
            ));
        }
    }

    Ok(())
}

fn extract_packages(config: &Value) -> Vec<String> {
    config
        .get_attr("packages")
        .ok()
        .and_then(|v| v.try_iter().ok())
        .map(|iter| {
            iter.filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn extract_timeout(config: &Value) -> u64 {
    let Some(value) = config.get_attr("timeout").ok() else {
        return 0;
    };

    let timeout = value
        .as_i64()
        .and_then(|n| u64::try_from(n).ok())
        .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()));

    match timeout {
        Some(t) => t,
        None => {
            emit_warn_log_message(
                ErrorCode::InvalidConfig,
                "Invalid timeout value, using default of 0",
                None,
            );
            0
        }
    }
}

fn build_libraries(
    packages: &[String],
    index_url: Option<&str>,
    additional_libs: &[serde_json::Value],
) -> Vec<serde_json::Value> {
    let mut libraries = Vec::new();

    for package in packages {
        let lib = if let Some(idx_url) = index_url {
            json!({
                "pypi": {
                    "package": package,
                    "repo": idx_url
                }
            })
        } else {
            json!({
                "pypi": {
                    "package": package
                }
            })
        };
        libraries.push(lib);
    }

    libraries.extend_from_slice(additional_libs);
    libraries
}

fn submit_notebook_job(
    api_client: &DatabricksApiClient,
    run_name: &str,
    notebook_path: &str,
    task_settings: serde_json::Value,
    timeout_seconds: u64,
) -> AdapterResult<String> {
    let mut task = json!({
        "task_key": "inner_notebook",
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "WORKSPACE"
        }
    });

    if let serde_json::Value::Object(settings_map) = task_settings
        && let serde_json::Value::Object(ref mut task_map) = task
    {
        task_map.extend(settings_map);
    }

    api_client.submit_job_run(run_name, &task, timeout_seconds)
}

fn poll_job_completion(
    api_client: &DatabricksApiClient,
    run_id: &str,
    timeout_seconds: u64,
) -> AdapterResult<()> {
    api_client.poll_job_completion(run_id, timeout_seconds)
}

/// https://github.com/databricks/dbt-databricks/blob/955743ab67543ef1fad3c4f7c13cc8b4a0ab8c06/dbt/adapters/databricks/python_models/python_submissions.py#L92
fn upload_notebook(
    api_client: &DatabricksApiClient,
    catalog: &str,
    schema: &str,
    identifier: &str,
    compiled_code: &str,
    create_notebook: bool,
) -> AdapterResult<String> {
    if !create_notebook {
        return Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            "create_notebook=false is not supported yet. Notebooks are required for job_cluster submission.",
        ));
    }

    let notebook_path = api_client.notebook_path(catalog, schema, identifier)?;
    let parent_path = notebook_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .filter(|parent| !parent.is_empty())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Internal,
                format!("Failed to determine parent directory for notebook path {notebook_path}"),
            )
        })?;

    api_client.ensure_directory(parent_path)?;
    api_client.import_notebook(&notebook_path, compiled_code)?;

    Ok(notebook_path)
}

/// Build cluster settings for workflow task
/// https://github.com/databricks/dbt-databricks/blob/955743ab67543ef1fad3c4f7c13cc8b4a0ab8c06/dbt/adapters/databricks/python_models/python_submissions.py#L494
fn build_cluster_settings(config: &Value) -> AdapterResult<serde_json::Value> {
    // Priority: job_cluster_config > cluster_id > empty (serverless)
    if let Ok(job_cluster_config) = config.get_attr("job_cluster_config") {
        let cluster_config_json = serde_json::to_value(&job_cluster_config).map_err(|e| {
            AdapterError::new(
                AdapterErrorKind::Internal,
                format!("Failed to serialize job_cluster_config: {}", e),
            )
        })?;
        return Ok(json!({
            "new_cluster": cluster_config_json
        }));
    }

    if let Some(cluster_id) = config
        .get_attr("cluster_id")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
    {
        return Ok(json!({
            "existing_cluster_id": cluster_id
        }));
    }

    // No cluster specified - serverless
    Ok(json!({}))
}

/// https://github.com/databricks/dbt-databricks/blob/955743ab67543ef1fad3c4f7c13cc8b4a0ab8c06/dbt/adapters/databricks/python_models/python_submissions.py#L477
fn build_workflow_spec(
    python_job_config: &Value,
    catalog: &str,
    schema: &str,
    identifier: &str,
    notebook_path: &str,
    task_settings: serde_json::Value,
) -> AdapterResult<(serde_json::Value, Option<u64>)> {
    let workflow_name = python_job_config
        .get_attr("name")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| format!("dbt__{}-{}-{}", catalog, schema, identifier));

    let existing_job_id = python_job_config
        .get_attr("existing_job_id")
        .ok()
        .and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .or_else(|| v.as_i64().map(|i| i as u64))
        });

    let post_hook_tasks = python_job_config
        .get_attr("post_hook_tasks")
        .ok()
        .and_then(|v| v.try_iter().ok())
        .map(|iter| {
            iter.filter_map(|v| serde_json::to_value(&v).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut notebook_task = json!({
        "task_key": "inner_notebook",
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "WORKSPACE"
        }
    });

    // notebook_task is always an Object since we just created it with json!({...})
    let task_map = notebook_task.as_object_mut().unwrap();

    if let serde_json::Value::Object(settings_map) = task_settings {
        task_map.extend(settings_map);
    }

    if let Ok(additional_settings) = python_job_config.get_attr("additional_task_settings") {
        let additional_json =
            serde_json::to_value(&additional_settings).unwrap_or_else(|_| json!({}));
        if let serde_json::Value::Object(add_map) = additional_json {
            task_map.extend(add_map);
        }
    }

    let mut tasks = vec![notebook_task];
    tasks.extend(post_hook_tasks);

    let mut workflow_spec = serde_json::to_value(python_job_config).unwrap_or_else(|_| json!({}));

    // Ensure workflow_spec is an Object - fail loudly if user provided invalid config
    let spec_map = workflow_spec.as_object_mut().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            "python_job_config must be an object/dictionary",
        )
    })?;

    spec_map.insert("name".to_string(), json!(workflow_name));
    spec_map.insert("tasks".to_string(), json!(tasks));

    Ok((workflow_spec, existing_job_id))
}

/// https://github.com/databricks/dbt-databricks/blob/955743ab67543ef1fad3c4f7c13cc8b4a0ab8c06/dbt/adapters/databricks/python_models/python_submissions.py#L369
fn create_or_update_workflow(
    api_client: &DatabricksApiClient,
    workflow_spec: serde_json::Value,
    existing_job_id: Option<u64>,
) -> AdapterResult<u64> {
    if let Some(job_id) = existing_job_id {
        api_client.update_workflow(job_id, workflow_spec)?;
        return Ok(job_id);
    }

    let workflow_name = workflow_spec
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Internal,
                "Workflow spec missing 'name' field",
            )
        })?;

    let search_response = api_client.search_workflows_by_name(workflow_name)?;

    if search_response.jobs.len() > 1 {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!(
                "Multiple jobs found with name '{}'. Use a unique job name or specify \
                'existing_job_id' in python_job_config.",
                workflow_name
            ),
        ));
    }

    if let Some(workflow) = search_response.jobs.first() {
        api_client.update_workflow(workflow.job_id, workflow_spec)?;
        return Ok(workflow.job_id);
    }

    api_client.create_workflow(workflow_spec)
}
