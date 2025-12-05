use crate::{AdapterResponse, TypedBaseAdapter};

use crate::databricks::api_client::DatabricksApiClient;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};
use serde_json::json;

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
        .unwrap_or_else(|| "job_cluster".to_string());

    if submission_method != "job_cluster" {
        return Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            format!(
                "Only 'job_cluster' submission method is currently supported, got: {}",
                submission_method
            ),
        ));
    }

    let job_cluster_config = config.get_attr("job_cluster_config").ok().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            "'job_cluster_config' is required for job_cluster submission method",
        )
    })?;

    validate_job_cluster_config(&job_cluster_config)?;

    let packages = extract_packages(&config);
    let timeout = config
        .get_attr("timeout")
        .ok()
        .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()))
        .unwrap_or(86400);

    let create_notebook = config
        .get_attr("create_notebook")
        .ok()
        .map(|v| v.is_true())
        .unwrap_or(true);

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

    let use_user_folder_for_python = config
        .get_attr("user_folder_for_python")
        .ok()
        .map(|v| v.is_true())
        .unwrap_or(false);

    let api_client = DatabricksApiClient::new(adapter, use_user_folder_for_python)?;

    let run_name = format!(
        "{}-{}-{}-{}",
        catalog,
        schema,
        identifier,
        uuid::Uuid::new_v4()
    );

    let notebook_path = upload_notebook(
        &api_client,
        &catalog,
        &schema,
        &identifier,
        compiled_code,
        create_notebook,
    )?;

    let job_spec = build_job_spec(
        &notebook_path,
        &job_cluster_config,
        &packages,
        index_url.as_deref(),
        &additional_libs,
    )?;

    let run_id = submit_job_run(&api_client, &run_name, &job_spec, timeout)?;

    poll_job_completion(&api_client, &run_id, timeout)?;

    Ok(AdapterResponse {
        message: format!(
            "Python model executed successfully via job_cluster. Run ID: {}, Notebook: {}",
            run_id, notebook_path
        ),
        code: "OK".to_string(),
        rows_affected: 0,
        query_id: Some(run_id),
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

fn build_job_spec(
    notebook_path: &str,
    job_cluster_config: &Value,
    packages: &[String],
    index_url: Option<&str>,
    additional_libs: &[serde_json::Value],
) -> AdapterResult<serde_json::Value> {
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

    let cluster_config_json = serde_json::to_value(job_cluster_config).map_err(|e| {
        AdapterError::new(
            AdapterErrorKind::Internal,
            format!("Failed to serialize job_cluster_config: {}", e),
        )
    })?;

    let job_spec = json!({
        "task_key": "inner_notebook",
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "WORKSPACE"
        },
        "new_cluster": cluster_config_json,
        "libraries": libraries
    });

    Ok(job_spec)
}

fn submit_job_run(
    api_client: &DatabricksApiClient,
    run_name: &str,
    job_spec: &serde_json::Value,
    timeout_seconds: u64,
) -> AdapterResult<String> {
    api_client.submit_job_run(run_name, job_spec, timeout_seconds)
}

fn poll_job_completion(
    api_client: &DatabricksApiClient,
    run_id: &str,
    timeout_seconds: u64,
) -> AdapterResult<()> {
    api_client.poll_job_completion(run_id, timeout_seconds)
}
