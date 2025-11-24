use crate::{AdapterResponse, TypedBaseAdapter};
use std::collections::HashMap;

use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_serde_yaml::Value::Mapping;
use dbt_serde_yaml::{Error, Value as YmlValue};
use dbt_xdbc::bigquery::{
    CREATE_BATCH_REQ_BATCH_ID, CREATE_BATCH_REQ_BATCH_YML, CREATE_BATCH_REQ_PARENT,
    DATAPROC_POOLING_TIMEOUT, DATAPROC_PROJECT, DATAPROC_REGION,
    DATAPROC_SUBMIT_JOB_REQ_CLUSTER_NAME, DATAPROC_SUBMIT_JOB_REQ_GCS_PATH, WRITE_GCS_BUCKET,
    WRITE_GCS_CONTENT, WRITE_GCS_OBJECT_NAME,
};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};

const DEFAULT_JAR_FILE_URI: &str =
    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar";

#[derive(Debug, Clone, PartialEq)]
enum SubmissionMethod {
    Cluster,
    Serverless,
    BigFrames,
}

pub struct JobContext<'a> {
    pub adapter: &'a dyn TypedBaseAdapter,
    pub ctx: &'a QueryCtx,
    pub conn: &'a mut dyn Connection,
    pub config: &'a Value,
}

pub struct ClusterJobParams<'a> {
    pub project: &'a str,
    pub region: &'a str,
    pub gcs_path: &'a str,
    pub timeout: i64,
}

pub struct ServerlessJobParams<'a> {
    pub project: &'a str,
    pub region: &'a str,
    pub gcs_path: &'a str,
    pub batch_id: &'a str,
    pub timeout: i64,
}

impl SubmissionMethod {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "cluster" => Some(Self::Cluster),
            "serverless" => Some(Self::Serverless),
            "bigframes" => Some(Self::BigFrames),
            _ => None,
        }
    }
}

pub fn submit_python_job(
    adapter: &dyn TypedBaseAdapter,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    _state: &State,
    model: &Value,
    compiled_code: &str,
) -> AdapterResult<AdapterResponse> {
    let config = model
        .get_attr("config")
        .map_err(|e| AdapterError::new(AdapterErrorKind::Internal, e.to_string()))?;

    let schema = model
        .get_attr("schema")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .ok_or_else(|| AdapterError::new(AdapterErrorKind::Internal, "schema is required"))?;

    let alias = model
        .get_attr("alias")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .ok_or_else(|| AdapterError::new(AdapterErrorKind::Internal, "alias is required"))?;

    let database = adapter
        .get_db_config("database")
        .map(|v| v.to_string())
        .unwrap_or_default();

    let execution_project = adapter
        .get_db_config("execution_project")
        .map(|v| v.to_string())
        .unwrap_or(database);

    let compute_region = adapter
        .get_db_config("compute_region")
        .map(|v| v.to_string())
        .unwrap_or_default();

    let gcs_bucket = adapter
        .get_db_config("gcs_bucket")
        .map(|v| v.to_string())
        .unwrap_or_default();

    if compute_region.is_empty() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "Need to supply compute_region in profile to submit python job",
        ));
    }

    if gcs_bucket.is_empty() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "Need to supply gcs_bucket in profile to submit python job",
        ));
    }

    let submission_method = match adapter
        .get_db_config("submission_method")
        .map(|v| v.to_string())
    {
        None => SubmissionMethod::Serverless,
        Some(v) => SubmissionMethod::from_str(&v.to_string()).ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Internal,
                format!("Invalid submission method: {}", v),
            )
        })?,
    };

    let job_execution_timeout = adapter
        .get_db_config("job_execution_timeout_seconds")
        .map(|v| v.to_string())
        .map(|v| {
            v.parse::<i64>()
                .expect("job_execution_timeout_seconds must be a valid integer")
        });

    let timeout = config
        .get_attr("timeout")
        .ok()
        .and_then(|v| v.as_i64())
        .or(job_execution_timeout)
        .unwrap_or(24 * 60 * 60);

    let batch_id = config
        .get_attr("batch_id")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let model_file_name = format!("{}/{}.py", schema, alias);
    let gcs_path = format!("gs://{}/{}", gcs_bucket, model_file_name);

    let options: HashMap<_, _> = vec![
        (WRITE_GCS_BUCKET.to_string(), gcs_bucket),
        (WRITE_GCS_OBJECT_NAME.to_string(), model_file_name),
        (WRITE_GCS_CONTENT.to_string(), compiled_code.to_string()),
    ]
    .into_iter()
    .collect();

    adapter.execute(
        None,
        conn,
        ctx,
        "SELECT 1",
        false,
        false,
        None,
        Some(options),
    )?;

    let job_ctx = JobContext {
        adapter,
        ctx,
        conn,
        config: &config,
    };

    match submission_method {
        SubmissionMethod::Cluster => {
            let params = ClusterJobParams {
                project: execution_project.as_str(),
                region: compute_region.as_str(),
                gcs_path: gcs_path.as_str(),
                timeout,
            };
            submit_cluster_job(job_ctx, params)
        }
        SubmissionMethod::Serverless => {
            let params = ServerlessJobParams {
                project: execution_project.as_str(),
                region: compute_region.as_str(),
                gcs_path: gcs_path.as_str(),
                batch_id: batch_id.as_str(),
                timeout,
            };
            submit_serverless_job(job_ctx, params)
        }
        SubmissionMethod::BigFrames => Err(AdapterError::new(
            AdapterErrorKind::Internal,
            "submission_method=bigframes is not implemented yet. use submission_method=serverless or submission_method=cluster instead",
        )),
    }
}

fn submit_cluster_job(
    job_ctx: JobContext,
    params: ClusterJobParams,
) -> AdapterResult<AdapterResponse> {
    let proj_dataproc_cluster_name = job_ctx
        .adapter
        .get_db_config("dataproc_cluster_name")
        .map(|v| v.to_string());

    let dataproc_cluster_name = job_ctx.config
        .get_attr("dataproc_cluster_name")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .or(proj_dataproc_cluster_name)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "Need to supply dataproc_cluster_name in profile or config to submit python job with cluster submission method",
            )
        })?;

    let options: HashMap<_, _> = vec![
        (DATAPROC_REGION.to_string(), params.region.to_string()),
        (DATAPROC_PROJECT.to_string(), params.project.to_string()),
        (
            DATAPROC_POOLING_TIMEOUT.to_string(),
            params.timeout.to_string(),
        ),
        (
            DATAPROC_SUBMIT_JOB_REQ_CLUSTER_NAME.to_string(),
            dataproc_cluster_name,
        ),
        (
            DATAPROC_SUBMIT_JOB_REQ_GCS_PATH.to_string(),
            params.gcs_path.to_string(),
        ),
    ]
    .into_iter()
    .collect();

    let response = job_ctx.adapter.execute(
        None,
        job_ctx.conn,
        job_ctx.ctx,
        "SELECT 1",
        false,
        false,
        None,
        Some(options),
    )?;

    Ok(response.0)
}

fn submit_serverless_job(
    job_ctx: JobContext,
    params: ServerlessJobParams,
) -> AdapterResult<AdapterResponse> {
    let jar_file_uri = job_ctx
        .config
        .get_attr("jar_file_uri")
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| DEFAULT_JAR_FILE_URI.to_string());

    let dataproc_batch_config = job_ctx.adapter.get_db_config_value("dataproc_batch");
    let batch = batch_to_str(
        params.gcs_path.to_string(),
        jar_file_uri,
        dataproc_batch_config,
    )?;

    let options: HashMap<_, _> = vec![
        (DATAPROC_REGION.to_string(), params.region.to_string()),
        (
            CREATE_BATCH_REQ_PARENT.to_string(),
            format!("projects/{}/locations/{}", params.project, params.region),
        ),
        (CREATE_BATCH_REQ_BATCH_YML.to_string(), batch),
        (
            CREATE_BATCH_REQ_BATCH_ID.to_string(),
            params.batch_id.to_string(),
        ),
        (
            DATAPROC_POOLING_TIMEOUT.to_string(),
            params.timeout.to_string(),
        ),
    ]
    .into_iter()
    .collect();

    let response = job_ctx.adapter.execute(
        None,
        job_ctx.conn,
        job_ctx.ctx,
        "SELECT 1",
        false,
        false,
        None,
        Some(options),
    )?;

    Ok(response.0)
}

pub fn batch_to_str(
    gcs_path: String,
    jar_file_uri: String,
    overrides: Option<&YmlValue>,
) -> Result<String, Error> {
    let mut batch = default_batch_value(gcs_path, jar_file_uri);

    if let Some(ovr) = overrides {
        merge_yaml(&mut batch, ovr);
    }

    let yaml_string = dbt_serde_yaml::to_string(&batch)?;

    Ok(yaml_string)
}

fn default_batch_value(gcs_path: String, jar_file_uri: String) -> YmlValue {
    let yaml_str = format!(
        r#"
runtime_config:
  version: "1.1"
  properties:
    spark.executor.instances: "2"
pyspark_batch:
  main_python_file_uri: "{}"
  jar_file_uris:
    - "{}"
"#,
        gcs_path, jar_file_uri
    );

    dbt_serde_yaml::from_str(yaml_str.as_str()).unwrap()
}

fn merge_yaml(base: &mut YmlValue, overrides: &YmlValue) {
    if overrides.is_null() {
        return;
    }

    match (base, overrides) {
        (Mapping(base_map, ..), Mapping(override_map, ..)) => {
            for (k, v) in override_map {
                if let Some(existing) = base_map.get_mut(k) {
                    merge_yaml(existing, v);
                } else {
                    base_map.insert(k.clone(), v.clone());
                }
            }
        }
        (base_slot, new_val) => {
            *base_slot = new_val.clone();
        }
    }
}
