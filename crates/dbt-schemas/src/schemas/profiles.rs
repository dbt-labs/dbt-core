#![allow(unused_qualifications)]

use crate::schemas::relations::DEFAULT_DATABRICKS_DATABASE;
use crate::schemas::serde::{StringOrInteger, StringOrMap};

use dbt_serde_yaml::JsonSchema;
use merge::Merge;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;

pub type ProfileName = String;
pub type TargetName = String;
pub type DefaultTargetName = String;

#[derive(Debug, Deserialize)]
pub struct DbtProfilesIntermediate {
    pub config: Option<dbt_serde_yaml::Value>,
    #[serde(flatten)]
    pub profiles: HashMap<ProfileName, dbt_serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, JsonSchema)]
pub struct DbtProfiles {
    #[serde(flatten)]
    pub profiles: HashMap<ProfileName, DbConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
#[allow(clippy::large_enum_variant)]
pub enum DbConfig {
    Redshift(RedshiftDbConfig),
    Snowflake(SnowflakeDbConfig),
    Postgres(PostgresDbConfig),
    Bigquery(BigqueryDbConfig),
    Trino(TrinoDbConfig),
    Datafusion(DatafusionDbConfig),
    // SqlServer,
    // SingleStore,
    // Spark,
    Databricks(DatabricksDbConfig),
    // Hive,
    // Exasol,
    // Oracle,
    // Synapse,
    // Fabric,
    // Dremio,
    // ClickHouse,
    // Materialize,
    // Rockset,
    // Firebolt,
    // Teradata,
    // Athena,
    // Vertica,
    // TiDB,
    // #[serde(rename = "glue")]
    // AWSGlue,
    // MindsDB,
    // Greenplum,
    // Impala,
    // #[serde(rename = "layer_bigquery")]
    // LayerBigquery,
    // Iomete,
    // DuckDB,
    // SQLite,
    // MySQL,
    // IBMDB2,
    // AlloyDB,
    // Doris,
    // Infer,
    // Databend,
    // Fal,
    // Decodable,
    // Upsolver,
    // Starrocks,
}

impl DbConfig {
    pub fn get_execute_mode(&self) -> Execute {
        match self {
            DbConfig::Snowflake(config) => config.execute,
            DbConfig::Datafusion(_) => Execute::Local,
            _ => Execute::Remote,
        }
    }

    pub fn is_execute_local(&self) -> bool {
        matches!(self.get_execute_mode(), Execute::Local)
    }

    pub fn get_credential_value(&self) -> serde_json::Value {
        match self {
            DbConfig::Snowflake(config) => serde_json::to_value(config).unwrap(),
            DbConfig::Postgres(config) => serde_json::to_value(config).unwrap(),
            DbConfig::Bigquery(config) => serde_json::to_value(config).unwrap(),
            DbConfig::Trino(config) => serde_json::to_value(config).unwrap(),
            DbConfig::Datafusion(config) => serde_json::to_value(config).unwrap(),
            DbConfig::Redshift(config) => serde_json::to_value(config).unwrap(),
            DbConfig::Databricks(config) => serde_json::to_value(config).unwrap(),
        }
    }

    pub fn adapter_type(&self) -> String {
        match self {
            DbConfig::Redshift(..) => "redshift".to_string(),
            DbConfig::Snowflake(..) => "snowflake".to_string(),
            DbConfig::Postgres(..) => "postgres".to_string(),
            DbConfig::Bigquery(..) => "bigquery".to_string(),
            DbConfig::Trino(..) => "trino".to_string(),
            DbConfig::Datafusion(..) => "datafusion".to_string(),
            DbConfig::Databricks(..) => "databricks".to_string(),
        }
    }

    pub fn get_database(&self) -> Option<String> {
        match self {
            DbConfig::Redshift(config) => config.database.clone(),
            DbConfig::Snowflake(config) => config.database.clone(),
            DbConfig::Postgres(config) => config.database.clone().or(config.database.clone()),
            DbConfig::Bigquery(config) => config.database.clone(),
            DbConfig::Trino(config) => config.database.clone(),
            DbConfig::Datafusion(config) => config.database.clone(),
            DbConfig::Databricks(config) => config.database.clone(),
        }
    }

    pub fn get_schema(&self) -> Option<String> {
        match self {
            DbConfig::Redshift(config) => config.schema.clone(),
            DbConfig::Snowflake(config) => config.schema.clone(),
            DbConfig::Postgres(config) => config.schema.clone(),
            DbConfig::Trino(config) => config.schema.clone(),
            DbConfig::Bigquery(config) => config.schema.clone(),
            DbConfig::Datafusion(config) => config.schema.clone(),
            DbConfig::Databricks(config) => config.schema.clone(),
        }
    }

    pub fn get_threads(&self) -> Option<StringOrInteger> {
        match self {
            DbConfig::Snowflake(config) => config.threads.clone(),
            DbConfig::Databricks(config) => config.threads.clone(),
            DbConfig::Bigquery(config) => config.threads.clone(),
            DbConfig::Redshift(config) => config.threads.clone(),
            DbConfig::Postgres(config) => config.threads.clone(),
            DbConfig::Trino(config) => config.threads.clone(),
            _ => None,
        }
    }

    pub fn set_threads(&mut self, threads: Option<StringOrInteger>) {
        match self {
            DbConfig::Snowflake(config) => config.threads = threads,
            DbConfig::Databricks(config) => config.threads = threads,
            DbConfig::Postgres(config) => config.threads = threads,
            DbConfig::Bigquery(config) => config.threads = threads,
            DbConfig::Trino(config) => config.threads = threads,
            DbConfig::Redshift(config) => config.threads = threads,
            _ => (),
        }
    }

    pub fn get_connection_keys(&self) -> Vec<String> {
        match self {
            DbConfig::Snowflake(_) => vec![
                "account".to_string(),
                "user".to_string(),
                "database".to_string(),
                "warehouse".to_string(),
                "role".to_string(),
                "schema".to_string(),
                "authenticator".to_string(),
                "oauth_client_id".to_string(),
                "query_tag".to_string(),
                "client_session_keep_alive".to_string(),
                "host".to_string(),
                "port".to_string(),
                "proxy_host".to_string(),
                "proxy_port".to_string(),
                "protocol".to_string(),
                "connect_retries".to_string(),
                "connect_timeout".to_string(),
                "retry_on_database_errors".to_string(),
                "retry_all".to_string(),
                "insecure_mode".to_string(),
                "reuse_connections".to_string(),
            ],
            DbConfig::Postgres(_) => vec![
                "host".to_string(),
                "port".to_string(),
                "user".to_string(),
                "database".to_string(),
                "schema".to_string(),
                "connect_timeout".to_string(),
                "role".to_string(),
                "search_path".to_string(),
                "keepalives_idle".to_string(),
                "sslmode".to_string(),
                "sslcert".to_string(),
                "sslkey".to_string(),
                "sslrootcert".to_string(),
                "application_name".to_string(),
                "retries".to_string(),
            ],
            DbConfig::Bigquery(_) => vec![
                "method".to_string(),
                "database".to_string(),
                "execution_project".to_string(),
                "schema".to_string(),
                "location".to_string(),
                "priority".to_string(),
                "maximum_bytes_billed".to_string(),
                "impersonate_service_account".to_string(),
                "job_retry_deadline_seconds".to_string(),
                "job_retries".to_string(),
                "job_creation_timeout_seconds".to_string(),
                "job_execution_timeout_seconds".to_string(),
                "timeout_seconds".to_string(),
                "client_id".to_string(),
                "token_uri".to_string(),
                "compute_region".to_string(),
                "dataproc_cluster_name".to_string(),
                "gcs_bucket".to_string(),
                "dataproc_batch".to_string(),
            ],
            DbConfig::Redshift(_) => vec![
                "host".to_string(),
                "user".to_string(),
                "port".to_string(),
                "database".to_string(),
                "method".to_string(),
                "cluster_id".to_string(),
                "iam_profile".to_string(),
                "schema".to_string(),
                "sslmode".to_string(),
                "region".to_string(),
                "sslmode".to_string(),
                "autocreate".to_string(),
                "db_groups".to_string(),
                "ra3_node".to_string(),
                "connect_timeout".to_string(),
                "role".to_string(),
                "retries".to_string(),
                "retry_all".to_string(),
                "autocommit".to_string(),
                "access_key_id".to_string(),
                "is_serverless".to_string(),
                "serverless_work_group".to_string(),
                "serverless_acct_id".to_string(),
            ],
            DbConfig::Databricks(_) => vec![
                "host".to_string(),
                "http_path".to_string(),
                "schema".to_string(),
            ],
            _ => vec![],
        }
    }

    pub fn to_connection_dict(&self) -> HashMap<String, serde_json::Value> {
        let all_dict = self.to_dict();
        let connection_keys = self.get_connection_keys();
        all_dict
            .into_iter()
            .filter(|(key, _)| connection_keys.contains(key))
            .collect()
    }

    pub fn to_dict(&self) -> HashMap<String, serde_json::Value> {
        match self {
            DbConfig::Snowflake(config) => {
                // Serialize into json, then deserialize into a dictionary
                let json = serde_json::to_value(config).unwrap();
                serde_json::from_value(json).expect("Failed to deserialize Snowflake config")
            }
            DbConfig::Postgres(config) => {
                let json = serde_json::to_value(config).unwrap();
                serde_json::from_value(json).expect("Failed to deserialize Postgres config")
            }
            DbConfig::Bigquery(config) => {
                let json = serde_json::to_value(config).unwrap();
                serde_json::from_value(json).expect("Failed to deserialize Bigquery config")
            }
            DbConfig::Redshift(config) => {
                let json = serde_json::to_value(config).unwrap();
                serde_json::from_value(json).expect("Failed to deserialize Redshift config")
            }
            DbConfig::Databricks(config) => {
                let json = serde_json::to_value(config).unwrap();
                serde_json::from_value(json).expect("Failed to deserialize Databricks config")
            }
            _ => panic!("Unsupported database type: {self:?}"),
        }
    }

    pub fn get_aliases(&self) -> Vec<String> {
        // TODO: Implement Aliases for databases that need them. Snowflake does not need aliases.
        vec![]
    }

    pub fn ignored_properties(&self) -> HashMap<String, serde_json::Value> {
        match self {
            DbConfig::Snowflake(config) => config.ignored_properties.clone(),
            DbConfig::Postgres(config) => config.ignored_properties.clone(),
            DbConfig::Bigquery(config) => config.ignored_properties.clone(),
            DbConfig::Trino(config) => config.ignored_properties.clone(),
            DbConfig::Datafusion(config) => config.ignored_properties.clone(),
            DbConfig::Redshift(config) => config.ignored_properties.clone(),
            DbConfig::Databricks(config) => config.ignored_properties.clone(),
        }
    }

    pub fn get_unique_field(&self) -> Option<String> {
        match self {
            DbConfig::Snowflake(config) => config.account.clone(),
            DbConfig::Postgres(config) => config.host.clone(),
            DbConfig::Bigquery(config) => config.database.clone(),
            DbConfig::Trino(config) => config.host.clone(),
            DbConfig::Datafusion(config) => config.database.clone(),
            DbConfig::Redshift(config) => config.host.clone(),
            DbConfig::Databricks(config) => config.host.clone(),
        }
    }

    pub fn get_adapter_unique_id(&self) -> Option<String> {
        /*
        Generates a hash of a database-specific unique field (eg. hostname on redshift, account on snowflake, etc.)
        Used for telemetry to anonymously identify a data warehouse.
        */
        let unique_field = self.get_unique_field();
        unique_field.map(|unique_field| format!("{:x}", md5::compute(unique_field.as_bytes())))
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Execute {
    #[default]
    Remote,
    Local,
}

impl Display for Execute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Execute::Remote => write!(f, "remote"),
            Execute::Local => write!(f, "local"),
        }
    }
}

impl std::str::FromStr for Execute {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "remote" => Ok(Execute::Remote),
            "local" => Ok(Execute::Local),
            _ => Err(format!("Invalid execute mode: {s}")),
        }
    }
}

impl Execute {
    pub fn is_default(&self) -> bool {
        matches!(self, Execute::Remote)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DbTargets {
    #[serde(rename = "target", default = "default_target")]
    pub default_target: DefaultTargetName,
    pub outputs: HashMap<TargetName, serde_json::Value>,
}

fn default_target() -> String {
    "default".to_string()
}

/// Extend merge_strategies from `merge` crate
mod merge_strategies_extend {
    pub fn overwrite_always<T>(left: &mut T, right: T) {
        *left = right;
    }

    pub fn overwrite_option<T>(left: &mut Option<T>, right: Option<T>) {
        if left.is_none() {
            *left = right;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema, Merge)]
#[serde(rename_all = "snake_case")]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
pub struct RedshiftDbConfig {
    // Configuration Parameters
    pub port: Option<StringOrInteger>, // Setting as Option but required as of dbt 1.7.1
    #[serde(alias = "dbname")] // Same as Postgres, it allows either dbname or database
    pub database: Option<String>, // Setting as Option but required as of dbt 1.7.1
    pub schema: Option<String>,        // Setting as Option but required as of dbt 1.7.1
    pub connect_timeout: Option<i64>,
    pub sslmode: Option<String>,
    pub role: Option<String>,
    pub autocreate: Option<bool>,
    pub db_groups: Option<Vec<String>>,
    pub ra3_node: Option<bool>,
    pub autocommit: Option<bool>,
    pub retries: Option<i64>,
    // Authentication Parameters (Password)
    pub method: Option<String>,
    pub host: Option<String>, // Setting as Option but required as of dbt 1.7.1
    pub user: Option<String>, // Setting as Option but required as of dbt 1.7.1
    pub password: Option<String>,
    // Authentication Parameters (IAM)
    pub iam_profile: Option<String>,
    pub cluster_id: Option<String>,
    pub region: Option<String>,
    pub threads: Option<StringOrInteger>,
    #[serde(flatten)]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub ignored_properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct SnowflakeDbConfig {
    // Configuration Parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_session_keep_alive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_all: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_on_database_errors: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_retries: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reuse_connections: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authenticator: Option<String>,
    // Authentication Parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key_passphrase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<String>,
    #[serde(default, skip_serializing_if = "Execute::is_default")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub execute: Execute,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_client_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(flatten)]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub ignored_properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct PostgresDbConfig {
    // Configuration Parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<StringOrInteger>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "dbname")] // Postgres allows either dbname or database
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keepalives_idle: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sslcert: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sslkey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sslrootcert: Option<String>,
    // Authentication Parameters (Password)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(flatten)]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub ignored_properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct BigqueryDbConfig {
    pub threads: Option<StringOrInteger>,
    pub profile_type: Option<String>,
    #[serde(alias = "project")]
    pub database: Option<String>,
    #[serde(alias = "dataset")]
    pub schema: Option<String>,
    pub timeout_seconds: Option<i64>,
    pub priority: Option<String>,
    pub method: Option<String>,
    pub maximum_bytes_billed: Option<i64>,
    pub impersonate_service_account: Option<String>,
    pub refresh_token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub token_uri: Option<String>,
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyfile: Option<String>,
    pub retries: Option<i64>,
    pub location: Option<String>,
    pub scopes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyfile_json: Option<StringOrMap>,
    pub execution_project: Option<String>,
    pub compute_region: Option<String>,
    // TODO: support this https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup
    pub dataproc_batch: Option<Value>,
    pub dataproc_cluster_name: Option<String>,
    pub dataproc_region: Option<String>,
    pub gcs_bucket: Option<String>,
    pub job_creation_timeout_seconds: Option<String>,
    pub job_execution_timeout_seconds: Option<String>,
    pub job_retries: Option<i64>,
    pub job_retry_deadline_seconds: Option<String>,
    pub target_name: Option<String>,

    #[serde(flatten)]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub ignored_properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct TrinoDbConfig {
    // Configuration Parameters
    pub port: Option<StringOrInteger>, // Setting as Option but required as of dbt 1.7.1
    pub user: Option<String>,          // Setting as Option but required as of dbt 1.7.1
    pub database: Option<String>,
    pub host: Option<String>, // Setting as Option but required as of dbt 1.7.1/ Setting as Option but required as of dbt 1.7.1
    pub schema: Option<String>,
    pub threads: Option<StringOrInteger>,
    pub password: Option<String>,
    pub role: Option<String>,
    #[serde(flatten)]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub ignored_properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct DatafusionDbConfig {
    pub database: Option<String>,
    pub schema: Option<String>,
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub execute: Execute,
    #[serde(flatten)]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub ignored_properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct DatabricksDbConfig {
    #[serde(alias = "catalog", default = "default_databricks_database")]
    pub database: Option<String>,
    pub schema: Option<String>,
    pub host: Option<String>,
    pub http_path: Option<String>,
    pub token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub oauth_redirect_url: Option<String>,
    pub oauth_scopes: Option<Vec<String>>,
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub session_properties: Option<HashMap<String, serde_json::Value>>,
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub connection_parameters: Option<HashMap<String, serde_json::Value>>,
    pub auth_type: Option<String>,
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub compute: Option<HashMap<String, serde_json::Value>>,
    pub connect_retries: Option<i32>,
    pub connect_timeout: Option<i32>,
    pub retry_all: Option<bool>,
    pub connect_max_idle: Option<i32>,
    pub threads: Option<StringOrInteger>,
    #[serde(flatten)]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub ignored_properties: HashMap<String, serde_json::Value>,
}

fn default_databricks_database() -> Option<String> {
    Some(DEFAULT_DATABRICKS_DATABASE.to_string())
}

#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum TargetContext {
    Snowflake(SnowflakeTargetEnv),
    Trino(TrinoTargetEnv),
    Datafusion(DatafusionTargetEnv),
    Postgres(PostgresTargetEnv),
    Bigquery(BigqueryTargetEnv),
    Databricks(DatabricksTargetEnv),
    Redshift(RedshiftTargetEnv),
    // Add other variants as needed
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TrinoTargetEnv {
    #[serde(flatten)]
    pub common: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DatafusionTargetEnv {
    pub database: String,
    #[serde(flatten)]
    pub common: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PostgresTargetEnv {
    pub dbname: String,
    pub host: String,
    pub user: String,
    pub port: StringOrInteger,
    #[serde(flatten)]
    pub common: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct SnowflakeTargetEnv {
    pub warehouse: String,
    pub user: String,
    pub role: Option<String>,
    pub account: String,
    #[serde(flatten)]
    pub common: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct BigqueryTargetEnv {
    pub project: String,
    pub dataset: String,
    pub client_id: Option<String>,
    pub compute_region: Option<String>,
    pub dataproc_batch: Option<Value>,
    pub dataproc_cluster_name: Option<String>,
    pub dataproc_region: Option<String>,
    pub execution_project: Option<String>,
    pub gcs_bucket: Option<String>,
    pub impersonate_service_account: Option<String>,
    pub job_creation_timeout_seconds: Option<String>,
    pub job_execution_timeout_seconds: Option<String>,
    pub job_retries: Option<i64>,
    pub job_retry_deadline_seconds: Option<String>,
    pub location: Option<String>,
    pub maximum_bytes_billed: Option<i64>,
    pub method: Option<String>,
    pub priority: Option<String>,
    pub retries: Option<i64>,
    pub target_name: Option<String>,
    pub timeout_seconds: Option<i64>,
    pub token_uri: Option<String>,
    #[serde(flatten)]
    pub common: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct CommonTargetContext {
    pub database: String,
    pub schema: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub threads: Option<u16>,
}

#[derive(Serialize, JsonSchema)]
pub struct DatabricksTargetEnv {
    #[serde(flatten)]
    pub common: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct RedshiftTargetEnv {
    pub dbname: String,
    pub host: String,
    pub user: String,
    pub port: StringOrInteger,
    #[serde(flatten)]
    pub common: CommonTargetContext,
}

fn missing(field: &str) -> String {
    format!("In file `profiles.yml`, field `{field}` is required.")
}

// This target context is only to be used in rendering yml's
// See: https://docs.getdbt.com/reference/dbt-jinja-functions/target
impl TryFrom<DbConfig> for TargetContext {
    type Error = String;

    fn try_from(db_config: DbConfig) -> Result<Self, Self::Error> {
        let adapter_type = db_config.adapter_type();
        match db_config {
            // Snowflake case
            DbConfig::Snowflake(config) => {
                let database = config.database.ok_or_else(|| missing("database"))?;
                Ok(TargetContext::Snowflake(SnowflakeTargetEnv {
                    warehouse: config.warehouse.ok_or_else(|| missing("warehouse"))?,
                    user: config.user.ok_or_else(|| missing("user"))?,
                    role: config.role.clone(),
                    account: config.account.ok_or_else(|| missing("account"))?,
                    common: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: match config.threads {
                            Some(StringOrInteger::String(threads)) => {
                                Some(threads.parse::<u16>().map_err(|_| {
                                    "threads must be a positive integer".to_string()
                                })?)
                            }
                            Some(StringOrInteger::Integer(threads)) => Some(threads as u16),
                            None => None,
                        },
                    },
                }))
            }

            // Trino case
            DbConfig::Trino(config) => {
                let database = config.database.ok_or_else(|| missing("database"))?;
                Ok(TargetContext::Trino(TrinoTargetEnv {
                    common: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: match config.threads {
                            Some(StringOrInteger::String(threads)) => {
                                Some(threads.parse::<u16>().map_err(|_| {
                                    "threads must be a positive integer".to_string()
                                })?)
                            }
                            Some(StringOrInteger::Integer(threads)) => Some(threads as u16),
                            None => None,
                        },
                    },
                }))
            }

            // Datafusion case
            DbConfig::Datafusion(config) => {
                let database = config.database.ok_or_else(|| missing("database"))?;
                Ok(TargetContext::Datafusion(DatafusionTargetEnv {
                    database: database.clone(),
                    common: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: None, // Assuming Datafusion does not have threads configuration
                    },
                }))
            }

            DbConfig::Postgres(config) => {
                let database = config
                    .database
                    .ok_or_else(|| missing("dbname or database"))?;
                Ok(TargetContext::Postgres(PostgresTargetEnv {
                    dbname: database.clone(),
                    host: config.host.ok_or_else(|| missing("host"))?,
                    user: config.user.ok_or_else(|| missing("user"))?,
                    port: config.port.ok_or_else(|| missing("port"))?,
                    common: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: None,
                    },
                }))
            }

            // Bigquery case
            DbConfig::Bigquery(config) => {
                let database = config
                    .database
                    .ok_or_else(|| missing("database or project"))?;
                let schema = config.schema.ok_or_else(|| missing("schema or dataset"))?;
                Ok(TargetContext::Bigquery(BigqueryTargetEnv {
                    project: database.clone(),
                    dataset: schema.clone(),
                    common: CommonTargetContext {
                        database,
                        schema,
                        type_: adapter_type,
                        threads: None,
                    },
                    client_id: config.client_id.clone(),
                    compute_region: config.compute_region.clone(),
                    dataproc_batch: config.dataproc_batch.clone(),
                    dataproc_cluster_name: config.dataproc_cluster_name.clone(),
                    dataproc_region: config.dataproc_region.clone(),
                    execution_project: config.execution_project.clone(),
                    gcs_bucket: config.gcs_bucket.clone(),
                    impersonate_service_account: config.impersonate_service_account.clone(),
                    job_creation_timeout_seconds: config.job_creation_timeout_seconds.clone(),
                    job_execution_timeout_seconds: config.job_execution_timeout_seconds.clone(),
                    job_retries: config.job_retries,
                    job_retry_deadline_seconds: config.job_retry_deadline_seconds.clone(),
                    location: config.location.clone(),
                    maximum_bytes_billed: config.maximum_bytes_billed,
                    method: config.method.clone(),
                    priority: config.priority.clone(),
                    retries: config.retries,
                    target_name: config.target_name.clone(),
                    timeout_seconds: config.timeout_seconds,
                    token_uri: config.token_uri,
                }))
            }

            DbConfig::Databricks(config) => {
                let database = config
                    .database
                    .unwrap_or_else(|| DEFAULT_DATABRICKS_DATABASE.to_string());
                Ok(TargetContext::Databricks(DatabricksTargetEnv {
                    common: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: None,
                    },
                }))
            }

            DbConfig::Redshift(config) => {
                let database = config
                    .database
                    .ok_or_else(|| missing("dbname or database"))?;
                Ok(TargetContext::Redshift(RedshiftTargetEnv {
                    dbname: database.clone(),
                    host: config.host.ok_or_else(|| missing("host"))?,
                    user: config.user.ok_or_else(|| missing("user"))?,
                    port: config.port.ok_or_else(|| missing("port"))?,
                    common: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: None,
                    },
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snowflake_adapter_unique_id() {
        let config = DbConfig::Snowflake(SnowflakeDbConfig {
            account: Some("kw27752".to_string()),
            ..Default::default()
        });

        assert_eq!(config.get_unique_field(), Some("kw27752".to_string()));
        assert_eq!(
            config.get_adapter_unique_id(),
            Some("c27a9a57d35df4a8f81aec929cbdc7cd".to_string())
        );
    }

    #[test]
    fn test_snowflake_adapter_unique_id_with_missing_account() {
        let config = DbConfig::Snowflake(SnowflakeDbConfig {
            account: None,
            ..Default::default()
        });

        assert_eq!(config.get_unique_field(), None);
        assert_eq!(config.get_adapter_unique_id(), None);
    }
}
