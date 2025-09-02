#![allow(unused_qualifications)]

use crate::schemas::relations::DEFAULT_DATABRICKS_DATABASE;
use crate::schemas::serde::{StringOrInteger, StringOrMap};
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::UntaggedEnumDeserialize;
use merge::Merge;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;

type YmlValue = dbt_serde_yaml::Value;

pub type ProfileName = String;
pub type TargetName = String;
pub type DefaultTargetName = String;

#[derive(Debug, Deserialize)]
pub struct DbtProfilesIntermediate {
    pub config: Option<dbt_serde_yaml::Value>,
    pub __profiles__: HashMap<ProfileName, dbt_serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, JsonSchema)]
pub struct DbtProfiles {
    pub __profiles__: HashMap<ProfileName, DbConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, UntaggedEnumDeserialize, JsonSchema)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
#[allow(clippy::large_enum_variant)]
pub enum DbConfig {
    Redshift(Box<RedshiftDbConfig>),
    Snowflake(Box<SnowflakeDbConfig>),
    Postgres(Box<PostgresDbConfig>),
    Bigquery(Box<BigqueryDbConfig>),
    Trino(Box<TrinoDbConfig>),
    Datafusion(Box<DatafusionDbConfig>),
    // SqlServer,
    // SingleStore,
    // Spark,
    Databricks(Box<DatabricksDbConfig>),
    Salesforce(Box<SalesforceDbConfig>),
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

macro_rules! impl_from_db_config {
    ($variant:ident, $config_type:ty) => {
        impl From<$config_type> for DbConfig {
            fn from(config: $config_type) -> Self {
                DbConfig::$variant(Box::new(config))
            }
        }
    };
}

impl_from_db_config!(Redshift, RedshiftDbConfig);
impl_from_db_config!(Snowflake, SnowflakeDbConfig);
impl_from_db_config!(Postgres, PostgresDbConfig);
impl_from_db_config!(Bigquery, BigqueryDbConfig);
impl_from_db_config!(Trino, TrinoDbConfig);
impl_from_db_config!(Datafusion, DatafusionDbConfig);
impl_from_db_config!(Databricks, DatabricksDbConfig);

impl DbConfig {
    pub fn get_unique_field(&self) -> Option<&String> {
        match self {
            DbConfig::Snowflake(config) => config.account.as_ref(),
            DbConfig::Postgres(config) => config.host.as_ref(),
            DbConfig::Bigquery(config) => config.database.as_ref(),
            DbConfig::Trino(config) => config.host.as_ref(),
            DbConfig::Datafusion(config) => config.database.as_ref(),
            DbConfig::Redshift(config) => config.host.as_ref(),
            DbConfig::Databricks(config) => config.host.as_ref(),
            DbConfig::Salesforce(config) => config.client_id.as_ref(),
        }
    }

    pub fn get_adapter_unique_id(&self) -> Option<String> {
        // Generates a hash of a database-specific unique field (eg. hostname on redshift,
        // account on snowflake). Used for telemetry to anonymously identify a data warehouse.
        self.get_unique_field()
            .map(|unique_field| format!("{:x}", md5::compute(unique_field.as_bytes())))
    }

    // XXX: this outdated and it affects the `dbt debug` command. A review is pending.
    pub fn get_connection_keys(&self) -> &'static [&'static str] {
        match self {
            DbConfig::Snowflake(_) => &[
                "account",
                "user",
                "database",
                "warehouse",
                "role",
                "schema",
                "authenticator",
                "oauth_client_id",
                "query_tag",
                "client_session_keep_alive",
                "host",
                "port",
                "proxy_host",
                "proxy_port",
                "protocol",
                "connect_retries",
                "connect_timeout",
                "retry_on_database_errors",
                "retry_all",
                "insecure_mode",
                "reuse_connections",
            ],
            DbConfig::Postgres(_) => &[
                "host",
                "port",
                "user",
                "database",
                "schema",
                "connect_timeout",
                "role",
                "search_path",
                "keepalives_idle",
                "sslmode",
                "sslcert",
                "sslkey",
                "sslrootcert",
                "application_name",
                "retries",
            ],
            DbConfig::Bigquery(_) => &[
                "method",
                "database",
                "execution_project",
                "schema",
                "location",
                "priority",
                "maximum_bytes_billed",
                "impersonate_service_account",
                "job_retry_deadline_seconds",
                "job_retries",
                "job_creation_timeout_seconds",
                "job_execution_timeout_seconds",
                "timeout_seconds",
                "client_id",
                "token_uri",
                "compute_region",
                "dataproc_cluster_name",
                "gcs_bucket",
                "dataproc_batch",
            ],
            DbConfig::Redshift(_) => &[
                "host",
                "user",
                "port",
                "database",
                "method",
                "cluster_id",
                "iam_profile",
                "schema",
                "sslmode",
                "region",
                "sslmode",
                "autocreate",
                "db_groups",
                "ra3_node",
                "connect_timeout",
                "role",
                "retries",
                "retry_all",
                "autocommit",
                "access_key_id",
                "is_serverless",
                "serverless_work_group",
                "serverless_acct_id",
            ],
            DbConfig::Databricks(_) => &["host", "http_path", "schema"],
            // TODO: Salesforce connection keys
            DbConfig::Salesforce(_) => &[],
            // TODO: Trino and Datafusion connection keys
            DbConfig::Trino(_) => &[],
            DbConfig::Datafusion(_) => &[],
        }
    }

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

    pub fn get_execution_timezone(&self) -> Option<String> {
        match self {
            DbConfig::Snowflake(config) => config.execution_timezone.clone(),
            _ => None,
        }
    }

    pub fn to_yaml_value(&self) -> Result<YmlValue, dbt_serde_yaml::Error> {
        match self {
            DbConfig::Snowflake(config) => dbt_serde_yaml::to_value(config),
            DbConfig::Postgres(config) => dbt_serde_yaml::to_value(config),
            DbConfig::Bigquery(config) => dbt_serde_yaml::to_value(config),
            DbConfig::Trino(config) => dbt_serde_yaml::to_value(config),
            DbConfig::Datafusion(config) => dbt_serde_yaml::to_value(config),
            DbConfig::Redshift(config) => dbt_serde_yaml::to_value(config),
            DbConfig::Databricks(config) => dbt_serde_yaml::to_value(config),
            DbConfig::Salesforce(config) => dbt_serde_yaml::to_value(config),
        }
    }

    // TODO: change to enum AdapterType
    pub fn adapter_type(&self) -> &str {
        match self {
            DbConfig::Redshift(..) => "redshift",
            DbConfig::Snowflake(..) => "snowflake",
            DbConfig::Postgres(..) => "postgres",
            DbConfig::Bigquery(..) => "bigquery",
            DbConfig::Trino(..) => "trino",
            DbConfig::Datafusion(..) => "datafusion",
            DbConfig::Databricks(..) => "databricks",
            DbConfig::Salesforce(..) => "salesforce",
        }
    }

    pub fn get_database(&self) -> Option<&String> {
        match self {
            DbConfig::Redshift(config) => config.database.as_ref(),
            DbConfig::Snowflake(config) => config.database.as_ref(),
            DbConfig::Postgres(config) => config.database.as_ref().or(config.database.as_ref()),
            DbConfig::Bigquery(config) => config.database.as_ref(),
            DbConfig::Trino(config) => config.database.as_ref(),
            DbConfig::Datafusion(config) => config.database.as_ref(),
            DbConfig::Databricks(config) => config.database.as_ref(),
            DbConfig::Salesforce(config) => config.database.as_ref(),
        }
    }

    pub fn get_schema(&self) -> Option<&String> {
        match self {
            DbConfig::Redshift(config) => config.schema.as_ref(),
            DbConfig::Snowflake(config) => config.schema.as_ref(),
            DbConfig::Postgres(config) => config.schema.as_ref(),
            DbConfig::Trino(config) => config.schema.as_ref(),
            DbConfig::Bigquery(config) => config.schema.as_ref(),
            DbConfig::Datafusion(config) => config.schema.as_ref(),
            DbConfig::Databricks(config) => config.schema.as_ref(),
            DbConfig::Salesforce(_) => None,
        }
    }

    pub fn get_threads(&self) -> Option<&StringOrInteger> {
        match self {
            DbConfig::Snowflake(config) => config.threads.as_ref(),
            DbConfig::Databricks(config) => config.threads.as_ref(),
            DbConfig::Bigquery(config) => config.threads.as_ref(),
            DbConfig::Redshift(config) => config.threads.as_ref(),
            DbConfig::Postgres(config) => config.threads.as_ref(),
            DbConfig::Trino(config) => config.threads.as_ref(),
            DbConfig::Datafusion(_) => None,
            DbConfig::Salesforce(_) => None,
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
            DbConfig::Datafusion(_) => (),
            DbConfig::Salesforce(_) => (),
        }
    }

    pub fn to_connection_mapping(&self) -> Result<dbt_serde_yaml::Mapping, dbt_serde_yaml::Error> {
        let connection_keys = self.get_connection_keys();
        let mapping = self.to_mapping()?;
        let filtered = mapping
            .into_iter()
            .filter(|(key, _)| {
                key.as_str()
                    .map(|s| connection_keys.contains(&s))
                    .unwrap_or(false)
            })
            .collect();
        Ok(filtered)
    }

    pub fn to_mapping(&self) -> Result<dbt_serde_yaml::Mapping, dbt_serde_yaml::Error> {
        let mut mapping = dbt_serde_yaml::Mapping::default();

        // Convert self to YmlValue and return it as a YAML Mapping value
        let mut yml_value = self.to_yaml_value()?;
        let tmp = yml_value.as_mapping_mut().unwrap();
        std::mem::swap(tmp, &mut mapping);

        Ok(mapping)
    }

    pub fn get_aliases(&self) -> Vec<String> {
        // TODO: Implement Aliases for databases that need them. Snowflake does not need aliases.
        vec![]
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
    pub outputs: HashMap<TargetName, YmlValue>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autocreate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_groups: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ra3_node: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autocommit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<i64>,
    // Authentication Parameters (Password)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    pub host: Option<String>, // Setting as Option but required as of dbt 1.7.1
    pub user: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    // Authentication Parameters (IAM)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iam_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<StringOrInteger>,
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
    pub execution_timezone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_client_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_stage_vpce_dns_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct PostgresDbConfig {
    // Configuration Parameters
    pub port: Option<StringOrInteger>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "dbname")] // Postgres allows either dbname or database
    pub database: Option<String>,
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
    pub host: Option<String>, // Setting as Option but required as of dbt 1.7.1
    pub user: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct BigqueryDbConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "project")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "dataset")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum_bytes_billed: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impersonate_service_account: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyfile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyfile_json: Option<StringOrMap>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_region: Option<String>,
    // TODO: support this https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup
    pub dataproc_batch: Option<YmlValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataproc_cluster_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataproc_region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcs_bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_creation_timeout_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_execution_timeout_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_retries: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_retry_deadline_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct TrinoDbConfig {
    // Configuration Parameters
    pub port: Option<StringOrInteger>, // Setting as Option but required as of dbt 1.7.1
    pub user: Option<String>,          // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    pub host: Option<String>, // Setting as Option but required as of dbt 1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<StringOrInteger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct DatafusionDbConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub execute: Execute,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct DatabricksDbConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "catalog", default = "default_databricks_database")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_redirect_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_scopes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub session_properties: Option<HashMap<String, YmlValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub connection_parameters: Option<HashMap<String, YmlValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub compute: Option<HashMap<String, YmlValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_retries: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_all: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_max_idle: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<StringOrInteger>,
}

fn default_databricks_database() -> Option<String> {
    Some(DEFAULT_DATABRICKS_DATABASE.to_string())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct SalesforceDbConfig {
    /// The method to use to authenticate with Salesforce.
    /// `jwt_bearer`, `username_password`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    // schema is not applicable here
    #[serde(alias = "data_space", default = "default_salesforce_database")]
    pub database: Option<String>,
    pub client_id: Option<String>,
    pub private_key_path: Option<PathBuf>,
    pub login_url: Option<String>,
    pub username: Option<String>,
}

fn default_salesforce_database() -> Option<String> {
    Some("default".to_string())
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
    Salesforce(SalesforceTargetEnv),
    // Add other variants as needed
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TrinoTargetEnv {
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DatafusionTargetEnv {
    pub database: String,
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PostgresTargetEnv {
    pub dbname: String,
    pub host: String,
    pub user: String,
    pub port: StringOrInteger,
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct SnowflakeTargetEnv {
    pub warehouse: Option<String>,
    pub user: String,
    pub role: Option<String>,
    pub account: String,
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct BigqueryTargetEnv {
    pub project: String,
    pub dataset: String,
    pub client_id: Option<String>,
    pub compute_region: Option<String>,
    pub dataproc_batch: Option<YmlValue>,
    pub dataproc_cluster_name: Option<String>,
    pub dataproc_region: Option<String>,
    pub execution_project: Option<String>,
    pub gcs_bucket: Option<String>,
    pub impersonate_service_account: Option<String>,
    pub job_creation_timeout_seconds: Option<i64>,
    pub job_execution_timeout_seconds: Option<i64>,
    pub job_retries: Option<i64>,
    pub job_retry_deadline_seconds: Option<i64>,
    pub location: Option<String>,
    pub maximum_bytes_billed: Option<i64>,
    pub method: Option<String>,
    pub priority: Option<String>,
    pub retries: Option<i64>,
    pub target_name: Option<String>,
    pub timeout_seconds: Option<i64>,
    pub token_uri: Option<String>,
    pub __common__: CommonTargetContext,
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
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct RedshiftTargetEnv {
    pub dbname: String,
    pub host: String,
    pub user: String,
    pub port: StringOrInteger,
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct SalesforceTargetEnv {
    pub __common__: CommonTargetContext,
}

fn missing(field: &str) -> String {
    format!("In file `profiles.yml`, field `{field}` is required.")
}

// This target context is only to be used in rendering yml's
// See: https://docs.getdbt.com/reference/dbt-jinja-functions/target
impl TryFrom<DbConfig> for TargetContext {
    type Error = String;

    fn try_from(db_config: DbConfig) -> Result<Self, Self::Error> {
        let adapter_type = db_config.adapter_type().to_string();
        match db_config {
            // Snowflake case
            DbConfig::Snowflake(config) => {
                let database = config.database.ok_or_else(|| missing("database"))?;
                Ok(TargetContext::Snowflake(SnowflakeTargetEnv {
                    warehouse: config.warehouse,
                    user: config.user.ok_or_else(|| missing("user"))?,
                    role: config.role.clone(),
                    account: config.account.ok_or_else(|| missing("account"))?,
                    __common__: CommonTargetContext {
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
                    __common__: CommonTargetContext {
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
                    __common__: CommonTargetContext {
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
                    __common__: CommonTargetContext {
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
                    __common__: CommonTargetContext {
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
                    job_creation_timeout_seconds: config.job_creation_timeout_seconds,
                    job_execution_timeout_seconds: config.job_execution_timeout_seconds,
                    job_retries: config.job_retries,
                    job_retry_deadline_seconds: config.job_retry_deadline_seconds,
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
                    __common__: CommonTargetContext {
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
                    __common__: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: None,
                    },
                }))
            }

            DbConfig::Salesforce(config) => Ok(TargetContext::Salesforce(SalesforceTargetEnv {
                __common__: CommonTargetContext {
                    database: config.database.ok_or_else(|| missing("database"))?,
                    // `SalesforceDbConfig` doesn't have `schema`
                    schema: "".to_string(),
                    type_: adapter_type,
                    threads: None,
                },
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snowflake_adapter_unique_id() {
        let config: DbConfig = SnowflakeDbConfig {
            account: Some("kw27752".to_string()),
            ..Default::default()
        }
        .into();

        assert_eq!(
            config.get_unique_field().map(String::as_str),
            Some("kw27752")
        );
        assert_eq!(
            config.get_adapter_unique_id(),
            Some("c27a9a57d35df4a8f81aec929cbdc7cd".to_string())
        );
    }

    #[test]
    fn test_snowflake_adapter_unique_id_with_missing_account() {
        let config: DbConfig = SnowflakeDbConfig {
            account: None,
            ..Default::default()
        }
        .into();

        assert_eq!(config.get_unique_field(), None);
        assert_eq!(config.get_adapter_unique_id(), None);
    }

    #[test]
    fn test_bigquery_adapter_config_parsing() {
        let config: DbConfig = dbt_serde_yaml::from_str(
            "type: bigquery\n\
             job_creation_timeout_seconds: 123\n\
             job_execution_timeout_seconds: 456\n\
             job_retry_deadline_seconds: 789",
        )
        .unwrap();
        if let DbConfig::Bigquery(bigquery_config) = config {
            assert_eq!(bigquery_config.job_creation_timeout_seconds, Some(123));
            assert_eq!(bigquery_config.job_execution_timeout_seconds, Some(456));
            assert_eq!(bigquery_config.job_retry_deadline_seconds, Some(789));
        } else {
            panic!("Expected DbConfig::Bigquery, got {config:?}",);
        }
    }
}
