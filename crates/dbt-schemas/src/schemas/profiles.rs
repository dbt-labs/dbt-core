#![allow(unused_qualifications)]

use crate::schemas::relations::DEFAULT_DATABRICKS_DATABASE;
use crate::schemas::serde::{QueryTag, StringOrInteger, StringOrMap};

use dbt_common::adapter::AdapterType;
use dbt_yaml::JsonSchema;
use dbt_yaml::UntaggedEnumDeserialize;
use merge::Merge;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;

type YmlValue = dbt_yaml::Value;

pub type ProfileName = String;
pub type TargetName = String;
pub type DefaultTargetName = String;

#[derive(Debug, Deserialize)]
pub struct DbtProfilesIntermediate {
    pub config: Option<dbt_yaml::Value>,
    pub __profiles__: HashMap<ProfileName, dbt_yaml::Value>,
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
    Spark(Box<SparkDbConfig>),
    Databricks(Box<DatabricksDbConfig>),
    Salesforce(Box<SalesforceDbConfig>),
    DuckDB(Box<DuckDbConfig>),
    // Hive,
    // Exasol,
    // Oracle,
    // Synapse,
    Fabric(Box<FabricDbConfig>),
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
impl_from_db_config!(DuckDB, DuckDbConfig);
impl_from_db_config!(Fabric, FabricDbConfig);

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
            DbConfig::DuckDB(config) => config.path.as_ref(),
            DbConfig::Spark(config) => config.host.as_ref(),
            DbConfig::Fabric(config) => config.host.as_ref(),
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
                "submission_method",
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
            DbConfig::Salesforce(_) => &["login_url", "database", "data_transform_run_timeout"],
            DbConfig::DuckDB(_) => &[
                "path",
                "database",
                "schema",
                "extensions",
                "settings",
                "secrets",
                "attach",
            ],
            // TODO(serramatutu): Spark connection keys
            DbConfig::Spark(_) => &[],
            // TODO: Trino and Datafusion connection keys
            DbConfig::Trino(_) => &[],
            DbConfig::Datafusion(_) => &[],
            DbConfig::Fabric(_) => &[
                "server",
                "database",
                "schema",
                "warehouse_snapshot_name",
                "snapshot_timestamp",
                "UID",
                "workspace_id",
                "authentication",
                "retries",
                "login_timeout",
                "query_timeout",
                "trace_flag",
                "encrypt",
                "trust_cert",
                "api_url",
            ],
        }
    }

    pub fn get_execute_mode(&self) -> Execute {
        match self {
            DbConfig::Snowflake(config) => config.execute.unwrap_or_default(),
            // DuckDB uses Remote execution to go through the adapter like Snowflake/BigQuery
            DbConfig::DuckDB(_) => Execute::Remote,
            DbConfig::Datafusion(_) => Execute::Local,
            _ => Execute::Remote,
        }
    }

    /// Returns true if the profile has an explicit `execute:` field set.
    /// Used to distinguish "no execute: field" (default) from "execute: remote" (explicit).
    pub fn has_explicit_execute(&self) -> bool {
        match self {
            DbConfig::Snowflake(config) => config.execute.is_some(),
            _ => false,
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

    pub fn to_yaml_value(&self) -> Result<YmlValue, dbt_yaml::Error> {
        match self {
            DbConfig::Snowflake(config) => dbt_yaml::to_value(config),
            DbConfig::Postgres(config) => dbt_yaml::to_value(config),
            DbConfig::Bigquery(config) => dbt_yaml::to_value(config),
            DbConfig::Trino(config) => dbt_yaml::to_value(config),
            DbConfig::Datafusion(config) => dbt_yaml::to_value(config),
            DbConfig::Redshift(config) => dbt_yaml::to_value(config),
            DbConfig::Databricks(config) => dbt_yaml::to_value(config),
            DbConfig::Salesforce(config) => dbt_yaml::to_value(config),
            DbConfig::Spark(config) => dbt_yaml::to_value(config),
            DbConfig::Fabric(config) => dbt_yaml::to_value(config),
            DbConfig::DuckDB(config) => dbt_yaml::to_value(config),
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
            DbConfig::DuckDB(..) => "duckdb",
            DbConfig::Spark(..) => "spark",
            DbConfig::Fabric(..) => "fabric",
        }
    }

    pub fn adapter_type_if_supported(&self) -> Option<AdapterType> {
        match self {
            DbConfig::Redshift(..) => Some(AdapterType::Redshift),
            DbConfig::Snowflake(..) => Some(AdapterType::Snowflake),
            DbConfig::Postgres(..) => Some(AdapterType::Postgres),
            DbConfig::Bigquery(..) => Some(AdapterType::Bigquery),
            DbConfig::Trino(..) => None,
            DbConfig::Datafusion(..) => None,
            DbConfig::Databricks(..) => Some(AdapterType::Databricks),
            DbConfig::Salesforce(..) => Some(AdapterType::Salesforce),
            DbConfig::DuckDB(..) => Some(AdapterType::DuckDB),
            DbConfig::Spark(..) => Some(AdapterType::Spark),
            DbConfig::Fabric(..) => Some(AdapterType::Fabric),
        }
    }

    /// Returns a hint string if the adapter is gated behind an environment variable.
    /// Used by call sites to produce helpful error messages.
    pub fn unsupported_adapter_hint(&self) -> Option<&'static str> {
        None
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
            DbConfig::DuckDB(config) => config.database.as_ref(),
            DbConfig::Spark(_) => None,
            DbConfig::Fabric(config) => config.database.as_ref(),
        }
    }

    /// Returns the database name with adapter-specific defaults when not explicitly configured.
    /// - DuckDB: derived from file path stem (e.g., "jaffle_shop.duckdb" → "jaffle_shop"),
    ///   or "main" for in-memory (":memory:") or when no path is specified
    /// - Databricks: uses hive_metastore as default catalog
    /// - Others: "dbt" as generic fallback
    pub fn get_database_or_default(&self) -> String {
        // First check if database is explicitly set
        if let Some(db) = self.get_database() {
            return db.clone();
        }

        // Otherwise, use adapter-specific defaults
        match self {
            DbConfig::DuckDB(config) => {
                // For DuckDB, derive database name from file path
                if let Some(path) = &config.path {
                    if path == ":memory:" {
                        "main".to_string()
                    } else {
                        // Extract file stem (e.g., "jaffle_shop.duckdb" → "jaffle_shop")
                        std::path::Path::new(path)
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "main".to_string())
                    }
                } else {
                    "main".to_string()
                }
            }
            DbConfig::Databricks(_) => DEFAULT_DATABRICKS_DATABASE.to_string(),
            _ => "dbt".to_string(),
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
            DbConfig::Spark(config) => config.schema.as_ref(),
            DbConfig::DuckDB(config) => config.schema.as_ref(),
            DbConfig::Salesforce(_) => None,
            DbConfig::Fabric(config) => config.schema.as_ref(),
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
            DbConfig::DuckDB(config) => config.threads.as_ref(),
            DbConfig::Datafusion(_) => None,
            DbConfig::Salesforce(_) => None,
            DbConfig::Spark(_) => None,
            DbConfig::Fabric(_) => None,
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
            DbConfig::DuckDB(config) => config.threads = threads,
            DbConfig::Datafusion(_) => (),
            DbConfig::Salesforce(_) => (),
            DbConfig::Spark(_) => (),
            DbConfig::Fabric(_) => (),
        }
    }

    pub fn to_connection_mapping(&self) -> Result<dbt_yaml::Mapping, dbt_yaml::Error> {
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

    pub fn to_mapping(&self) -> Result<dbt_yaml::Mapping, dbt_yaml::Error> {
        let mut mapping = dbt_yaml::Mapping::default();

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
    Sidecar,
    Service,
}

impl Display for Execute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Execute::Remote => write!(f, "remote"),
            Execute::Local => write!(f, "local"),
            Execute::Sidecar => write!(f, "sidecar"),
            Execute::Service => write!(f, "service"),
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

    /// Skip serialization for absent (`None`) or default (`Some(Remote)`) execute values.
    /// Preserves the prior serialization behavior while allowing `Option<Execute>` for
    /// distinguishing absent from explicit `execute: remote` at deserialization time.
    pub fn skip_serialize_option(val: &Option<Execute>) -> bool {
        match val {
            None => true,
            Some(e) => e.is_default(),
        }
    }

    /// Compute the effective execute mode for the **execution** phase.
    ///
    /// `--compute sidecar|service` overrides the profile `execute:` field unconditionally
    /// (including `execute: local`). `--compute remote` (default) and `--compute inline`
    /// (legacy) defer to the profile, preserving `execute: local` (DataFusion dev mode).
    pub fn for_execution_phase(
        self,
        compute_flag: dbt_common::io_args::LocalExecutionBackendKind,
    ) -> Self {
        use dbt_common::io_args::LocalExecutionBackendKind;
        match compute_flag {
            LocalExecutionBackendKind::Remote => {
                if self == Execute::Local {
                    Execute::Local
                } else {
                    Execute::Remote
                }
            }
            LocalExecutionBackendKind::Worker => Execute::Sidecar,
            LocalExecutionBackendKind::Service => Execute::Service,
            LocalExecutionBackendKind::Inline => self, // legacy: defer to profile
        }
    }

    /// Compute the effective execute mode for the **compilation** phase.
    ///
    /// `execute: local` is always preserved regardless of `--compute`, because the
    /// compilation phase uses MockAdapter with no sidecar client, no schema cache
    /// clearing, and no DuckDB introspection for local profiles. The `--compute`
    /// override takes effect only during task execution (see [`Self::with_compute`]).
    pub fn for_compilation_phase(
        self,
        compute_flag: dbt_common::io_args::LocalExecutionBackendKind,
    ) -> Self {
        if self == Execute::Local {
            return Execute::Local;
        }
        self.for_execution_phase(compute_flag)
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
    #[serde(skip_serializing_if = "Option::is_none", alias = "pass")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub token_endpoint: Option<HashMap<String, YmlValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idc_region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idp_listen_port: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idc_client_display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idp_response_timeout: Option<i64>,
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
    pub query_tag: Option<QueryTag>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_all: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_on_database_errors: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_retries: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_timeout: Option<i64>,
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
    #[serde(default, skip_serializing_if = "Execute::skip_serialize_option")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub execute: Option<Execute>,
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
    pub quota_project: Option<String>,
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
    pub submission_method: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key_path: Option<PathBuf>,
    pub login_url: Option<String>,
    pub username: Option<String>,
    #[serde(default = "default_data_transform_run_timeout")]
    pub data_transform_run_timeout: Option<i64>,
}

fn default_salesforce_database() -> Option<String> {
    Some("default".to_string())
}

fn default_data_transform_run_timeout() -> Option<i64> {
    Some(180000) // 3 mins
}

/// A DuckDB secret for the Secrets Manager.
/// Corresponds to upstream dbt-duckdb `Secret` dataclass.
/// Generates a `CREATE OR REPLACE SECRET` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DuckDbSecret {
    /// Secret type: s3, azure, gcs, r2, huggingface, etc.
    #[serde(rename = "type")]
    pub secret_type: String,
    /// Optional name. Auto-generated as `__dbt_secret_{index}` if omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Optional provider (e.g., "credential_chain" for AWS default creds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Optional scope restriction (e.g., "s3://my-bucket").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Whether this is a persistent secret (survives restarts).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent: Option<bool>,
    /// All other key-value params become CREATE SECRET parameters.
    /// E.g., key_id, secret, region, endpoint, account_id, token, etc.
    #[serde(flatten)]
    pub params: HashMap<String, YmlValue>,
}

/// A DuckDB database attachment.
/// Corresponds to upstream dbt-duckdb `Attachment` dataclass.
/// Generates an `ATTACH IF NOT EXISTS` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DuckDbAttachment {
    /// Path to the database file or connection string.
    pub path: String,
    /// Alias name for the attached database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// Database type (e.g., "duckdb", "sqlite", "postgres").
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,
    /// Whether to attach read-only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// DuckDB adapter configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct DuckDbConfig {
    /// Path to the DuckDB database file. Defaults to in-memory (:memory:)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Database name (defaults to "main")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Schema name (defaults to "main")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Extensions to load (e.g., ["httpfs", "parquet"])
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub extensions: Option<Vec<String>>,
    /// DuckDB configuration settings (e.g., {"memory_limit": "4GB"})
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub settings: Option<HashMap<String, YmlValue>>,
    /// Number of threads
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<StringOrInteger>,
    /// Secrets to register with DuckDB's Secrets Manager.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub secrets: Option<Vec<DuckDbSecret>>,
    /// External databases to attach.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[merge(strategy = merge_strategies_extend::overwrite_always)]
    pub attach: Option<Vec<DuckDbAttachment>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SparkMethod {
    Thrift,
    Livy,
    // TODO: HTTP, Spark Connect, EMR StartJob, Session (?)
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum SparkAuth {
    NoSasl,
    SaslCustom,
    SaslKerberos,
    SaslLdap,
    #[default]
    SaslNone,
    SaslPlain,
}

impl Display for SparkAuth {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NoSasl => write!(f, "NOSASL"),
            Self::SaslCustom => write!(f, "CUSTOM"),
            Self::SaslKerberos => write!(f, "KERBEROS"),
            Self::SaslLdap => write!(f, "LDAP"),
            Self::SaslNone => write!(f, "NONE"),
            Self::SaslPlain => write!(f, "PLAIN"),
        }
    }
}

impl std::str::FromStr for SparkAuth {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "CUSTOM" => Ok(Self::SaslCustom),
            "KERBEROS" => Ok(Self::SaslKerberos),
            "LDAP" => Ok(Self::SaslLdap),
            "NONE" => Ok(Self::SaslNone),
            "NOSASL" => Ok(Self::NoSasl),
            "PLAIN" => Ok(Self::SaslPlain),
            _ => Err(format!("Invalid Spark auth mode: {s}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct SparkDbConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<SparkAuth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_ssl: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<SparkMethod>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kerberos_service_name: Option<String>,
    // TODO: python supports some extra properties:
    // - cluster
    // - connect_retries
    // - connect_timeout
    // - connection_string_suffix
    // - database (same as schema)
    // - driver
    // - endpoint
    // - organization
    // - poll_interval
    // - query_retries
    // - query_timeout
    // - retry_all
    // - server_side_parameters
    // - token
}

// https://docs.getdbt.com/docs/core/connect-data-platform/fabric-setup#microsoft-entra-id-authentication
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum FabricAuth {
    #[default]
    #[serde(alias = "ServicePrincipal")]
    ActiveDirectoryServicePrincipal,
    // TODO: other auth methods

    // ActiveDirectoryPassword,

    // #[serde(rename = "environment")]
    // Environment,

    // #[serde(rename = "CLI")]
    // AzureCLI,

    // /// Tries the following in order:
    // /// 1. Environment
    // /// 2. Managed Identity (not supported)
    // /// 3. Visual Studio (Windows only)
    // /// 4. VSCode
    // /// 5. Azure CLI
    // /// 6. Azure PowerShell module
    // #[serde(rename = "auto")]
    // Auto,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Merge)]
#[merge(strategy = merge_strategies_extend::overwrite_option)]
#[serde(rename_all = "snake_case")]
pub struct FabricDbConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver: Option<String>, // TODO: this seems to be ODBC related... keep it?
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "server")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "UID", alias = "user", alias = "username")]
    pub user: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "PWD", alias = "pass", alias = "password")]
    pub password: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "trusted_connection")]
    pub windows_login: Option<bool>, // default = False
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "SQL_ATTR_TRACE")]
    pub trace_flag: Option<bool>, // default = False
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "app_id")]
    pub client_id: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "app_secret")]
    pub client_secret: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token_expires_on: Option<String>, // default = 0 | Added for access token expiration for oAuth and integration tests scenarios.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "auth")]
    pub authentication: Option<FabricAuth>, // default = "ActiveDirectoryServicePrincipal"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypt: Option<bool>, // default = True  | default value in MS ODBC Driver 18 as well
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "TrustServerCertificate")]
    pub trust_cert: Option<bool>, // default = False  | default value in MS ODBC Driver 18 as well
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<i64>, // default = 3
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "schema_auth")]
    pub schema_authorization: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login_timeout: Option<i64>, // default = 0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_timeout: Option<i64>, // default = 0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_snapshot_name: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_snapshot_id: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_timestamp: Option<String>, // default = None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>, // default = "https://api.fabric.microsoft.com/v1"
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
    DuckDB(DuckDbTargetEnv),
    Spark(SparkTargetEnv),
    Fabric(FabricTargetEnv),
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
    pub account: String,
    pub user: String,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub authenticator: Option<String>,
    pub oauth_client_id: Option<String>,
    pub query_tag: Option<QueryTag>,
    pub client_session_keep_alive: bool, // Default: false
    pub host: Option<String>,
    pub port: Option<String>,
    pub protocol: Option<String>,
    pub proxy_host: Option<String>,
    pub proxy_port: Option<String>,
    pub insecure_mode: bool,  // Default: false
    pub connect_retries: i64, // Default: 1
    pub connect_timeout: Option<i64>,
    pub request_timeout: Option<i64>,
    pub retry_on_database_errors: bool, // Default: false
    pub retry_all: bool,                // Default: false
    pub reuse_connections: Option<bool>,
    pub s3_stage_vpce_dns_name: Option<String>,
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
    pub quota_project: Option<String>,
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
    pub host: Option<String>,
    pub http_path: Option<String>,
    pub catalog: Option<String>,
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct RedshiftTokenEndpoint {
    pub r#type: String,
    pub request_url: String,
    pub idp_auth_credentials: Option<String>,
    pub request_data: String,
}

#[derive(Serialize, JsonSchema)]
pub struct RedshiftTargetEnv {
    pub host: String,
    pub user: Option<String>,
    pub port: StringOrInteger,
    pub method: Option<String>,
    pub cluster_id: Option<String>,
    pub iam_profile: Option<String>,
    pub sslmode: Option<String>,
    pub region: Option<String>,
    pub autocreate: bool,
    pub db_groups: Option<Vec<String>>,
    pub ra3_node: Option<bool>,
    pub connect_timeout: Option<i64>,
    pub role: Option<String>,
    pub retries: i64,
    pub autocommit: Option<bool>,
    pub dbname: String,
    pub __common__: CommonTargetContext,
    pub retry_all: bool,
    pub access_key_id: Option<String>,
    pub is_serverless: Option<bool>,
    pub serverless_work_group: Option<String>,
    pub serverless_acct_id: Option<String>,
    pub token_endpoint: Option<HashMap<String, YmlValue>>,
    pub idc_region: Option<String>,
    pub issuer_url: Option<String>,
    pub idp_listen_port: Option<i64>,
    pub idc_client_display_name: Option<String>,
    pub idp_response_timeout: Option<i64>,
}

#[derive(Serialize, JsonSchema)]
pub struct SalesforceTargetEnv {
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct DuckDbTargetEnv {
    pub path: Option<String>,
    pub __common__: CommonTargetContext,
}

#[derive(Serialize, JsonSchema)]
pub struct SparkTargetEnv {
    pub __common__: CommonTargetContext,
    pub method: SparkMethod,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub auth: SparkAuth,
    pub use_ssl: bool,
    pub kerberos_service_name: String,
}

#[derive(Serialize, JsonSchema)]
pub struct FabricTargetEnv {
    pub __common__: CommonTargetContext,
    pub authentication: FabricAuth,
    // TODO: ...
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
                    account: config.account.ok_or_else(|| missing("account"))?,
                    user: config.user.ok_or_else(|| missing("user"))?,
                    warehouse: config.warehouse,
                    role: config.role.clone(),
                    authenticator: config.authenticator,
                    oauth_client_id: config.oauth_client_id,
                    query_tag: config.query_tag,
                    client_session_keep_alive: config.client_session_keep_alive.unwrap_or(false),
                    host: config.host,
                    port: config.port,
                    protocol: config.protocol,
                    connect_retries: config.connect_retries.unwrap_or(1),
                    connect_timeout: config.connect_timeout,
                    request_timeout: config.request_timeout,
                    retry_on_database_errors: config.retry_on_database_errors.unwrap_or(false),
                    retry_all: config.retry_all.unwrap_or(false),
                    reuse_connections: config.reuse_connections,
                    s3_stage_vpce_dns_name: config.s3_stage_vpce_dns_name,
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
                    proxy_host: None,
                    proxy_port: None,
                    insecure_mode: false,
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
                    quota_project: config.quota_project.clone(),
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
                    host: config.host,
                    http_path: config.http_path,
                    catalog: Some(database.clone()),
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
                    host: config.host.ok_or_else(|| missing("host"))?,
                    user: config.user,
                    port: config.port.ok_or_else(|| missing("port"))?,
                    method: config.method,
                    cluster_id: config.cluster_id,
                    iam_profile: config.iam_profile,
                    sslmode: config.sslmode,
                    region: config.region,
                    autocreate: config.autocreate.unwrap_or(false),
                    db_groups: config.db_groups,
                    ra3_node: config.ra3_node,
                    connect_timeout: config.connect_timeout,
                    role: config.role,
                    retries: config.retries.unwrap_or(1),
                    autocommit: config.autocommit,
                    dbname: database.clone(),
                    __common__: CommonTargetContext {
                        database,
                        schema: config.schema.ok_or_else(|| missing("schema"))?,
                        type_: adapter_type,
                        threads: None,
                    },
                    retry_all: false,
                    access_key_id: None,
                    is_serverless: None,
                    serverless_work_group: None,
                    serverless_acct_id: None,
                    token_endpoint: config.token_endpoint,
                    idc_region: config.idc_region,
                    idc_client_display_name: config.idc_client_display_name,
                    issuer_url: config.issuer_url,
                    idp_listen_port: config.idp_listen_port,
                    idp_response_timeout: config.idp_response_timeout,
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

            DbConfig::DuckDB(config) => Ok(TargetContext::DuckDB(DuckDbTargetEnv {
                path: config.path.clone(),
                __common__: CommonTargetContext {
                    // Derive database name from path if not explicitly set (same logic as get_database())
                    database: config.database.clone().unwrap_or_else(|| {
                        if let Some(path) = &config.path {
                            if path == ":memory:" {
                                "main".to_string()
                            } else {
                                std::path::Path::new(path)
                                    .file_stem()
                                    .and_then(|s| s.to_str())
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| "main".to_string())
                            }
                        } else {
                            "main".to_string()
                        }
                    }),
                    schema: config.schema.unwrap_or_else(|| "main".to_string()),
                    type_: adapter_type,
                    threads: match config.threads {
                        Some(StringOrInteger::String(threads)) => Some(
                            threads
                                .parse::<u16>()
                                .map_err(|_| "threads must be a positive integer".to_string())?,
                        ),
                        Some(StringOrInteger::Integer(threads)) => Some(threads as u16),
                        None => None,
                    },
                },
            })),

            DbConfig::Spark(config) => Ok(TargetContext::Spark(SparkTargetEnv {
                method: config.method.ok_or_else(|| missing("method"))?,
                host: config.host.ok_or_else(|| missing("host"))?,
                port: config.port.unwrap_or(10000),
                user: config.user.unwrap_or_default(),

                kerberos_service_name: config.kerberos_service_name.unwrap_or_default(),
                use_ssl: config.use_ssl.unwrap_or(false),
                auth: config.auth.unwrap_or_default(),

                __common__: CommonTargetContext {
                    database: "".to_string(),
                    schema: config.schema.ok_or_else(|| missing("schema"))?,
                    type_: adapter_type,
                    threads: None,
                },
            })),
            DbConfig::Fabric(config) => {
                let common = CommonTargetContext {
                    database: config.database.ok_or_else(|| missing("database"))?,
                    schema: config.schema.ok_or_else(|| missing("schema"))?,
                    type_: adapter_type,
                    threads: None,
                };

                let authentication = config.authentication.unwrap_or_default();

                Ok(TargetContext::Fabric(FabricTargetEnv {
                    __common__: common,
                    authentication,
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
        let config: DbConfig = dbt_yaml::from_str(
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
