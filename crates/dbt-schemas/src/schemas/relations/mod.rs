use crate::schemas::common::{DbtQuoting, ResolvedQuoting};

pub mod base;

pub static DEFAULT_RESOLVED_QUOTING: ResolvedQuoting = ResolvedQuoting {
    database: true,
    schema: true,
    identifier: true,
};

pub static SNOWFLAKE_RESOLVED_QUOTING: ResolvedQuoting = ResolvedQuoting {
    database: false,
    schema: false,
    identifier: false,
};

pub static DEFAULT_DBT_QUOTING: DbtQuoting = DbtQuoting {
    database: Some(true),
    schema: Some(true),
    identifier: Some(true),
};

pub static SNOWFLAKE_DBT_QUOTING: DbtQuoting = DbtQuoting {
    database: Some(false),
    schema: Some(false),
    identifier: Some(false),
};

pub static DEFAULT_DATABRICKS_DATABASE: &str = "hive_metastore";
