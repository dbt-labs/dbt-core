//! ADBC connection options for Spark

pub const HOST: &str = "adbc.spark.host";
pub const PORT: &str = "adbc.spark.port";

pub const TRANSPORT_API: &str = "adbc.spark.api";
pub mod transport_api {
    pub const THRIFT_BINARY: &str = "thrift+binary";
    pub const THRIFT_HTTP: &str = "thrift+http";
    pub const LIVY: &str = "livy";
}

pub const AUTH_TYPE: &str = "adbc.spark.auth_type";
pub mod auth_type {
    pub const NOSASL: &str = "nosasl";
    pub const PLAIN: &str = "plain";
    pub const LDAP: &str = "ldap";
    pub const KERBEROS: &str = "kerberos";
}

pub const USERNAME: &str = "username";
pub const PASSWORD: &str = "password";

pub const KERBEROS_SERVICE_NAME: &str = "adbc.spark.kerberos.service_name";
