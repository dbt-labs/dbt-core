//! A builder for a [`Database`]
//!
//!

use adbc_core::{
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
};
use core::str;
use dirs;
use ini::ini;
use percent_encoding::percent_decode_str;
use std::fmt;
use url::Url;

use crate::database::fingerprint_config;
use crate::{Backend, Database, Driver, builder::BuilderIter};

use super::Fingerprint;

/// Log levels.
#[derive(Copy, Clone, Debug, Default)]
#[non_exhaustive]
pub enum LogLevel {
    /// Trace level
    Trace,
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warn level
    Warn,
    /// Error level
    Error,
    /// Fatal level
    Fatal,
    /// Off
    #[default]
    Off,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Trace => "trace",
                Self::Debug => "debug",
                Self::Info => "info",
                Self::Warn => "warn",
                Self::Error => "error",
                Self::Fatal => "fatal",
                Self::Off => "off",
            }
        )
    }
}

impl str::FromStr for LogLevel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            "fatal" => Ok(Self::Fatal),
            "off" => Ok(Self::Off),
            _ => Err(Error::with_message_and_status(
                format!(
                    "invalid log level: {s} (possible values: {}, {}, {}, {}, {}, {}, {})",
                    Self::Trace,
                    Self::Debug,
                    Self::Info,
                    Self::Warn,
                    Self::Error,
                    Self::Fatal,
                    Self::Off
                ),
                Status::InvalidArguments,
            )),
        }
    }
}

/// A builder for [`Database`].
///
/// The builder can be used to initialize a [`Database`] with
/// [`Builder::build`] or by directly passing it to
/// [`Driver::new_database_with_opts`].
#[derive(Clone)]
pub struct Builder {
    /// The backend target of the driver.
    pub backend: Backend,

    /// The URI ([`OptionDatabase::Uri`]).
    pub uri: Option<Url>,

    /// The username ([`OptionDatabase::Username`]).
    pub username: Option<String>,

    /// The password ([`OptionDatabase::Password`]).
    pub password: Option<String>,

    /// All the other database options.
    pub other: Vec<(OptionDatabase, OptionValue)>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HIDDEN: &str = "*****";
        let mut dbg = f.debug_struct("Builder");
        dbg.field(
            "uri",
            &self.uri.clone().map(|uri| {
                let scheme = uri.scheme();
                let host = uri.host_str();
                let port = uri.port();
                let path = uri.path();
                let query = uri.query();
                let mut s = String::with_capacity(uri.as_str().len());
                if !scheme.is_empty() {
                    s.push_str(scheme);
                    s.push_str("://");
                }
                if let Some(host) = host {
                    s.push_str(host);
                    if let Some(port) = port {
                        s.push(':');
                        s.push_str(&port.to_string());
                    }
                }
                s.push_str(path);
                if query.is_some() {
                    let mut first = true;
                    for (name, value) in uri.query_pairs() {
                        let new_value =
                            if name == "username" || name == "user" || name == "password" {
                                &name
                            } else {
                                &value
                            };
                        s.push(if first { '?' } else { '&' });
                        s.push_str(&name);
                        if !name.is_empty() {
                            s.push('=');
                            s.push_str(new_value);
                        }
                        first = false;
                    }
                }
                let safe_uri = Url::parse(&s);
                debug_assert!(safe_uri.is_ok(), "failed to parse safe URI: {s}");
                safe_uri.unwrap_or(uri)
            }),
        )
        .field("username", &self.username)
        .field("password", &self.password.as_ref().map(|_| HIDDEN));

        for (name, value) in &self.other {
            // if option is one of the sensitive options, hide the value
            if let OptionDatabase::Other(s) = name {
                if s.contains("auth_token")
                    || s.contains("jwt_private_key_pkcs8_value")
                    || s.contains("jwt_private_key_pkcs8_password")
                {
                    dbg.field(s, &HIDDEN);
                } else {
                    dbg.field(s, value);
                }
            } else {
                dbg.field(name.as_ref(), value);
            }
        }
        dbg.finish()
    }
}

impl Builder {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            uri: None,
            username: None,
            password: None,
            other: Vec::new(),
        }
    }

    pub fn from_snowsql_config() -> Result<Self> {
        use crate::snowflake;

        let home = dirs::home_dir().ok_or_else(|| {
            Error::with_message_and_status("failed to get home directory", Status::Internal)
        })?;
        let config_path_buf = home.join(".snowsql").join("config");
        let config_path = config_path_buf.to_str().ok_or_else(|| {
            Error::with_message_and_status("failed to convert path to string", Status::Internal)
        })?;
        let map = ini!(&config_path);
        let get_field = |key: &str| {
            map["connections"][key].clone().ok_or_else(|| {
                Error::with_message_and_status(
                    format!("missing connections.{key} in .snowsql/config"),
                    Status::Internal,
                )
            })
        };

        let account = get_field("accountname")?;
        let warehouse = get_field("warehousename")?;
        let role = get_field("rolename")?;
        let username = get_field("username")?;
        let password = get_field("password")?;

        let mut builder = Self::new(Backend::Snowflake);
        builder
            .with_named_option(snowflake::ACCOUNT, account)?
            .with_named_option(snowflake::WAREHOUSE, warehouse)?
            .with_named_option(snowflake::ROLE, role)?
            .with_username(username)
            .with_password(password);
        Ok(builder)
    }

    /// Use the provided URI ([`Self::uri`]).
    pub fn with_uri(&mut self, uri: Url) -> &mut Self {
        // override the username/password if they are present in the URI
        // authority instead of query (e.g. https://username:password@host/path)
        if !uri.username().is_empty() {
            let username = percent_decode_str(uri.username());
            self.with_username(username.decode_utf8_lossy());
        }
        if let Some(password) = uri.password() {
            // safely set the password via the options interface
            let password = percent_decode_str(password);
            self.with_password(password.decode_utf8_lossy());
        }

        self.uri = Some(uri);
        self
    }

    /// Parse and use the provided URI ([`Self::uri`]).
    ///
    /// Returns an error when parsing fails.
    pub fn with_parse_uri(&mut self, uri: impl AsRef<str>) -> Result<&mut Self> {
        Url::parse(uri.as_ref())
            .map_err(|error| {
                Error::with_message_and_status(error.to_string(), Status::InvalidArguments)
            })
            .map(|uri| self.with_uri(uri))
    }

    pub fn with_username(&mut self, username: impl AsRef<str>) -> &mut Self {
        self.username = Some(username.as_ref().to_string());
        self
    }

    pub fn with_password(&mut self, password: impl AsRef<str>) -> &mut Self {
        self.password = Some(password.as_ref().to_string());
        self
    }

    #[inline(never)]
    pub fn with_typed_option(
        &mut self,
        name: OptionDatabase,
        value: OptionValue,
    ) -> Result<&mut Self> {
        match name {
            OptionDatabase::Uri => match value {
                OptionValue::String(value) => self.with_parse_uri(value),
                _ => Err(Error::with_message_and_status(
                    "uri must be a string",
                    Status::InvalidArguments,
                )),
            },
            OptionDatabase::Username => match value {
                OptionValue::String(value) => Ok(self.with_username(value)),
                _ => Err(Error::with_message_and_status(
                    "username must be a string",
                    Status::InvalidArguments,
                )),
            },
            OptionDatabase::Password => match value {
                OptionValue::String(value) => Ok(self.with_password(value)),
                _ => Err(Error::with_message_and_status(
                    "password must be a string",
                    Status::InvalidArguments,
                )),
            },
            OptionDatabase::Other(_) => {
                self.other.push((name, value));
                Ok(self)
            }
            _ => Err(Error::with_message_and_status(
                "option must be a string",
                Status::InvalidArguments,
            )),
        }
    }

    pub fn with_option(
        &mut self,
        option: OptionDatabase,
        value: impl Into<String>,
    ) -> Result<&mut Self> {
        self.with_typed_option(option, OptionValue::String(value.into()))
    }

    pub fn with_named_option(
        &mut self,
        name: impl AsRef<str>,
        value: impl Into<String>,
    ) -> Result<&mut Self> {
        let option = OptionDatabase::Other(name.as_ref().to_string());
        self.with_typed_option(option, OptionValue::String(value.into()))
    }

    /// Attempt to initialize a [`Database`] using the values provided to this
    /// builder using the provided [`Driver`].
    pub fn build(self, driver: &mut Box<dyn Driver>) -> Result<Box<dyn Database>> {
        let iter = self.into_iter();
        let opts = iter.collect::<Vec<_>>();
        driver.new_database_with_opts(opts)
    }

    /// Calculate a fingerprint for the database options.
    pub fn fingerprint<'a>(
        opts: impl Iterator<Item = &'a (OptionDatabase, OptionValue)>,
    ) -> Fingerprint {
        fingerprint_config(opts)
    }
}

impl IntoIterator for Builder {
    type Item = (OptionDatabase, OptionValue);
    type IntoIter = BuilderIter<OptionDatabase, 3>;

    fn into_iter(self) -> Self::IntoIter {
        let fixed = match self.backend {
            Backend::Redshift | Backend::Postgres => {
                // take username/password options and put them in the URI sent to the driver
                // https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                let mut uri = self
                    .uri
                    .unwrap_or_else(|| Url::parse("postgres://").unwrap());
                {
                    let mut query = uri.query_pairs_mut();
                    if let Some(username) = self.username {
                        query.append_pair("user", &username);
                    }
                    if let Some(password) = self.password {
                        query.append_pair("password", &password);
                    }
                }
                [
                    Some((OptionDatabase::Uri, OptionValue::String(uri.to_string()))),
                    None,
                    None,
                ]
            }
            _ => [
                self.uri
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (OptionDatabase::Uri, value)),
                self.username
                    .map(OptionValue::String)
                    .map(|value| (OptionDatabase::Username, value)),
                self.password
                    .map(OptionValue::String)
                    .map(|value| (OptionDatabase::Password, value)),
            ],
        };
        BuilderIter::new(fixed, self.other)
    }
}
