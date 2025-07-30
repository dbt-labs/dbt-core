//! ADBC Database
//!
//!

use adbc_core::{
    Database as _, Optionable,
    driver_manager::ManagedDatabase as ManagedAdbcDatabase,
    error::{Error, Result, Status},
    options::{AdbcVersion, InfoCode, OptionConnection, OptionDatabase, OptionValue},
};
use arrow_array::{
    Array,
    cast::AsArray,
    types::{Int64Type, UInt32Type},
};
use parking_lot::RwLockUpgradableReadGuard;
use serde::Deserialize;
use siphasher::sip128::{Hasher128, SipHasher24};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use std::{collections::HashSet, ffi::c_int};
use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicI64, Ordering},
};
use tracy_client::span;

#[cfg(feature = "odbc")]
use crate::connection::OdbcConnection;
#[cfg(feature = "odbc")]
use crate::odbc::OdbcEnv;
use crate::{
    Backend, Connection, connection::AdbcConnection, semaphore::Semaphore, snowflake,
    str_from_sqlstate,
};

mod builder;
pub use builder::*;

/// XDBC Database.
///
/// dyn-compatible trait covering functionality from the adbc_core::{Database, Optionable} traits.
pub trait Database: Send + Sync + DatabaseInfo {
    // adbc_core::Database<Box<dyn Connection>> functions -----------------------
    fn new_connection(&mut self) -> Result<Box<dyn Connection>>;
    fn new_connection_with_opts(
        &mut self,
        opts: Vec<(OptionConnection, OptionValue)>,
    ) -> Result<Box<dyn Connection>>;

    // adbc_core::Optionable<Option = OptionDatabase> functions -----------------
    fn set_option(&mut self, key: OptionDatabase, value: OptionValue) -> Result<()>;
    fn get_option_string(&self, key: OptionDatabase) -> Result<String>;
    fn get_option_bytes(&self, key: OptionDatabase) -> Result<Vec<u8>>;
    fn get_option_int(&self, key: OptionDatabase) -> Result<i64>;
    fn get_option_double(&self, key: OptionDatabase) -> Result<f64>;

    /// Returns the [`AdbcVersion`] reported by the driver.
    fn adbc_version(&mut self) -> Result<AdbcVersion> {
        self.new_connection()?
            .get_info(Some(HashSet::from_iter([InfoCode::DriverAdbcVersion])))?
            .next()
            .ok_or(Error::with_message_and_status(
                "failed to get info",
                Status::Internal,
            ))?
            .map_err(Into::into)
            .and_then(|record_batch| {
                assert_eq!(
                    record_batch.column(0).as_primitive::<UInt32Type>().value(0),
                    u32::from(&InfoCode::DriverAdbcVersion)
                );
                AdbcVersion::try_from(
                    record_batch
                        .column(1)
                        .as_union()
                        .value(0)
                        .as_primitive::<Int64Type>()
                        .value(0) as c_int,
                )
            })
    }

    fn clone_box(&self) -> Box<dyn Database>;
}

pub trait DatabaseInfo {
    fn get_info(&mut self, info_code: InfoCode) -> Result<Arc<dyn Array>>;

    /// Returns the name of the vendor.
    fn vendor_name(&mut self) -> Result<String> {
        self.get_info(InfoCode::VendorName)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the version of the vendor.
    fn vendor_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::VendorVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the Arrow version of the vendor.
    fn vendor_arrow_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::VendorArrowVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns true if SQL queries are supported.
    fn vendor_sql(&mut self) -> Result<bool> {
        self.get_info(InfoCode::VendorSql)
            .map(|array| array.as_boolean().value(0))
    }

    /// Returns true if Substrait queries are supported.
    fn vendor_substrait(&mut self) -> Result<bool> {
        self.get_info(InfoCode::VendorSubstrait)
            .map(|array| array.as_boolean().value(0))
    }

    /// Returns the name of the wrapped driver.
    fn driver_name(&mut self) -> Result<String> {
        self.get_info(InfoCode::DriverName)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the version of the wrapped driver.
    fn driver_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::DriverVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the Arrow version of the wrapped driver.
    fn driver_arrow_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::DriverArrowVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }
}

impl Clone for Box<dyn Database> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Deserialize, Debug)]
struct RefreshResponse {
    access_token: String,
}

const REFRESH_TOKEN_REQ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct TokenRefresher {
    http_agent: ureq::Agent,
    token_request_url: String,
    encoded_creds: String,
    refresh_token: String,
}

impl TokenRefresher {
    pub fn new(
        client_id: String,
        client_secret: String,
        account: String,
        refresh_token: String,
    ) -> Self {
        use base64::Engine as _;
        use base64::engine::general_purpose;
        const BASE64_ENGINE: &general_purpose::GeneralPurpose = &general_purpose::STANDARD;

        let http_config = ureq::Agent::config_builder()
            .timeout_global(Some(REFRESH_TOKEN_REQ_TIMEOUT))
            .build();
        let http_agent = ureq::Agent::new_with_config(http_config);

        let token_request_url = format!(
            "https://{}.snowflakecomputing.com/oauth/token-request",
            &account
        );

        let encoded_creds = BASE64_ENGINE.encode(format!("{client_id}:{client_secret}"));

        TokenRefresher {
            http_agent,
            token_request_url,
            encoded_creds,
            refresh_token,
        }
    }

    pub fn for_database(backend: Backend, database: &ManagedAdbcDatabase) -> Result<Option<Self>> {
        if backend != Backend::Snowflake {
            return Ok(None);
        }
        let client_id =
            database.get_option_string(OptionDatabase::Other(snowflake::CLIENT_ID.to_string()))?;
        let client_secret = database
            .get_option_string(OptionDatabase::Other(snowflake::CLIENT_SECRET.to_string()))?;
        let account =
            database.get_option_string(OptionDatabase::Other(snowflake::ACCOUNT.to_string()))?;
        let refresh_token = database
            .get_option_string(OptionDatabase::Other(snowflake::REFRESH_TOKEN.to_string()))?;

        // Not an auth configuration that needs refreshing
        if client_id.is_empty()
            || client_secret.is_empty()
            || account.is_empty()
            || refresh_token.is_empty()
        {
            return Ok(None);
        }

        let refresher = Self::new(client_id, client_secret, account, refresh_token);
        Ok(Some(refresher))
    }

    fn refreshed_auth_token(&self) -> Result<String> {
        let _span = span!("refreshed_auth_token");
        use http::header::AUTHORIZATION;
        let result = self
            .http_agent
            .post(&self.token_request_url)
            .header(AUTHORIZATION, format!("Basic {}", self.encoded_creds))
            .send_form([
                ("grant_type", "refresh_token"),
                ("refresh_token", self.refresh_token.as_str()),
            ]);

        let resp = result
            .map_err(|e| {
                Error::with_message_and_status(
                    format!(
                        "Failed to make request to {}: {}",
                        self.token_request_url, e
                    ),
                    Status::IO,
                )
            })?
            .into_body()
            .read_json::<RefreshResponse>()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed parse payload of auth token request: {e}"),
                    Status::InvalidData,
                )
            })?;

        Ok(resp.access_token)
    }
}

struct InnerAdbcDatabase {
    pub(crate) backend: Backend,
    /// Readers-writer lock to protect the database state.
    ///
    /// Unlike [std::sync::RwLock], this lock is fair to writers and won't block them
    /// indefinitely if there is a steady flow of readers acquiring the lock.
    pub(crate) managed_database: parking_lot::RwLock<ManagedAdbcDatabase>,
    token_refresher: Option<TokenRefresher>,
    /// Current number of connection attempts.
    conn_attempts: AtomicI64,
    /// Last connection attempt that succeeded.
    ///
    /// INVARIANTS:
    ///     conn_attempts >= 0
    ///     last_conn_success >= -1
    ///     last_conn_success < conn_attempts
    last_conn_success: AtomicI64,
}

impl InnerAdbcDatabase {
    fn new_with_refresher(
        backend: Backend,
        managed_database: ManagedAdbcDatabase,
        token_refresher: Option<TokenRefresher>,
    ) -> Self {
        InnerAdbcDatabase {
            backend,
            managed_database: parking_lot::RwLock::new(managed_database),
            token_refresher,
            conn_attempts: AtomicI64::new(0),
            last_conn_success: AtomicI64::new(-1),
        }
    }

    fn new_connection_with_opts_impl(
        &self,
        conn_opts: Vec<(OptionConnection, OptionValue)>,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Result<Box<dyn Connection>> {
        let conn_opts_for_retry = conn_opts.clone();
        self.try_new_connection_with_opts_once(conn_opts, semaphore.clone())
            .or_else(|e| {
                // Snowflake might return a connection error when the cached ID token is invalid.
                //
                //     390195 (08004): The provided ID Token is invalid.
                //
                // Trying again consistently is enough to make the issue go away.
                if self.backend == Backend::Snowflake
                    && e.vendor_code == 390195
                    && str_from_sqlstate(&e.sqlstate) == "08004"
                {
                    self.try_new_connection_with_opts_once(conn_opts_for_retry, semaphore)
                } else {
                    Err(e)
                }
            })
    }

    /// Called before a connection is being opened with new_connection() or
    /// new_connection_with_opts().
    ///
    /// Based on the warehouse backend and auth options, this function may:
    /// 1) add database options to be overriden right before a new connection is created
    /// 2) add connection-specific options to be used by the new connection
    ///
    /// NOTE: this function must not try to acquire any locks on the database.
    /// See `new_connection_with_opts()` to see how it's used.
    #[allow(clippy::ptr_arg)]
    fn will_open_connection(
        &self,
        db_opts: &mut Vec<(OptionDatabase, OptionValue)>,
        _conn_opts: &mut Vec<(OptionConnection, OptionValue)>,
    ) -> Result<()> {
        self.token_refresher
            .as_ref()
            .map(|refresher| {
                let auth_token = refresher.refreshed_auth_token()?;
                if self.backend == Backend::Snowflake {
                    let pair = (
                        OptionDatabase::Other(snowflake::AUTH_TOKEN.to_string()),
                        OptionValue::String(auth_token),
                    );
                    db_opts.push(pair);
                }
                Ok(())
            })
            .unwrap_or(Ok(()))
    }

    fn try_new_connection_with_opts_once(
        &self,
        conn_opts: Vec<(OptionConnection, OptionValue)>,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Result<Box<dyn Connection>> {
        let span = span!("try_new_connection_with_opts_once");
        let mut db_opts = Vec::<(OptionDatabase, OptionValue)>::new();
        let mut conn_opts = conn_opts;

        // Start the connection creation process with a READ lock
        let lock_guard = self.managed_database.upgradable_read();

        // Upgrade to a write lock during the connection if it's the first
        // connection attempt or the last connection attempt has failed.
        let must_upgrade = {
            let conn_attempt = self.conn_attempts.fetch_add(1, Ordering::Acquire);
            let last_conn_success = self.last_conn_success.load(Ordering::Acquire);
            last_conn_success == -1 || last_conn_success + 1 < conn_attempt
        };
        let lock_guard = if must_upgrade {
            let wlock = RwLockUpgradableReadGuard::upgrade(lock_guard);
            (None, Some(wlock))
        } else {
            (Some(lock_guard), None)
        };

        // Run preparations (doesn't rely on exclusive locking)
        let prepare_res = self.will_open_connection(&mut db_opts, &mut conn_opts);

        let conn = {
            // The lock guard only lives until the end of this scope.
            // Should an upgrade to a write lock be performed?
            let mut lock_guard = match lock_guard {
                (Some(rlock), _) => {
                    if db_opts.is_empty() {
                        // we have a read lock, but no options to set, don't upgrade
                        (Some(rlock), None)
                    } else {
                        // upgrade the read lock to a write lock
                        let wlock = RwLockUpgradableReadGuard::upgrade(rlock);
                        (None, Some(wlock))
                    }
                }
                (_, Some(wlock)) => {
                    // preserve the write lock we got from the beginning
                    (None, Some(wlock))
                }
                _ => unreachable!(),
            };
            prepare_res.and_then(|_| {
                let res = (|| {
                    if !db_opts.is_empty() {
                        let managed_database = match &mut lock_guard {
                            (_, Some(wlock)) => &mut **wlock,
                            _ => {
                                unreachable!("lock_guard must be a write lock when setting options")
                            }
                        };
                        for (key, value) in db_opts.into_iter() {
                            managed_database.set_option(key, value)?;
                        }
                    }
                    #[cfg(feature = "xdbc-fuzzying")]
                    {
                        use rand::Rng as _;
                        let mut rng = rand::rng();
                        let x: f32 = rng.random();
                        if x < 0.25 {
                            let e = Error::with_message_and_status(
                                "simulated connection error",
                                Status::Internal,
                            );
                            return Err(e);
                        }
                    }
                    let managed_database = match &lock_guard {
                        (Some(rlock), _) => &**rlock,
                        (_, Some(wlock)) => &**wlock,
                        _ => unreachable!(),
                    };
                    if conn_opts.is_empty() {
                        managed_database.new_connection()
                    } else {
                        managed_database.new_connection_with_opts(conn_opts)
                    }
                })();
                if res.is_ok() {
                    // Upgrade to a write lock so we can set `last_conn_success` to
                    // `conn_attempts-1`
                    let wlock_guard = match lock_guard {
                        (Some(rlock), _) => RwLockUpgradableReadGuard::upgrade(rlock),
                        (_, Some(wlock)) => wlock,
                        _ => unreachable!(),
                    };
                    self.last_conn_success.store(
                        self.conn_attempts.load(Ordering::Acquire) - 1,
                        Ordering::Release,
                    );
                    // drop() called explicitly to make sure we don't
                    // accidentally move the lock guard in the code
                    // above and release the lock before intended.
                    drop(wlock_guard);
                }
                res
            })
        }?;
        let conn = AdbcConnection(self.backend, conn, semaphore);
        drop(span);
        Ok(Box::new(conn))
    }
}

/// ADBC Database.
///
/// Databases hold state shared by multiple connections. Generally, this means common
/// configuration and caches.
pub(crate) struct AdbcDatabase {
    inner: Arc<InnerAdbcDatabase>,
    semaphore: Option<Arc<Semaphore>>,
}

impl AdbcDatabase {
    pub fn new(
        backend: Backend,
        managed_database: ManagedAdbcDatabase,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        let token_refresher = TokenRefresher::for_database(backend, &managed_database)
            .ok()
            .flatten();
        let inner =
            InnerAdbcDatabase::new_with_refresher(backend, managed_database, token_refresher);
        Self {
            inner: Arc::new(inner),
            semaphore,
        }
    }
}

impl DatabaseInfo for AdbcDatabase {
    fn get_info(&mut self, info_code: InfoCode) -> Result<Arc<dyn Array>> {
        let conn = self.new_connection()?;
        let _rlock = self.inner.managed_database.read();
        let mut record_batch_reader = conn.get_info(Some(HashSet::from_iter([info_code])))?;
        record_batch_reader
            .next()
            .ok_or(Error::with_message_and_status(
                "failed to get info",
                Status::Internal,
            ))?
            .map_err(Into::into)
            .and_then(|record_batch| {
                if InfoCode::try_from(record_batch.column(0).as_primitive::<UInt32Type>().value(0))?
                    == info_code
                {
                    Ok(record_batch.column(1).as_union().value(0))
                } else {
                    Err(Error::with_message_and_status(
                        "invalid get info reply",
                        Status::Internal,
                    ))
                }
            })
    }
}

impl Database for AdbcDatabase {
    fn new_connection(&mut self) -> Result<Box<dyn Connection>> {
        let opts = Vec::new();
        self.new_connection_with_opts(opts)
    }

    fn new_connection_with_opts(
        &mut self,
        conn_opts: Vec<(OptionConnection, OptionValue)>,
    ) -> Result<Box<dyn Connection>> {
        self.inner
            .new_connection_with_opts_impl(conn_opts, self.semaphore.clone())
    }

    fn set_option(&mut self, key: OptionDatabase, value: OptionValue) -> Result<()> {
        let mut managed_database = self.inner.managed_database.write();
        managed_database.set_option(key, value)
    }

    fn get_option_string(&self, key: OptionDatabase) -> Result<String> {
        let managed_database = self.inner.managed_database.read();
        managed_database.get_option_string(key)
    }

    fn get_option_bytes(&self, key: OptionDatabase) -> Result<Vec<u8>> {
        let managed_database = self.inner.managed_database.read();
        managed_database.get_option_bytes(key)
    }

    fn get_option_int(&self, key: OptionDatabase) -> Result<i64> {
        let managed_database = self.inner.managed_database.read();
        managed_database.get_option_int(key)
    }

    fn get_option_double(&self, key: OptionDatabase) -> Result<f64> {
        let managed_database = self.inner.managed_database.read();
        managed_database.get_option_double(key)
    }

    fn clone_box(&self) -> Box<dyn Database> {
        let adbc_database = AdbcDatabase {
            inner: self.inner.clone(),
            semaphore: self.semaphore.clone(),
        };
        Box::new(adbc_database)
    }
}

/// The managed ODBC database.
///
/// To match the ADBC API design, a managed ODBC database is an object that
/// carries the ODBC environment handle and the connection options that will
/// be used to create connections. Note that more options can be provided when
/// creating a connection. This lets an ODBC driver cache some information
/// in the environment handle and share it across connections.
#[derive(Clone)]
#[cfg(feature = "odbc")]
pub(crate) struct OdbcDatabase {
    backend: Backend,
    env: OdbcEnv,
    /// Since ODBC doesn't have the concept of a [`Database`], only connections,
    /// this variant only carries the configuration options that are later passed
    /// to the connections.
    options: Vec<(OptionConnection, OptionValue)>,
}

#[cfg(feature = "odbc")]
impl OdbcDatabase {
    pub fn try_new(backend: Backend) -> Result<Self> {
        let env = OdbcEnv::try_new()?;
        let database = OdbcDatabase {
            backend,
            env,
            options: Vec::new(),
        };
        Ok(database)
    }

    pub fn try_new_with_opts(
        backend: Backend,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self> {
        let env = OdbcEnv::try_new()?;
        let connection_options = opts
            .into_iter()
            .map(|(k, v)| {
                let option_connection = match k {
                    OptionDatabase::Other(s) => OptionConnection::Other(s),
                    OptionDatabase::Uri => {
                        // XXX: I don't believe the "URI" option really exist
                        // in the ODBC API, but we need to forward it somehow
                        OptionConnection::Other("URI".to_string())
                    }
                    OptionDatabase::Username => OptionConnection::Other("UID".to_string()),
                    OptionDatabase::Password => OptionConnection::Other("PWD".to_string()),
                    _ => todo!("unsupported OptionDatabase variant for ODBC"),
                };
                (option_connection, v)
            })
            .collect::<Vec<_>>();
        Ok(OdbcDatabase {
            backend,
            env,
            options: connection_options,
        })
    }

    fn augment_new_connection_error(
        &self,
        e: Error,
        opts: &[(OptionConnection, OptionValue)],
    ) -> Error {
        let mut e = e;
        for (k, v) in opts.iter() {
            match k {
                // connection errors can happen due to a missing ODBC driver in the system
                OptionConnection::Other(k) if k == "Driver" => {
                    if let OptionValue::String(driver) = v {
                        if str_from_sqlstate(&e.sqlstate) != "01000" {
                            // errors related to driver location are general warnings (SQLSTATE 01000),
                            // so skip this augmentation if the error is not that
                            continue;
                        }
                        let driver_path = std::path::Path::new(driver);
                        if !driver_path.exists() {
                            e.message = format!(
                                "{}\nHint: install the {} ODBC driver if you have not done so yet.",
                                e.message, self.backend,
                            );
                            match self.backend {
                                Backend::DatabricksODBC => {
                                    const URL: &str =
                                        "https://www.databricks.com/spark/odbc-drivers-download";
                                    e.message = format!(
                                        "{} The Databricks ODBC driver can be downloaded from {} .\n\
If you have already installed it and know the location of the driver in your system, \
try setting the {} environment variable to the correct location and try again.",
                                        e.message,
                                        URL,
                                        crate::databricks::odbc::DATABRICKS_DRIVER_PATH_ENV_VAR_NAME,
                                    );
                                }
                                Backend::RedshiftODBC => {
                                    #[cfg(target_os = "linux")]
                                    const URL: &str = "https://docs.aws.amazon.com/redshift/latest/mgmt/odbc-driver-linux-how-to-install.html";
                                    #[cfg(target_os = "macos")]
                                    const URL: &str = "https://docs.aws.amazon.com/redshift/latest/mgmt/odbc-driver-mac-how-to-install.html";
                                    #[cfg(target_os = "windows")]
                                    const URL: &str = "https://docs.aws.amazon.com/redshift/latest/mgmt/odbc-driver-windows-how-to-install.html";
                                    e.message = format!(
                                        "{} The Amazon Redshift ODBC driver can be downloaded from {} .\n\
If you have already installed it and know the location of the driver in your system, \
try setting the {} environment variable to the correct location and try again.",
                                        e.message,
                                        URL,
                                        crate::redshift::odbc::REDSHIFT_DRIVER_PATH_ENV_VAR_NAME,
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
        e
    }
}

#[cfg(feature = "odbc")]
impl Database for OdbcDatabase {
    fn new_connection(&mut self) -> Result<Box<dyn Connection>> {
        unimplemented!("OdbcDatabase::new_connection: use new_connection_with_opts() instead")
    }

    fn new_connection_with_opts(
        &mut self,
        opts: Vec<(OptionConnection, OptionValue)>,
    ) -> Result<Box<dyn Connection>> {
        let mut conn_string = String::new();
        let opt_value_as_string = |v: &OptionValue| match v {
            OptionValue::String(s) => Ok(s.to_owned()),
            OptionValue::Int(i) => Ok(i.to_string()),
            _ => Err(Error::with_message_and_status(
                format!("invalid option value type for ODBC driver: {v:?}"),
                Status::InvalidArguments,
            )),
        };
        let mut push_option = |opt: &(OptionConnection, OptionValue)| -> Result<()> {
            match opt {
                (OptionConnection::Other(k), v) => {
                    let s = opt_value_as_string(v)?;
                    // TODO(felipecrv): implement escaping for ODBC option values
                    conn_string.push_str(&format!("{k}={s};"));
                    Ok(())
                }
                _ => unimplemented!("non-string ODBC connection option handling"),
            }
        };
        for opt in self.options.iter() {
            push_option(opt)?;
        }
        for opt in opts.iter() {
            push_option(opt)?;
        }
        let conn = self
            .env
            .new_connection(&conn_string)
            .map_err(|e| self.augment_new_connection_error(e, self.options.as_slice()))?;
        let conn = OdbcConnection(self.backend, Arc::new(conn));
        Ok(Box::new(conn))
    }

    fn set_option(&mut self, _key: OptionDatabase, _value: OptionValue) -> Result<()> {
        todo!("ODBC database option setting")
    }

    fn get_option_string(&self, _key: OptionDatabase) -> Result<String> {
        todo!("ODBC database string option retrieval")
    }

    fn get_option_bytes(&self, _key: OptionDatabase) -> Result<Vec<u8>> {
        todo!("ODBC database bytes option retrieval")
    }

    fn get_option_int(&self, _key: OptionDatabase) -> Result<i64> {
        todo!("ODBC database integer option retrieval")
    }

    fn get_option_double(&self, _key: OptionDatabase) -> Result<f64> {
        todo!("ODBC database float option retrieval")
    }

    fn clone_box(&self) -> Box<dyn Database> {
        Box::new(self.clone())
    }
}

#[cfg(feature = "odbc")]
impl DatabaseInfo for OdbcDatabase {
    fn get_info(&mut self, _info_code: InfoCode) -> Result<Arc<dyn Array>> {
        todo!("ODBC database info retrieval")
    }
}

#[derive(Debug, Copy, Clone, Ord, Eq, PartialOrd, PartialEq)]
pub struct Fingerprint {
    h1: u64,
    h2: u64,
}

impl Hash for Fingerprint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // the fingerprint itself is already a hash, so we can use a truncated
        // version of it to produce a 64-bit hash of the fingerprint
        state.write_u64(self.h1)
    }
}

/// The key used to fingerprint the database configuration.
static HASHER_KEY: LazyLock<[u8; 16]> = LazyLock::new(|| {
    let mut key = [0u8; 16];
    getrandom::getrandom(&mut key).unwrap();
    key
});

#[inline]
pub(crate) fn fingerprint_config<'a>(
    opts: impl Iterator<Item = &'a (OptionDatabase, OptionValue)>,
) -> Fingerprint {
    let mut hasher = SipHasher24::new_with_key(&HASHER_KEY);
    for (name, value) in opts {
        match name {
            OptionDatabase::Uri => hasher.write_u64(1),
            OptionDatabase::Username => hasher.write_u64(2),
            OptionDatabase::Password => hasher.write_u64(3),
            OptionDatabase::Other(name) => {
                hasher.write_u64(4);
                hasher.write_u64(name.len() as u64);
                hasher.write(name.as_bytes());
            }
            _ => {
                let bytes = name.as_ref().as_bytes();
                hasher.write_u64(5);
                hasher.write_u64(bytes.len() as u64);
                hasher.write(bytes);
            }
        }

        match value {
            OptionValue::String(s) => {
                hasher.write_u64(1);
                hasher.write_u64(s.len() as u64);
                hasher.write(s.as_bytes());
            }
            OptionValue::Bytes(b) => {
                hasher.write_u64(2);
                hasher.write_u64(b.len() as u64);
                hasher.write(b);
            }
            OptionValue::Int(i) => {
                hasher.write_u64(3);
                hasher.write_u64(*i as u64);
            }
            OptionValue::Double(d) => {
                hasher.write_u64(4);
                hasher.write_u64(d.to_bits());
            }
            _ => panic!("unexpected OptionValue variant"),
        }
    }
    let hash = hasher.finish128();
    Fingerprint {
        h1: hash.h1,
        h2: hash.h2,
    }
}

#[cfg(test)]
mod tests {
    use crate::database::{Fingerprint, fingerprint_config};
    use crate::{Backend, database};
    use std::collections::HashSet;

    #[test]
    fn config_fingerprinting() {
        const BACKEND: Backend = Backend::Snowflake;
        let config1 = {
            let mut builder = database::Builder::new(BACKEND);
            builder
                .with_username("user")
                .with_password("password")
                .with_parse_uri("https://snowflakecomputing.com")
                .unwrap()
                .with_named_option("option", "value")
                .unwrap();
            builder.into_iter().collect::<Vec<_>>()
        };
        let config2 = {
            let mut builder = database::Builder::new(BACKEND);
            builder
                .with_username("user")
                .with_password("password")
                .with_parse_uri("https://snowflakecomputing.com")
                .unwrap()
                .with_named_option("option=value", "")
                .unwrap();
            builder.into_iter().collect::<Vec<_>>()
        };
        let config3 = {
            let mut builder = database::Builder::new(BACKEND);
            builder
                .with_username("user")
                .with_password("password")
                .with_parse_uri("https://snowflakecomputing.com")
                .unwrap();
            builder.into_iter().collect::<Vec<_>>()
        };
        let fingerprint1 = fingerprint_config(config1.iter());
        let fingerprint2 = fingerprint_config(config2.iter());
        let fingerprint3 = fingerprint_config(config3.iter());
        let fingerprint_set: HashSet<Fingerprint> =
            HashSet::from_iter([fingerprint1, fingerprint2, fingerprint3]);
        assert_eq!(fingerprint_set.len(), 3);
    }
}
