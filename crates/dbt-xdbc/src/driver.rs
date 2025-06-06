//! ADBC Driver
//!
//!

use crate::database::AdbcDatabase;
#[cfg(feature = "odbc")]
use crate::database::OdbcDatabase;
use crate::install;
use crate::semaphore::Semaphore;
use crate::Database;
use adbc_core::{
    driver_manager::ManagedDriver as ManagedAdbcDriver,
    error::{Error, Result, Status},
    options::{AdbcVersion, OptionDatabase, OptionValue},
    Driver as _,
};
use libloading;
use parking_lot::RwLockUpgradableReadGuard;
use std::sync::Arc;
use std::{
    collections::HashMap, env, ffi::c_int, fmt, fmt::Display, hash::Hash, path::PathBuf,
    sync::LazyLock,
};

mod builder;
pub use builder::*;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Backend {
    /// Snowflake driver implementation (ADBC).
    Snowflake,
    /// BigQuery driver implementation (ADBC).
    BigQuery,
    /// PostgreSQL driver implementation (ADBC).
    Postgres,
    /// Databricks driver implementation (ADBC).
    Databricks,
    /// Databricks driver implementation (ODBC).
    DatabricksODBC,
    /// Redshift driver implementation (ODBC).
    RedshiftODBC,
    /// Generic ADBC driver implementation.
    ///
    /// This variant is fully dynamic and experimental. Features might not work reliably and fail
    /// at runtime.
    Generic {
        /// The name of the dynamic library without prefix or suffix.
        ///
        /// Example: `adbc_driver_sqlite`.
        library_name: &'static str,
        /// The entry point of the dynamic library.
        ///
        /// Example: `Some(b"SqliteDriverInit")`.
        entrypoint: Option<&'static [u8]>,
    },
}

impl Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backend::Snowflake => write!(f, "Snowflake"),
            Backend::BigQuery => write!(f, "BigQuery"),
            Backend::Postgres => write!(f, "PostgreSQL"),
            Backend::Databricks => write!(f, "Databricks"),
            Backend::DatabricksODBC => write!(f, "Databricks"),
            Backend::RedshiftODBC => write!(f, "Redshift"),
            Backend::Generic { library_name, .. } => write!(f, "Generic({})", library_name),
        }
    }
}

impl Backend {
    pub fn adbc_library_name(&self) -> Option<&'static str> {
        match self {
            Backend::Snowflake => Some("adbc_driver_snowflake"),
            Backend::BigQuery => Some("adbc_driver_bigquery"),
            Backend::Postgres => Some("adbc_driver_postgresql"),
            Backend::Databricks => Some("adbc_driver_databricks"),
            Backend::DatabricksODBC | Backend::RedshiftODBC => None, // these use ODBC
            Backend::Generic { library_name, .. } => Some(library_name),
        }
    }

    pub fn adbc_driver_entrypoint(&self) -> Option<&'static [u8]> {
        match self {
            Backend::Snowflake => Some(b"SnowflakeDriverInit"),
            Backend::Generic {
                library_name: _,
                entrypoint,
            } => *entrypoint,
            _ => None,
        }
    }

    pub(crate) fn ffi_protocol(&self) -> FFIProtocol {
        match self {
            Backend::Snowflake => FFIProtocol::Adbc,
            Backend::BigQuery => FFIProtocol::Adbc,
            Backend::Postgres => FFIProtocol::Adbc,
            Backend::Databricks => FFIProtocol::Adbc,
            Backend::DatabricksODBC => FFIProtocol::Odbc,
            Backend::RedshiftODBC => FFIProtocol::Odbc,
            Backend::Generic { .. } => FFIProtocol::Adbc,
        }
    }
}

/// Private enum used to determine the FFI protocol to use for a given backend.
///
/// The Rust interface is the same for all backends and follows ADBC conventions,
/// but the FFI protocol might be ADBC (direct) or ODBC (with translation).
#[derive(PartialEq)]
pub(crate) enum FFIProtocol {
    /// Arrow Database Connectivity Protocol
    Adbc,
    /// Open Database Connectivity Protocol
    Odbc,
}

/// XDBC Driver.
///
/// A [`Driver`] is a wrapper around a loaded ADBC/ODBC driver. With a driver, you can create
/// new [`Database`] instances that, in turn, can create new [`Connection`] instances.
pub trait Driver {
    fn new_database(&mut self) -> Result<Box<dyn Database>>;

    fn new_database_with_opts(
        &mut self,
        opts: Vec<(OptionDatabase, OptionValue)>,
    ) -> Result<Box<dyn Database>>;
}

/// A key used to cache loaded ADBC drivers.
#[derive(PartialEq, Eq)]
struct AdbcDriverKey {
    backend: Backend,
    adbc_version: AdbcVersion,
}

impl Hash for AdbcDriverKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.backend.hash(state);
        c_int::from(self.adbc_version).hash(state);
    }
}

/// Climb up the directory tree and returns the first lib/ directory found.
fn find_adbc_libs_directory() -> Option<PathBuf> {
    const MAX_HEIGHT: i32 = 5;
    let mut height = 0;
    let mut current_dir = env::current_exe().ok()?.parent()?.to_path_buf();
    loop {
        let sibling_lib = current_dir.join("lib");
        if sibling_lib.is_dir() {
            return Some(sibling_lib);
        }
        if !current_dir.pop() {
            break;
        }
        height += 1;
        if height > MAX_HEIGHT {
            break;
        }
    }
    None
}

/// Directory used by [`AdbcDriver::load_dynamic_from_name`].
static ADBC_LIBS_DIRECTORY: LazyLock<Option<PathBuf>> = LazyLock::new(find_adbc_libs_directory);
/// All loaded ADBC drivers are cached in `LOADED_ADBC_DRIVERS`, no matter the loading strategy used.
static LOADED_ADBC_DRIVERS: LazyLock<
    parking_lot::RwLock<HashMap<AdbcDriverKey, Result<ManagedAdbcDriver>>>,
> = LazyLock::new(|| parking_lot::RwLock::new(HashMap::new()));

pub(crate) struct AdbcDriver {
    backend: Backend,
    driver: ManagedAdbcDriver,
    semaphore: Option<Arc<Semaphore>>,
}

impl AdbcDriver {
    /// Returns an ADBC [`Driver`] for a given [`Backend`] and [`AdbcVersion`].
    pub fn try_load_dynamic(
        backend: Backend,
        adbc_version: AdbcVersion,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Result<Self> {
        Self::try_load_driver_trough_cache(backend, adbc_version).map(|driver| Self {
            backend,
            driver,
            semaphore,
        })
    }

    /// Check the read-trough cache of loaded ADBC drivers before loading a new one.
    fn try_load_driver_trough_cache(
        backend: Backend,
        adbc_version: AdbcVersion,
    ) -> Result<ManagedAdbcDriver> {
        let key = AdbcDriverKey {
            backend,
            adbc_version,
        };
        let cache = LOADED_ADBC_DRIVERS.upgradable_read();
        if let Some(driver) = cache.get(&key) {
            return driver.clone();
        }
        // Upgrade the lock for writes before the driver is loaded. This also prevents
        // multiple threads from calling non-thread-safe OS functions used to load the driver.
        let mut cache = RwLockUpgradableReadGuard::upgrade(cache);
        // check again after exclusive lock
        if let Some(driver) = cache.get(&key) {
            return driver.clone();
        }
        let driver = Self::try_load_driver_internal(backend, adbc_version);
        cache.insert(key, driver.clone());
        driver
    }

    fn try_load_driver_internal(
        backend: Backend,
        adbc_version: AdbcVersion,
    ) -> Result<ManagedAdbcDriver> {
        match backend {
            // These drivers are published to the dbt Labs CDN.
            Backend::Snowflake | Backend::BigQuery | Backend::Postgres | Backend::Databricks => {
                debug_assert!(backend.ffi_protocol() == FFIProtocol::Adbc);
                debug_assert!(install::is_installable_driver(backend));
                #[cfg(debug_assertions)]
                {
                    // This option is only used during development of ADBC drivers to make sure
                    // the drivers are not downloaded from the CDN and are instead loaded from
                    // the nearby lib/ directory where debug builds of the drivers can be placed.
                    let disable_cdn_driver_cache = env::var("DISABLE_CDN_DRIVER_CACHE").is_ok();
                    if disable_cdn_driver_cache {
                        eprintln!(
                            "WARNING: {} ADBC driver is being loaded from {} in debug mode.",
                            backend,
                            ADBC_LIBS_DIRECTORY.as_ref().unwrap().display()
                        );
                        return Self::try_load_driver_from_name(
                            backend.adbc_library_name().unwrap(), // safe because it's ADBC
                            backend.adbc_driver_entrypoint(),
                            adbc_version,
                        );
                    }
                }
                Self::try_load_driver_through_cdn_cache(backend, adbc_version)
            }
            // Drivers that are not published to the dbt Labs CDN.
            Backend::Generic { .. } => Self::try_load_driver_from_name(
                backend.adbc_library_name().unwrap(),
                backend.adbc_driver_entrypoint(),
                adbc_version,
            ),
            // ODBC drivers.
            Backend::DatabricksODBC | Backend::RedshiftODBC => Err(Error::with_message_and_status(
                format!(
                    "Can not load ADBC driver for {:?} because ODBC should be used instead.",
                    backend
                ),
                Status::InvalidArguments,
            )),
        }
    }

    /// Simple driver loading function.
    ///
    /// This function relies on the OS to find the library in the system path or something like
    /// LD_LIBRARY_PATH. If that fails it climbs up the directory tree and chooses the first lib/
    /// directory it can find. This is used for drivers that are not published to the dbt Labs CDN
    /// or might not be part of the standard set of drivers.
    fn try_load_driver_from_name(
        name: &str,
        entrypoint: Option<&[u8]>,
        adbc_version: AdbcVersion,
    ) -> Result<ManagedAdbcDriver> {
        // Rely on the OS to find the library in the system path or something like LD_LIBRARY_PATH.
        let res = ManagedAdbcDriver::load_dynamic_from_name(name, entrypoint, adbc_version);
        if res.is_ok() {
            return res;
        }
        // If it fails, we climb up the directory tree and choose the first lib/ directory we can
        // find. The result of that search is cached in LIBS_DIRECTORY.
        if let Some(libs_dir) = ADBC_LIBS_DIRECTORY.as_ref() {
            let qualified_filename = libloading::library_filename(name);
            let full_path = libs_dir
                .join(qualified_filename)
                .to_string_lossy()
                .into_owned();
            ManagedAdbcDriver::load_dynamic_from_filename(full_path, entrypoint, adbc_version)
        } else {
            res
        }
    }

    fn try_load_driver_through_cdn_cache(
        backend: Backend,
        adbc_version: AdbcVersion,
    ) -> Result<ManagedAdbcDriver> {
        let entrypoint = backend.adbc_driver_entrypoint();
        let (backend_name, version, target_os) = install::driver_parameters(backend);
        let full_driver_path = install::format_driver_path(backend_name, version, target_os)
            .map_err(|e| e.to_adbc_error())?;
        ManagedAdbcDriver::load_dynamic_from_filename(&full_driver_path, entrypoint, adbc_version)
            .or_else(|_| {
                install::install_driver_internal(backend_name, version, target_os)
                    .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;

                let driver = ManagedAdbcDriver::load_dynamic_from_filename(
                    &full_driver_path,
                    entrypoint,
                    adbc_version,
                )?;
                Ok(driver)
            })
    }
}

impl Driver for AdbcDriver {
    fn new_database(&mut self) -> Result<Box<dyn Database>> {
        let managed_database = self.driver.new_database()?;
        let database = AdbcDatabase::new(self.backend, managed_database, self.semaphore.clone());
        Ok(Box::new(database))
    }

    fn new_database_with_opts(
        &mut self,
        opts: Vec<(OptionDatabase, OptionValue)>,
    ) -> Result<Box<dyn Database>> {
        let managed_database = self.driver.new_database_with_opts(opts)?;
        let database = AdbcDatabase::new(self.backend, managed_database, self.semaphore.clone());
        Ok(Box::new(database))
    }
}

#[cfg(feature = "odbc")]
pub(crate) struct OdbcDriver(Backend);

#[cfg(feature = "odbc")]
impl OdbcDriver {
    pub(crate) fn try_load_dynamic(backend: Backend) -> Result<Self> {
        match backend.ffi_protocol() {
            FFIProtocol::Adbc => Err(Error::with_message_and_status(
                format!("The {:?} backend uses ADBC instead of ODBC", backend),
                Status::InvalidArguments,
            )),
            FFIProtocol::Odbc => {
                // NOTE: this function might come in handy if we start loading the ODBC driver
                // *manager* library dynamically as well as the drivers. This is not at all an
                // issue for ADBC because we statically link the ADBC driver manager library.
                //
                // We can't statically link the unixODBC driver manager library because it's
                // GPL-licensed.
                let driver = Self(backend);
                Ok(driver)
            }
        }
    }
}

#[cfg(feature = "odbc")]
impl Driver for OdbcDriver {
    fn new_database(&mut self) -> Result<Box<dyn Database>> {
        let database = OdbcDatabase::try_new(self.0)?;
        Ok(Box::new(database))
    }

    fn new_database_with_opts(
        &mut self,
        opts: Vec<(OptionDatabase, OptionValue)>,
    ) -> Result<Box<dyn Database>> {
        let database = OdbcDatabase::try_new_with_opts(self.0, opts)?;
        Ok(Box::new(database))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_load_with_builder(backend: Backend, adbc_version: AdbcVersion) -> Result<()> {
        Builder::new(backend)
            .with_adbc_version(adbc_version)
            .try_load()?;
        Ok(())
    }

    // XXX: remove the `test_with` attribute when the CI image downloads the Snowflake driver.

    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    fn load_v1_0_0() -> Result<()> {
        try_load_with_builder(Backend::Snowflake, AdbcVersion::V100)?;
        try_load_with_builder(Backend::BigQuery, AdbcVersion::V100)?;
        try_load_with_builder(Backend::Postgres, AdbcVersion::V100)?;
        try_load_with_builder(Backend::Databricks, AdbcVersion::V100)?;
        Ok(())
    }

    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    fn load_v1_1_0() -> Result<()> {
        try_load_with_builder(Backend::Snowflake, AdbcVersion::V110)?;
        try_load_with_builder(Backend::BigQuery, AdbcVersion::V110)?;
        try_load_with_builder(Backend::Postgres, AdbcVersion::V110)?;
        try_load_with_builder(Backend::Databricks, AdbcVersion::V110)?;
        Ok(())
    }

    #[test_with::env(ADBC_DRIVER_TESTS)]
    #[test]
    fn dynamic() -> Result<()> {
        for backend in [
            Backend::Snowflake,
            Backend::BigQuery,
            Backend::Postgres,
            Backend::Databricks,
        ]
        .iter()
        .copied()
        {
            let _a = AdbcDriver::try_load_dynamic(backend, AdbcVersion::default(), None)?;
            let _b = AdbcDriver::try_load_dynamic(backend, AdbcVersion::default(), None)?;
        }
        Ok(())
    }

    #[cfg(feature = "odbc")]
    #[test_with::env(ODBC_DATABRICKS_TESTS)]
    #[test]
    fn dynamic_odbc() -> Result<()> {
        let _ = OdbcDriver::try_load_dynamic(Backend::DatabricksODBC)?;
        Ok(())
    }
}
