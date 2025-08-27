//! ADBC Driver
//!
//!

use crate::Database;
use crate::database::AdbcDatabase;
#[cfg(feature = "odbc")]
use crate::database::OdbcDatabase;
use crate::install;
use crate::semaphore::Semaphore;
use adbc_core::{
    Driver as _,
    driver_manager::ManagedDriver as ManagedAdbcDriver,
    error::{Error, Result, Status},
    options::{AdbcVersion, OptionDatabase, OptionValue},
};
use libloading;
use parking_lot::RwLockUpgradableReadGuard;
use std::sync::Arc;
use std::{
    collections::HashMap, env, ffi::c_int, fmt, fmt::Display, hash::Hash, path::Path,
    path::PathBuf, sync::LazyLock,
};

mod builder;
pub use builder::*;

#[cfg(debug_assertions)]
mod env_var;

#[cfg(debug_assertions)]
use {env_var::env_var_bool, std::io::ErrorKind, std::process::Command};

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
    /// Redshift driver implementation (ADBC).
    Redshift,
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
            Backend::Redshift => write!(f, "Redshift"),
            Backend::DatabricksODBC => write!(f, "Databricks"),
            Backend::RedshiftODBC => write!(f, "Redshift"),
            Backend::Generic { library_name, .. } => write!(f, "Generic({library_name})"),
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
            // todo: swap over to Redshift specific driver once available
            Backend::Redshift => Some("adbc_driver_postgresql"),
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
            Backend::Redshift => FFIProtocol::Adbc,
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

/// Attempt to run `make` in the location of arrow-adbc.
///
/// Only runs when `DISABLE_CDN_DRIVER_CACHE` is set and `DISABLE_AUTO_DRIVER_REBUILD`
/// is unset.
#[cfg(debug_assertions)]
fn rebuild_drivers(dir: &PathBuf) -> Result<()> {
    let needs_rebuild = match Command::new("make").arg("-C").arg(dir).arg("-q").status() {
        Ok(s) => s.code() == Some(1),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            eprintln!("`make` not found, skipping rebuild");
            false
        }
        Err(e) => {
            return Err(Error::with_message_and_status(
                format!("failed to spawn `make -q`: {e}"),
                Status::Internal,
            ));
        }
    };

    if needs_rebuild {
        let status = Command::new("make")
            .arg("-C")
            .arg(dir)
            .status()
            .expect("failed to spawn `make`");

        if !status.success() {
            return Err(Error::with_message_and_status(
                format!("`make` failed in {}", dir.display()),
                Status::Internal,
            ));
        }
    }
    Ok(())
}

/// Searches for subpath starting at `start` and continuing upward through its parents.
///
/// Always checks start. `max_hops = 0` checks `start` only.
/// does not canonicalize
pub fn find_upward_dir(start: &Path, subpath: &Path, max_hops: usize) -> Option<PathBuf> {
    if subpath.is_absolute() {
        return None;
    }

    for dir in start.ancestors().take(max_hops + 1) {
        let candidate = dir.join(subpath);
        if candidate.is_dir() {
            return Some(candidate);
        }
    }
    None
}

/// Climb up the directory tree and returns the first lib/ directory found.
fn find_adbc_libs_directory() -> Option<PathBuf> {
    // No. of dirs to walk is chosen for `dbt` to operate when the invoked dbt project is:
    // * a subdir of the fusion root but not past the crate level
    // * a sibling directory to the invoked fs repo
    // Also supports invocations by relative paths to <fs repo root>/target/debug/dbt
    const LIB_HEIGHT_MAX: usize = 5;
    #[cfg(debug_assertions)]
    const ARROW_HEIGHT_MAX: usize = 10;

    let starting_dir = env::current_exe().ok()?.parent()?.to_path_buf();

    #[cfg(debug_assertions)]
    {
        let arrow_adbc_pkg_rel_path: PathBuf = ["arrow-adbc", "go", "adbc", "pkg"].iter().collect();

        if let Some(sibling_arrow_adbc) =
            find_upward_dir(&starting_dir, &arrow_adbc_pkg_rel_path, ARROW_HEIGHT_MAX)
        {
            if !env_var_bool("DISABLE_AUTO_DRIVER_REBUILD").ok()? {
                rebuild_drivers(&sibling_arrow_adbc).unwrap();
            }
            return Some(sibling_arrow_adbc);
        }
    }

    let lib_dir_rel_path = &PathBuf::from("lib");

    if let Some(sibling_lib) = find_upward_dir(&starting_dir, lib_dir_rel_path, LIB_HEIGHT_MAX) {
        return Some(sibling_lib);
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
            Backend::Snowflake
            | Backend::BigQuery
            | Backend::Postgres
            | Backend::Databricks
            | Backend::Redshift => {
                debug_assert!(backend.ffi_protocol() == FFIProtocol::Adbc);
                debug_assert!(install::is_installable_driver(backend));
                #[cfg(debug_assertions)]
                {
                    // This option is only used during development of ADBC drivers to make sure
                    // the drivers are not downloaded from the CDN and are instead loaded from
                    // either the repo root lib/ directory or an arrow-adbc repo whose root is
                    // a sibling to this fs repo.
                    let disable_cdn_driver_cache = env_var_bool("DISABLE_CDN_DRIVER_CACHE")?;

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
                    "Can not load ADBC driver for {backend:?} because ODBC should be used instead."
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
                format!("The {backend:?} backend uses ADBC instead of ODBC"),
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
