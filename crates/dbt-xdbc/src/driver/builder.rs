//! A builder for [`Driver`]
//!
//!

use std::sync::Arc;

#[cfg(feature = "odbc")]
use super::OdbcDriver;
use super::{AdbcDriver, FFIProtocol};
use crate::{Backend, Driver, semaphore::Semaphore};
#[allow(unused_imports)]
use adbc_core::{
    error::{Error, Result, Status},
    options::AdbcVersion,
};

/// A builder for [`Driver`].
#[derive(Clone, Debug)]
pub struct Builder {
    /// The backend target of the driver.
    pub backend: Backend,

    /// The optionally required [`AdbcVersion`] version of the driver.
    pub adbc_version: Option<AdbcVersion>,

    /// The semaphore for limiting the number of concurrent parallelism.
    pub semaphore: Option<Arc<Semaphore>>,
}

impl Builder {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            adbc_version: None,
            semaphore: None,
        }
    }

    /// Require the provided [`AdbcVersion`] when loading the driver.
    pub fn with_adbc_version(&mut self, adbc_version: AdbcVersion) -> &mut Self {
        self.adbc_version = Some(adbc_version);
        self
    }

    /// Set the semaphore for limiting the number of concurrent connections.
    pub fn with_semaphore(&mut self, semaphore: Arc<Semaphore>) -> &mut Self {
        self.semaphore = Some(semaphore);
        self
    }

    /// Try to load the [`Driver`] using the values provided to this builder.
    pub fn try_load(&self) -> Result<Box<dyn Driver>> {
        match self.backend.ffi_protocol() {
            FFIProtocol::Adbc => {
                let adbc_driver = AdbcDriver::try_load_dynamic(
                    self.backend,
                    self.adbc_version.unwrap_or_default(),
                    self.semaphore.clone(),
                )?;
                let driver = Box::new(adbc_driver);
                Ok(driver)
            }
            FFIProtocol::Odbc => {
                #[cfg(feature = "odbc")]
                {
                    let odbc_driver = OdbcDriver::try_load_dynamic(self.backend)?;
                    let driver = Box::new(odbc_driver);
                    Ok(driver)
                }
                #[cfg(not(feature = "odbc"))]
                {
                    let error = Error::with_message_and_status(
                        "ODBC driver support is not enabled",
                        Status::NotImplemented,
                    );
                    Err(error)
                }
            }
        }
    }
}

impl TryFrom<Builder> for Box<dyn Driver> {
    type Error = Error;

    fn try_from(builder: Builder) -> Result<Self> {
        builder.try_load()
    }
}
