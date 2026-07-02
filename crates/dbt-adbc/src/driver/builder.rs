//! A builder for [`Driver`]
//!
//!

use std::sync::Arc;

use super::LoadStrategy;
use crate::{Backend, Driver, driver::AdbcDriver, semaphore::Semaphore};
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

    /// The strategy for loading the driver.
    pub load_strategy: LoadStrategy,
}

impl Builder {
    pub fn new(backend: Backend, load_strategy: LoadStrategy) -> Self {
        Self {
            backend,
            adbc_version: None,
            semaphore: None,
            load_strategy,
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

    /// Set the strategy for loading the driver.
    pub fn with_load_strategy(&mut self, load_strategy: LoadStrategy) -> &mut Self {
        self.load_strategy = load_strategy;
        self
    }

    /// Try to load the [`Driver`] using the values provided to this builder.
    pub fn try_load(&self) -> Result<Box<dyn Driver>> {
        let adbc_driver = AdbcDriver::try_load_dynamic(
            self.backend,
            self.adbc_version.unwrap_or_default(),
            self.semaphore.clone(),
            self.load_strategy.clone(),
        )?;
        let driver = Box::new(adbc_driver);
        Ok(driver)
    }
}

impl TryFrom<Builder> for Box<dyn Driver> {
    type Error = Error;

    fn try_from(builder: Builder) -> Result<Self> {
        builder.try_load()
    }
}
