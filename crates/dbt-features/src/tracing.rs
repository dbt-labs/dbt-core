use std::sync::Mutex;

use dbt_common::tracing::TelemetryHandle;
use dbt_common::tracing::TracingConfigProvider;
use dbt_common::tracing::error::TracingError;
use dbt_common::tracing::noop_tracing_config_provider;

pub struct TracingFeature {
    pub config_provider: Box<dyn TracingConfigProvider>,
    shutdown_handle: Mutex<Option<TelemetryHandle>>,
}

impl Default for TracingFeature {
    fn default() -> Self {
        Self {
            config_provider: noop_tracing_config_provider(),
            shutdown_handle: Mutex::new(None),
        }
    }
}

impl TracingFeature {
    pub fn with_config_provider(mut self, provider: Box<dyn TracingConfigProvider>) -> Self {
        self.config_provider = provider;
        self
    }

    pub fn with_shutdown_handle(mut self, handle: TelemetryHandle) -> Self {
        self.shutdown_handle = Mutex::new(Some(handle));
        self
    }

    pub fn shutdown_once(&self) -> Result<(), Vec<TracingError>> {
        if let Some(handle) = self.shutdown_handle.lock().unwrap().take() {
            handle.shutdown_once()
        } else {
            Ok(())
        }
    }
}
