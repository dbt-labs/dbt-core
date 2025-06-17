use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// Represents a catalog integration configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogIntegrationConfig {
    pub name: String,
    pub catalog_type: String,
    pub table_format: Option<String>,
    pub file_format: Option<String>,
    pub external_volume: Option<String>,
}

/// Represents a catalog configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    pub name: String,
    pub active_write_integration: String,
    pub write_integrations: Vec<CatalogIntegrationConfig>,
}

/// Represents the top-level dbt catalog configuration
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtCatalogConfig {
    pub catalogs: Vec<CatalogConfig>,
}
