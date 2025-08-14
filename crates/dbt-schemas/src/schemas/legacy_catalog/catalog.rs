use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{dbt_utils::get_dbt_schema_version, state::ResolverState};

fn default_dbt_version() -> String {
    "1.10.0a1".to_string()
}

// TODO: a lot of these are nullable, need to confirm against schema

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogMetadata {
    pub dbt_schema_version: String,
    #[serde(default = "default_dbt_version")]
    pub dbt_version: String,
    pub generated_at: DateTime<Utc>,
    pub invocation_id: Option<String>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogTable {
    pub metadata: TableMetadata,
    pub columns: BTreeMap<String, ColumnMetadata>,
    pub stats: BTreeMap<String, CatalogNodeStats>,
    pub unique_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableMetadata {
    #[serde(rename = "type")]
    pub materialization_type: String,
    pub schema: String,
    pub name: String,
    pub database: Option<String>,
    pub comment: Option<String>,
    pub owner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogNodeStats {
    pub id: String,
    pub label: String,
    pub value: Value,
    pub include: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnMetadata {
    #[serde(rename = "type")]
    pub data_type: String,
    pub index: i128, // TODO: this is only i128 because Snowflake is giving that back
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbtCatalog {
    pub metadata: CatalogMetadata,
    pub nodes: BTreeMap<String, CatalogTable>,
    pub sources: BTreeMap<String, CatalogTable>,
    pub errors: Option<Vec<String>>,
}

// TODO: implement Serialize for DbtCatalog and don't derive it (for things like "materialization_type" -> "type")
// TODO: dedupe code below
pub fn build_catalog(
    invocation_id: &str,
    resolver_state: &ResolverState,
    node_stats_and_stuff: BTreeMap<String, CatalogTable>,
    node_columns: BTreeMap<String, BTreeMap<String, ColumnMetadata>>,
) -> DbtCatalog {
    DbtCatalog {
        metadata: CatalogMetadata {
            dbt_schema_version: get_dbt_schema_version("catalog", 1),
            dbt_version: env!("CARGO_PKG_VERSION").to_string(),
            generated_at: Utc::now(),
            invocation_id: Some(invocation_id.to_string()),
            env: BTreeMap::new(), // TODO: how do we get env?
        },
        nodes: resolver_state
            .nodes
            .models
            .iter()
            .map(|(id, node)| {
                let fully_qualified_name = format!(
                    "{}.{}.{}",
                    node.base_attr.database.clone(),
                    node.base_attr.schema.clone(),
                    node.base_attr.alias.clone()
                );
                let mut partial_node = match node_stats_and_stuff.get(&fully_qualified_name) {
                    Some(node) => node.to_owned(),
                    None => CatalogTable {
                        unique_id: Some(id.clone()),
                        metadata: TableMetadata {
                            materialization_type: node.base_attr.materialized.clone().to_string(),
                            schema: node.base_attr.schema.clone(),
                            name: node.base_attr.alias.clone(),
                            database: Some(node.base_attr.database.clone()),
                            comment: node.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: BTreeMap::new(),
                        stats: BTreeMap::new(),
                    },
                };

                partial_node.unique_id = Some(id.clone());
                partial_node.columns = node_columns
                    .get(&fully_qualified_name)
                    .unwrap_or(&BTreeMap::new())
                    .to_owned();
                (id.clone(), partial_node)
            })
            .chain(resolver_state.nodes.snapshots.iter().map(|(id, node)| {
                let fully_qualified_name = format!(
                    "{}.{}.{}",
                    node.base_attr.database.clone(),
                    node.base_attr.schema.clone(),
                    node.base_attr.alias.clone()
                );
                let mut partial_node = match node_stats_and_stuff.get(&fully_qualified_name) {
                    Some(node) => node.to_owned(),
                    None => CatalogTable {
                        unique_id: Some(id.clone()),
                        metadata: TableMetadata {
                            materialization_type: node.base_attr.materialized.clone().to_string(),
                            schema: node.base_attr.schema.clone(),
                            name: node.base_attr.alias.clone(),
                            database: Some(node.base_attr.database.clone()),
                            comment: node.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: BTreeMap::new(),
                        stats: BTreeMap::new(),
                    },
                };

                partial_node.unique_id = Some(id.clone());
                partial_node.columns = node_columns
                    .get(&fully_qualified_name)
                    .unwrap_or(&BTreeMap::new())
                    .to_owned();
                (id.clone(), partial_node)
            }))
            .chain(resolver_state.nodes.seeds.iter().map(|(id, node)| {
                let fully_qualified_name = format!(
                    "{}.{}.{}",
                    node.base_attr.database.clone(),
                    node.base_attr.schema.clone(),
                    node.base_attr.alias.clone()
                );
                let mut partial_node = match node_stats_and_stuff.get(&fully_qualified_name) {
                    Some(node) => node.to_owned(),
                    None => CatalogTable {
                        unique_id: Some(id.clone()),
                        metadata: TableMetadata {
                            materialization_type: node.base_attr.materialized.clone().to_string(),
                            schema: node.base_attr.schema.clone(),
                            name: node.base_attr.alias.clone(),
                            database: Some(node.base_attr.database.clone()),
                            comment: node.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: BTreeMap::new(),
                        stats: BTreeMap::new(),
                    },
                };

                partial_node.unique_id = Some(id.clone());
                partial_node.columns = node_columns
                    .get(&fully_qualified_name)
                    .unwrap_or(&BTreeMap::new())
                    .to_owned();
                (id.clone(), partial_node)
            }))
            .collect(),
        sources: resolver_state
            .nodes
            .sources
            .iter()
            .map(|(id, source)| {
                let fully_qualified_name = format!(
                    "{}.{}.{}",
                    source.base_attr.database.clone(),
                    source.base_attr.schema.clone(),
                    source.base_attr.alias.clone()
                );
                let mut partial_node = match node_stats_and_stuff.get(&fully_qualified_name) {
                    Some(node) => node.to_owned(),
                    None => CatalogTable {
                        unique_id: Some(id.clone()),
                        metadata: TableMetadata {
                            materialization_type: source.base_attr.materialized.clone().to_string(),
                            schema: source.base_attr.schema.clone(),
                            name: source.base_attr.alias.clone(),
                            database: Some(source.base_attr.database.clone()),
                            comment: source.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: BTreeMap::new(),
                        stats: BTreeMap::new(),
                    },
                };

                partial_node.unique_id = Some(id.clone());
                partial_node.columns = node_columns
                    .get(&fully_qualified_name)
                    .unwrap_or(&BTreeMap::new())
                    .to_owned();
                (id.clone(), partial_node)
            })
            .collect(),
        errors: None, // TODO: look into errors and what this should look like
    }
}
