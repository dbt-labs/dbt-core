use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{dbt_utils::get_dbt_schema_version, schemas::InternalDbtNode, state::ResolverState};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogMetadata {
    pub dbt_schema_version: String,
    pub dbt_version: String,
    pub generated_at: DateTime<Utc>,
    pub invocation_id: Option<String>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogNode {
    pub unique_id: Option<String>,
    pub metadata: CatalogNodeMetadata,
    pub columns: BTreeMap<String, CatalogColumn>,
    pub stats: BTreeMap<String, CatalogNodeStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogNodeMetadata {
    pub materialization_type: String, // TODO this should just be "type"
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
    pub value: String,
    pub include: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogColumn {
    pub data_type: String, // TODO this should just be "type"
    pub index: i32,
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogSource {
    pub unique_id: Option<String>,
    pub metadata: CatalogNodeMetadata,
    pub columns: BTreeMap<String, CatalogColumn>,
    pub stats: BTreeMap<String, CatalogNodeStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogTable {
    pub name: String,
    pub description: Option<String>,
    pub columns: BTreeMap<String, CatalogColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbtCatalog {
    pub metadata: CatalogMetadata,
    pub nodes: BTreeMap<String, CatalogNode>,
    pub sources: BTreeMap<String, CatalogSource>,
    pub errors: Option<Vec<String>>,
}

// TODO: implement Serialize for DbtCatalog and don't derive it (for things like "materialization_type" -> "type")

pub fn build_catalog(invocation_id: &str, resolver_state: &ResolverState) -> DbtCatalog {
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
                (
                    id.clone(),
                    CatalogNode {
                        unique_id: Some(id.clone()),
                        metadata: CatalogNodeMetadata {
                            materialization_type: node.base_attr.materialized.clone().to_string(),
                            schema: node.base_attr.schema.clone(),
                            name: node.base_attr.alias.clone(),
                            database: Some(node.base_attr.database.clone()),
                            comment: node.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: node
                            .base()
                            .columns
                            .iter()
                            .map(|(name, column)| {
                                (
                                    name.clone(),
                                    CatalogColumn {
                                        data_type: column.data_type.clone().unwrap_or_default(),
                                        index: 1, // TODO: determine index
                                        name: name.clone(),
                                        comment: column.description.clone(),
                                    },
                                )
                            })
                            .collect(),
                        stats: BTreeMap::new(),
                    },
                )
            })
            .chain(resolver_state.nodes.snapshots.iter().map(|(id, node)| {
                (
                    id.clone(),
                    CatalogNode {
                        unique_id: Some(id.clone()),
                        metadata: CatalogNodeMetadata {
                            materialization_type: node.base_attr.materialized.clone().to_string(),
                            schema: node.base_attr.schema.clone(),
                            name: node.base_attr.alias.clone(),
                            database: Some(node.base_attr.database.clone()),
                            comment: node.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: node
                            .base()
                            .columns
                            .iter()
                            .map(|(name, column)| {
                                (
                                    name.clone(),
                                    CatalogColumn {
                                        data_type: column.data_type.clone().unwrap_or_default(),
                                        index: 1, // TODO: determine index
                                        name: name.clone(),
                                        comment: column.description.clone(),
                                    },
                                )
                            })
                            .collect(),
                        stats: BTreeMap::new(),
                    },
                )
            }))
            .chain(resolver_state.nodes.seeds.iter().map(|(id, node)| {
                (
                    id.clone(),
                    CatalogNode {
                        unique_id: Some(id.clone()),
                        metadata: CatalogNodeMetadata {
                            materialization_type: node.base_attr.materialized.clone().to_string(),
                            schema: node.base_attr.schema.clone(),
                            name: node.base_attr.alias.clone(),
                            database: Some(node.base_attr.database.clone()),
                            comment: node.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: node
                            .base()
                            .columns
                            .iter()
                            .map(|(name, column)| {
                                (
                                    name.clone(),
                                    CatalogColumn {
                                        data_type: column.data_type.clone().unwrap_or_default(),
                                        index: 1, // TODO: determine index
                                        name: name.clone(),
                                        comment: column.description.clone(),
                                    },
                                )
                            })
                            .collect(),
                        stats: BTreeMap::new(),
                    },
                )
            }))
            .collect(),
        sources: resolver_state
            .nodes
            .sources
            .iter()
            .map(|(id, source)| {
                (
                    id.clone(),
                    CatalogSource {
                        unique_id: Some(id.clone()),
                        metadata: CatalogNodeMetadata {
                            materialization_type: source.base_attr.materialized.clone().to_string(),
                            schema: source.base_attr.schema.clone(),
                            name: source.base_attr.alias.clone(),
                            database: Some(source.base_attr.database.clone()),
                            comment: source.common_attr.description.clone(),
                            owner: None,
                        },
                        columns: source
                            .base()
                            .columns
                            .iter()
                            .map(|(name, column)| {
                                (
                                    name.clone(),
                                    CatalogColumn {
                                        data_type: column.data_type.clone().unwrap_or_default(),
                                        index: 1, // TODO: determine index
                                        name: name.clone(),
                                        comment: column.description.clone(),
                                    },
                                )
                            })
                            .collect(),
                        stats: BTreeMap::new(),
                    },
                )
            })
            .collect(),
        errors: None,
    }
}
