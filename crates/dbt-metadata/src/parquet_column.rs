use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;

use dbt_common::FsResult;
use dbt_common::constants::*;
use dbt_common::io_args::IoArgs;
use dbt_metadata_parquet::utils::{generate_list_type, read_parquet_file};
use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::common::Constraint;
use dbt_schemas::schemas::dbt_column::{ColumnConfig, DbtColumn, DbtColumnRef};
use dbt_schemas::schemas::legacy_catalog::{CatalogTable, DbtCatalog};
use dbt_schemas::schemas::properties::ModelConstraint;
// for representing dbt metadata in Rust
use serde::{Deserialize, Serialize};

// ...and writeing Parquet files
use arrow::datatypes::{DataType, Field, Fields};
use dbt_yaml::Value;

use crate::parquet_node::{MetaFields, generate_constraints_fields, generate_meta_fields};

// ------------------------------------------------------------------------------------------------
// ColumnInfo represents a column in a dbt model, including its metadata and constraints.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ParquetColumn {
    pub unique_id: String, // e.g., "model.jaffle_shop.orders", fk, pk
    pub name: String,      // e.g., "order_id", pk
    pub description: Option<String>,
    pub comment: Option<String>,
    pub tags: Vec<String>,
    pub data_type: Option<String>,
    // a column constraint is like a model constraint, but for a column
    pub constraints: Vec<ModelConstraint>,
    // metadata
    pub meta: Option<MetaFields>,
    pub stats: Option<String>,

    // graph (points to the unique id for the test of this column)
    pub tested_by: Vec<String>,
}

pub type ParquetColumnRef = Arc<ParquetColumn>;

pub type ParquetColumnLookup = HashMap<String, Vec<ParquetColumnRef>>;

// ------------------------------------------------------------------------------------------------
// read/write
pub(crate) fn generate_columns_arrow_schema() -> Vec<Field> {
    let constraint_fields = Fields::from(generate_constraints_fields());
    let meta_fields = Fields::from(generate_meta_fields());
    // ColumnInfo fields
    vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("comment", DataType::Utf8, true),
        Field::new("tags", generate_list_type(DataType::Utf8), true),
        Field::new("data_type", DataType::Utf8, true),
        Field::new(
            "constraints",
            generate_list_type(DataType::Struct(constraint_fields)),
            true,
        ), // Store as JSON string
        Field::new("meta", DataType::Struct(meta_fields), true),
        Field::new("stats", DataType::Utf8, true),
        Field::new("tested_by", generate_list_type(DataType::Utf8), true),
    ]
}

fn serialize_columns(
    unique_id: &str,
    columns: &Vec<DbtColumnRef>,
    warehouse_catalog_table: Option<&CatalogTable>,
) -> Vec<ParquetColumnRef> {
    let mut res = vec![];
    let (warehouse_catalog_columns, stats_json) = if let Some(table) = warehouse_catalog_table {
        (
            Some(&table.columns),
            if table.stats.is_empty() {
                None
            } else {
                serde_json::to_string(&table.stats).ok()
            },
        )
    } else {
        (None, None)
    };
    for column in columns {
        let name = column.name.clone();
        let warehouse_catalog_columns = warehouse_catalog_columns.and_then(|cols| cols.get(&name));
        let data_type = column
            .data_type
            .clone()
            .or_else(|| warehouse_catalog_columns.map(|meta| meta.data_type.clone()));
        let description = column.description.clone();
        let comment = warehouse_catalog_columns.and_then(|meta| meta.comment.clone());
        let tags = if !column.tags.is_empty() {
            column.tags.clone()
        } else {
            vec![]
        };
        let meta: Option<MetaFields> = {
            let has_meta = !column.meta.is_empty();
            let has_policy_tags = column.policy_tags.as_ref().is_some_and(|ps| !ps.is_empty());

            if has_meta || has_policy_tags {
                let meta_blob = column
                    .meta
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::to_string(v).unwrap()))
                    .collect::<HashMap<String, String>>();

                // Only available for bigquery, e.g., ["projects/my-project/locations/us/taxonomies/123/policyTags/456"]
                // so we store it under Meta, not as a top-level field.

                let meta_list = if has_policy_tags {
                    if let Some(policy_tags) = &column.policy_tags {
                        HashMap::from_iter([("__policy_tags".to_string(), policy_tags.clone())])
                    } else {
                        HashMap::new()
                    }
                } else {
                    HashMap::new()
                };

                Some(MetaFields {
                    meta_blob: if meta_blob.is_empty() {
                        None
                    } else {
                        Some(meta_blob)
                    },
                    meta_list: if meta_list.is_empty() {
                        None
                    } else {
                        Some(meta_list)
                    },
                    ..Default::default()
                })
            } else {
                None
            }
        };

        let constraints = constraint_to_model_constraints(column.constraints.as_slice());

        res.push(Arc::new(ParquetColumn {
            unique_id: unique_id.to_string(),
            name,
            description,
            comment,
            tags,
            data_type,
            constraints,
            meta,
            stats: stats_json.clone(),
            tested_by: vec![], // Placeholder for tested_by, to be filled later
        }));
    }
    if let Some(cols) = warehouse_catalog_columns {
        for (name, meta) in cols {
            if columns.iter().any(|col| &col.name == name) {
                continue;
            }
            res.push(Arc::new(ParquetColumn {
                unique_id: unique_id.to_string(),
                name: name.clone(),
                description: None,
                comment: meta.comment.clone(),
                tags: vec![],
                data_type: Some(meta.data_type.clone()),
                constraints: Vec::new(),
                meta: None,
                stats: stats_json.clone(),
                tested_by: vec![],
            }));
        }
    }
    res
}

pub fn deserialize_columns(pcolumns: &[ParquetColumnRef]) -> Vec<DbtColumnRef> {
    let mut res = Vec::with_capacity(pcolumns.len());
    for pcolumn in pcolumns {
        let name = pcolumn.name.clone();
        let data_type = pcolumn.data_type.clone();
        let description = pcolumn.description.clone();
        let tags = if !pcolumn.tags.is_empty() {
            pcolumn.tags.clone()
        } else {
            vec![]
        };

        let policy_tags: Option<Vec<String>> = {
            if let Some(meta) = &pcolumn.meta {
                if let Some(meta_list) = &meta.meta_list {
                    meta_list.get("__policy_tags").cloned()
                } else {
                    None
                }
            } else {
                None
            }
        };

        let meta: IndexMap<String, Value> = {
            if let Some(meta) = &pcolumn.meta {
                if let Some(meta_blob) = &meta.meta_blob {
                    let mut meta = IndexMap::new();
                    for (k, v) in meta_blob {
                        meta.insert(k.clone(), serde_json::from_str(v).unwrap());
                    }
                    meta
                } else {
                    IndexMap::new()
                }
            } else {
                IndexMap::new()
            }
        };

        let constraints = model_constraints_to_constraints(pcolumn.constraints.as_slice());

        let column = Arc::new(DbtColumn {
            name: name.clone(),
            data_type,
            description,
            constraints,
            meta,
            tags,
            policy_tags,
            databricks_tags: None,
            column_mask: None,
            quote: None,
            deprecated_config: ColumnConfig::default(),
        });
        res.push(column);
    }
    res
}

fn constraint_to_model_constraints(constraints: &[Constraint]) -> Vec<ModelConstraint> {
    constraints
        .iter()
        .map(|constraint| ModelConstraint {
            type_: constraint.type_,
            expression: constraint.expression.clone(),
            name: constraint.name.clone(),
            to: constraint.to.clone(),
            to_columns: constraint.to_columns.clone(),
            columns: None,
            warn_unsupported: constraint.warn_unsupported,
            warn_unenforced: constraint.warn_unenforced,
        })
        .collect()
}

fn model_constraints_to_constraints(constraints: &[ModelConstraint]) -> Vec<Constraint> {
    constraints
        .iter()
        .map(|constraint| Constraint {
            type_: constraint.type_,
            expression: constraint.expression.clone(),
            name: constraint.name.clone(),
            to: constraint.to.clone(),
            to_columns: constraint.to_columns.clone(),
            warn_unsupported: constraint.warn_unsupported,
            warn_unenforced: constraint.warn_unenforced,
        })
        .collect()
}

fn transform_parquet_columns(pcolumns: Vec<ParquetColumnRef>) -> ParquetColumnLookup {
    let mut columns_lookup: ParquetColumnLookup = HashMap::new();
    for pcolumn in pcolumns {
        let columns = columns_lookup.entry(pcolumn.unique_id.clone()).or_default();
        columns.push(pcolumn);
    }
    columns_lookup
}

// ------------------------------------------------------------------------------------------------

/// Read columns metadata from a Parquet file at the given path.
pub fn read_columns(io: &IoArgs) -> FsResult<ParquetColumnLookup> {
    let pcolumns = read_parquet_file::<ParquetColumnRef>(io, "columns.parquet", COLUMNS_RD)?;
    Ok(transform_parquet_columns(pcolumns))
}

pub(crate) fn serialize_to_pcolumns(
    nodes: &Nodes,
    warehouse_catalog: Option<&DbtCatalog>,
) -> FsResult<Vec<ParquetColumnRef>> {
    let mut result = Vec::new();

    for node in nodes.models.values() {
        result.extend(serialize_columns(
            node.__common_attr__.unique_id.as_str(),
            &node.__base_attr__.columns,
            warehouse_catalog.and_then(|c| c.nodes.get(&node.__common_attr__.unique_id)),
        ));
    }
    for node in nodes.tests.values() {
        result.extend(serialize_columns(
            node.__common_attr__.unique_id.as_str(),
            &node.__base_attr__.columns,
            warehouse_catalog.and_then(|c| c.nodes.get(&node.__common_attr__.unique_id)),
        ));
    }
    for node in nodes.snapshots.values() {
        result.extend(serialize_columns(
            node.__common_attr__.unique_id.as_str(),
            &node.__base_attr__.columns,
            warehouse_catalog.and_then(|c| c.nodes.get(&node.__common_attr__.unique_id)),
        ));
    }
    for node in nodes.seeds.values() {
        result.extend(serialize_columns(
            node.__common_attr__.unique_id.as_str(),
            &node.__base_attr__.columns,
            warehouse_catalog.and_then(|c| c.nodes.get(&node.__common_attr__.unique_id)),
        ));
    }
    for node in nodes.sources.values() {
        result.extend(serialize_columns(
            node.__common_attr__.unique_id.as_str(),
            &node.__base_attr__.columns,
            warehouse_catalog.and_then(|c| c.nodes.get(&node.__common_attr__.unique_id)),
        ));
    }
    for node in nodes.unit_tests.values() {
        result.extend(serialize_columns(
            node.__common_attr__.unique_id.as_str(),
            &node.__base_attr__.columns,
            warehouse_catalog.and_then(|c| c.nodes.get(&node.__common_attr__.unique_id)),
        ));
    }

    Ok(result)
}
