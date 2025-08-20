//! Constraint configuration and processing for Databricks relations
//!
//! Handles extraction and diffing of constraint configurations from database metadata
//! and dbt node definitions. Supports non-null constraints and typed constraints.
//!
//! Reference: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py

use crate::databricks::constraints::TypedConstraint;
use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationMetadataKey,
    DatabricksRelationResults,
};

use crate::{
    AdapterResult,
    errors::{AdapterError, AdapterErrorKind},
};
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ConstraintsConfig {
    pub set_non_nulls: BTreeSet<String>,
    pub unset_non_nulls: BTreeSet<String>,
    pub set_constraints: BTreeSet<TypedConstraint>,
    pub unset_constraints: BTreeSet<TypedConstraint>,
}

impl ConstraintsConfig {
    pub fn new(
        set_non_nulls: BTreeSet<String>,
        unset_non_nulls: BTreeSet<String>,
        set_constraints: BTreeSet<TypedConstraint>,
        unset_constraints: BTreeSet<TypedConstraint>,
    ) -> Self {
        Self {
            set_non_nulls,
            unset_non_nulls,
            set_constraints,
            unset_constraints,
        }
    }

    /// Normalize expression for comparison by standardizing format
    ///
    /// This function standardizes SQL expressions for consistent comparison.
    /// Reference: https://raw.githubusercontent.com/databricks/dbt-databricks/refs/tags/v1.10.9/dbt/adapters/databricks/relation_configs/constraints.py
    fn normalize_expression(&self, expression: &str) -> String {
        if expression.is_empty() {
            return expression.to_string();
        }

        // TODO: Implement proper SQL formatting similar to sqlparse
        // The Python implementation uses sqlparse with specific formatting options:
        // - reindent=True
        // - keyword_case="lower"
        // - identifier_case="lower"
        //
        // For now, do basic normalization
        expression
            .trim()
            .to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Normalize a constraint for comparison by standardizing format and removing irrelevant fields
    ///
    /// This is necessary because Databricks:
    /// - Reformats expressions for check constraints
    /// - Does not persist the `columns` in check constraints
    ///
    /// Reference: https://raw.githubusercontent.com/databricks/dbt-databricks/refs/tags/v1.10.9/dbt/adapters/databricks/relation_configs/constraints.py
    fn normalize_constraint(&self, constraint: &TypedConstraint) -> TypedConstraint {
        match constraint {
            TypedConstraint::Check {
                name, expression, ..
            } => {
                // For check constraints, normalize expression and clear columns
                // since Databricks doesn't persist columns for check constraints
                TypedConstraint::Check {
                    name: name.clone(),
                    expression: self.normalize_expression(expression),
                    columns: None, // Clear columns as Databricks doesn't persist them
                }
            }
            _ => constraint.clone(), // Other constraint types are kept as-is
        }
    }

    /// Calculate the diff between this config and another config
    /// Returns None if no changes are needed, otherwise returns the changes to apply
    ///
    /// Uses normalization for proper constraint comparison, matching the Python implementation.
    ///
    /// Reference: https://raw.githubusercontent.com/databricks/dbt-databricks/refs/tags/v1.10.9/dbt/adapters/databricks/relation_configs/constraints.py
    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        // Normalize constraints for comparison (like Python implementation)
        let self_set_constraints_normalized: BTreeSet<_> = self
            .set_constraints
            .iter()
            .map(|c| self.normalize_constraint(c))
            .collect();

        let other_set_constraints_normalized: BTreeSet<_> = other
            .set_constraints
            .iter()
            .map(|c| self.normalize_constraint(c))
            .collect();

        // Find constraints that need to be unset (exist in other but not in self)
        let constraints_to_unset =
            &other_set_constraints_normalized - &self_set_constraints_normalized;

        // Find non-nulls that need to be unset (exist in other but not in self)
        let non_nulls_to_unset = &other.set_non_nulls - &self.set_non_nulls;

        // Find constraints that need to be set (exist in self but not in other)
        let set_constraints = &self_set_constraints_normalized - &other_set_constraints_normalized;

        // Find non-nulls that need to be set (exist in self but not in other)
        let set_non_nulls = &self.set_non_nulls - &other.set_non_nulls;

        if !set_constraints.is_empty()
            || !set_non_nulls.is_empty()
            || !constraints_to_unset.is_empty()
            || !non_nulls_to_unset.is_empty()
        {
            Some(ConstraintsConfig::new(
                set_non_nulls,
                non_nulls_to_unset,
                set_constraints,
                constraints_to_unset,
            ))
        } else {
            None
        }
    }
}

/// Zero-sized processor type for constraints - stateless processing unit
#[derive(Debug, Default)]
pub struct ConstraintsProcessor;

impl DatabricksComponentProcessorProperties for ConstraintsProcessor {
    fn name(&self) -> &'static str {
        "constraints"
    }
}

/// Processes constraint configurations from database results and dbt node attributes
///
/// Reference: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L50
impl DatabricksComponentProcessor for ConstraintsProcessor {
    #[allow(clippy::wrong_self_convention)]
    fn from_relation_results(
        &self,
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        // Get non-null constraint columns from results
        let non_null_columns = results
            .get(&DatabricksRelationMetadataKey::NonNullConstraints)
            .map(|table| {
                table
                    .rows()
                    .into_iter()
                    .filter_map(|row| {
                        // Try both "column_name" and "col_name" as different sources might use different names
                        // Try "column_name" first, but filter out undefined values
                        row.get_attr("column_name")
                            .ok()
                            .map(|v| v.to_string())
                            .filter(|s| !s.is_empty() && s != "undefined")
                            .or_else(|| {
                                // If that didn't work, try "col_name"
                                row.get_attr("col_name")
                                    .ok()
                                    .map(|v| v.to_string())
                                    .filter(|s| !s.is_empty() && s != "undefined")
                            })
                    })
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();

        // Process check constraints from table properties
        let check_constraints = self.process_check_constraints(
            results.get(&DatabricksRelationMetadataKey::ShowTblProperties),
        );

        // Process primary key constraints
        let pk_constraints = self.process_primary_key_constraints(
            results.get(&DatabricksRelationMetadataKey::PrimaryKeyConstraints),
        );

        // Process foreign key constraints
        let fk_constraints = self.process_foreign_key_constraints(
            results.get(&DatabricksRelationMetadataKey::ForeignKeyConstraints),
        );

        let mut all_constraints = BTreeSet::new();
        all_constraints.extend(check_constraints);
        all_constraints.extend(pk_constraints);
        all_constraints.extend(fk_constraints);

        Some(DatabricksComponentConfig::Constraints(
            ConstraintsConfig::new(
                non_null_columns,
                BTreeSet::new(),
                all_constraints,
                BTreeSet::new(),
            ),
        ))
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        let columns = &relation_config.base().columns;

        // Get model constraints from the node by downcasting to DbtModel
        let model_constraints = if let Some(model) = relation_config
            .as_any()
            .downcast_ref::<dbt_schemas::schemas::nodes::DbtModel>(
        ) {
            model.__model_attr__.constraints.as_slice()
        } else {
            &[]
        };

        // Use our parse_constraints function to handle both column and model constraints
        let (not_null_columns, other_constraints) =
            crate::databricks::constraints::parse_constraints(columns, model_constraints)
                .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, e))?;

        let constraints_set: BTreeSet<_> = other_constraints.into_iter().collect();

        Ok(Some(DatabricksComponentConfig::Constraints(
            ConstraintsConfig::new(
                not_null_columns,
                BTreeSet::new(),
                constraints_set,
                BTreeSet::new(),
            ),
        )))
    }
}

impl ConstraintsProcessor {
    /// Process check constraints from table properties
    /// Based on: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L53
    fn process_check_constraints(
        &self,
        table_properties: Option<&dbt_agate::AgateTable>,
    ) -> BTreeSet<TypedConstraint> {
        let mut check_constraints = BTreeSet::new();

        if let Some(table) = table_properties {
            for row in table.rows() {
                if let (Ok(property_name), Ok(property_value)) = (
                    row.get_attr("key")
                        .or_else(|_| row.get_attr("property_name")),
                    row.get_attr("value")
                        .or_else(|_| row.get_attr("property_value")),
                ) {
                    if let (Some(name_str), Some(value_str)) =
                        (property_name.as_str(), property_value.as_str())
                    {
                        if name_str.starts_with("delta.constraints.") {
                            let constraint_name =
                                name_str.strip_prefix("delta.constraints.").unwrap();
                            check_constraints.insert(TypedConstraint::Check {
                                name: Some(constraint_name.to_string()),
                                expression: value_str.to_string(),
                                columns: None,
                            });
                        }
                    }
                }
            }
        }

        check_constraints
    }

    /// Process primary key constraints
    /// Based on: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L69
    fn process_primary_key_constraints(
        &self,
        pk_table: Option<&dbt_agate::AgateTable>,
    ) -> BTreeSet<TypedConstraint> {
        let mut pk_constraints = BTreeSet::new();

        if let Some(table) = pk_table {
            let mut constraint_columns: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();

            for row in table.rows() {
                if let (Ok(constraint_name), Ok(column_name)) =
                    (row.get_attr("constraint_name"), row.get_attr("column_name"))
                {
                    if let (Some(name_str), Some(col_str)) =
                        (constraint_name.as_str(), column_name.as_str())
                    {
                        constraint_columns
                            .entry(name_str.to_string())
                            .or_default()
                            .push(col_str.to_string());
                    }
                }
            }

            for (constraint_name, columns) in constraint_columns {
                pk_constraints.insert(TypedConstraint::PrimaryKey {
                    name: Some(constraint_name),
                    columns,
                    expression: None,
                });
            }
        }

        pk_constraints
    }

    /// Process foreign key constraints
    /// Based on: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L87
    fn process_foreign_key_constraints(
        &self,
        fk_table: Option<&dbt_agate::AgateTable>,
    ) -> BTreeSet<TypedConstraint> {
        let mut fk_constraints = BTreeSet::new();

        if let Some(table) = fk_table {
            let mut fk_data: std::collections::HashMap<String, FkData> =
                std::collections::HashMap::new();

            for row in table.rows() {
                if let (
                    Ok(constraint_name),
                    Ok(column_name),
                    Ok(to_catalog),
                    Ok(to_schema),
                    Ok(to_table),
                    Ok(to_column),
                ) = (
                    row.get_attr("constraint_name"),
                    row.get_attr("column_name"),
                    row.get_attr("parent_catalog_name"),
                    row.get_attr("parent_schema_name"),
                    row.get_attr("parent_table_name"),
                    row.get_attr("parent_column_name"),
                ) {
                    if let (
                        Some(name_str),
                        Some(col_str),
                        Some(catalog_str),
                        Some(schema_str),
                        Some(table_str),
                        Some(to_col_str),
                    ) = (
                        constraint_name.as_str(),
                        column_name.as_str(),
                        to_catalog.as_str(),
                        to_schema.as_str(),
                        to_table.as_str(),
                        to_column.as_str(),
                    ) {
                        let entry = fk_data
                            .entry(name_str.to_string())
                            .or_insert_with(|| FkData {
                                columns: Vec::new(),
                                to: format!("`{catalog_str}`.`{schema_str}`.`{table_str}`"),
                                to_columns: Vec::new(),
                            });
                        entry.columns.push(col_str.to_string());
                        entry.to_columns.push(to_col_str.to_string());
                    }
                }
            }

            for (constraint_name, data) in fk_data {
                fk_constraints.insert(TypedConstraint::ForeignKey {
                    name: Some(constraint_name),
                    columns: data.columns,
                    to: Some(data.to),
                    to_columns: Some(data.to_columns),
                    expression: None,
                });
            }
        }

        fk_constraints
    }
}

#[derive(Debug)]
struct FkData {
    columns: Vec<String>,
    to: String,
    to_columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::databricks::constraints::TypedConstraint;
    use crate::databricks::relation_configs::base::{
        DatabricksRelationChangeSet, DatabricksRelationResultsBuilder,
    };
    use arrow::array::{RecordBatch, StringArray};
    use arrow::csv::ReaderBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::{
        common::*,
        common::{Constraint, ConstraintType},
        dbt_column::DbtColumn,
        nodes::DbtModel,
        nodes::*,
        properties::ModelConstraint,
        relations::relation_configs::{ComponentConfig, RelationChangeSet},
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::io;
    use std::sync::Arc;

    fn create_mock_non_null_constraints_table() -> AgateTable {
        let schema = Schema::new(vec![Field::new("col_name", DataType::Utf8, false)]);
        let col_name = StringArray::from(vec!["id", "name"]);
        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(col_name)]).unwrap();
        AgateTable::from_record_batch(Arc::new(record_batch))
    }

    fn create_mock_check_constraints_table() -> AgateTable {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        let csv = io::Cursor::new(
            r#"key,value
delta.constraints.valid_id,id > 0
delta.constraints.name_length,length(name) > 2
table.comment,This is a table comment"#,
        );
        let mut reader = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(csv)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        AgateTable::from_record_batch(Arc::new(batch))
    }

    fn create_mock_primary_key_constraints_table() -> AgateTable {
        let schema = Schema::new(vec![
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
        ]);
        let csv = io::Cursor::new(
            r#"constraint_name,column_name
pk_users,id
pk_composite,org_id
pk_composite,user_id"#,
        );
        let mut reader = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(csv)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        AgateTable::from_record_batch(Arc::new(batch))
    }

    fn create_mock_foreign_key_constraints_table() -> AgateTable {
        let schema = Schema::new(vec![
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("parent_catalog_name", DataType::Utf8, false),
            Field::new("parent_schema_name", DataType::Utf8, false),
            Field::new("parent_table_name", DataType::Utf8, false),
            Field::new("parent_column_name", DataType::Utf8, false),
        ]);
        let csv = io::Cursor::new(
            r#"constraint_name,column_name,parent_catalog_name,parent_schema_name,parent_table_name,parent_column_name
fk_user_org,org_id,main,default,organizations,id
fk_composite,parent_id,main,default,parents,id
fk_composite,parent_type,main,default,parents,type
"#,
        );
        let mut reader = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(csv)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        AgateTable::from_record_batch(Arc::new(batch))
    }

    fn create_mock_dbt_model_with_constraints(columns: BTreeMap<String, DbtColumn>) -> DbtModel {
        let base_attrs = NodeBaseAttributes {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            alias: "test_table".to_string(),
            relation_name: None,
            quoting: dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING,
            quoting_ignore_case: false,
            materialized: DbtMaterialization::Table,
            static_analysis: dbt_common::io_args::StaticAnalysisKind::On,
            enabled: true,
            extended_model: false,
            persist_docs: None,
            columns,
            refs: vec![],
            sources: vec![],
            metrics: vec![],
            depends_on: NodeDependsOn::default(),
        };

        DbtModel {
            __base_attr__: base_attrs,
            ..Default::default()
        }
    }

    #[test]
    fn test_processor_name() {
        let processor = ConstraintsProcessor;
        assert_eq!(processor.name(), "constraints");
    }

    #[test]
    fn test_constraints_config_new() {
        let non_nulls = BTreeSet::from(["id".to_string(), "name".to_string()]);
        let constraints = BTreeSet::from([TypedConstraint::Check {
            name: Some("positive_id".to_string()),
            expression: "id > 0".to_string(),
            columns: None,
        }]);

        let config = ConstraintsConfig::new(
            non_nulls.clone(),
            BTreeSet::new(),
            constraints.clone(),
            BTreeSet::new(),
        );

        assert_eq!(config.set_non_nulls, non_nulls);
        assert_eq!(config.set_constraints, constraints);
        assert!(config.unset_non_nulls.is_empty());
        assert!(config.unset_constraints.is_empty());
    }

    #[test]
    fn test_constraints_config_get_diff_no_changes() {
        let non_nulls = BTreeSet::from(["id".to_string()]);
        let constraints = BTreeSet::from([TypedConstraint::Check {
            name: Some("positive_id".to_string()),
            expression: "id > 0".to_string(),
            columns: None,
        }]);

        let config1 = ConstraintsConfig::new(
            non_nulls.clone(),
            BTreeSet::new(),
            constraints.clone(),
            BTreeSet::new(),
        );
        let config2 =
            ConstraintsConfig::new(non_nulls, BTreeSet::new(), constraints, BTreeSet::new());

        assert!(config1.get_diff(&config2).is_none());
    }

    #[test]
    fn test_constraints_config_get_diff_with_changes() {
        let old_non_nulls = BTreeSet::from(["id".to_string()]);
        let old_constraints = BTreeSet::from([TypedConstraint::Check {
            name: Some("old_check".to_string()),
            expression: "id > 0".to_string(),
            columns: None,
        }]);

        let new_non_nulls = BTreeSet::from(["id".to_string(), "name".to_string()]);
        let new_constraints = BTreeSet::from([
            TypedConstraint::Check {
                name: Some("positive_id".to_string()),
                expression: "id > 0".to_string(),
                columns: None,
            },
            TypedConstraint::PrimaryKey {
                name: Some("pk_users".to_string()),
                columns: vec!["id".to_string()],
                expression: None,
            },
        ]);

        let old_config = ConstraintsConfig::new(
            old_non_nulls,
            BTreeSet::new(),
            old_constraints,
            BTreeSet::new(),
        );
        let new_config = ConstraintsConfig::new(
            new_non_nulls,
            BTreeSet::new(),
            new_constraints,
            BTreeSet::new(),
        );

        let diff = new_config.get_diff(&old_config);
        assert!(diff.is_some());

        let diff_config = diff.unwrap();
        assert!(diff_config.set_non_nulls.contains("name"));
        assert_eq!(diff_config.set_constraints.len(), 2);
        assert_eq!(diff_config.unset_constraints.len(), 1);
    }

    #[test]
    fn test_from_relation_results_with_non_null_constraints() {
        let processor = ConstraintsProcessor;
        let non_null_table = create_mock_non_null_constraints_table();

        let results = DatabricksRelationResultsBuilder::new()
            .with_non_null_constraints(non_null_table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::Constraints(config)) = component {
            assert_eq!(config.set_non_nulls.len(), 2);
            assert!(config.set_non_nulls.contains("id"));
            assert!(config.set_non_nulls.contains("name"));
            assert!(config.set_constraints.is_empty());
        } else {
            panic!("Expected Constraints config");
        }
    }

    #[test]
    fn test_from_relation_results_with_check_constraints() {
        let processor = ConstraintsProcessor;
        let check_table = create_mock_check_constraints_table();

        let results = DatabricksRelationResultsBuilder::new()
            .with_show_tblproperties(check_table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::Constraints(config)) = component {
            assert_eq!(config.set_constraints.len(), 2);
            assert!(config.set_non_nulls.is_empty());

            let constraints: Vec<_> = config.set_constraints.iter().collect();
            assert!(constraints.iter().any(|c| match c {
                TypedConstraint::Check {
                    name, expression, ..
                } => name.as_deref() == Some("valid_id") && expression == "id > 0",
                _ => false,
            }));
            assert!(constraints.iter().any(|c| match c {
                TypedConstraint::Check {
                    name, expression, ..
                } => name.as_deref() == Some("name_length") && expression == "length(name) > 2",
                _ => false,
            }));
        } else {
            panic!("Expected Constraints config");
        }
    }

    #[test]
    fn test_from_relation_results_with_primary_key_constraints() {
        let processor = ConstraintsProcessor;
        let pk_table = create_mock_primary_key_constraints_table();

        let results = DatabricksRelationResultsBuilder::new()
            .with_primary_key_constraints(pk_table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::Constraints(config)) = component {
            assert_eq!(config.set_constraints.len(), 2);

            let constraints: Vec<_> = config.set_constraints.iter().collect();
            assert!(constraints.iter().any(|c| match c {
                TypedConstraint::PrimaryKey { name, columns, .. } =>
                    name.as_deref() == Some("pk_users") && columns == &vec!["id".to_string()],
                _ => false,
            }));
            assert!(constraints.iter().any(|c| match c {
                TypedConstraint::PrimaryKey { name, columns, .. } =>
                    name.as_deref() == Some("pk_composite") && columns.len() == 2,
                _ => false,
            }));
        } else {
            panic!("Expected Constraints config");
        }
    }

    #[test]
    fn test_from_relation_results_with_foreign_key_constraints() {
        let processor = ConstraintsProcessor;
        let fk_table = create_mock_foreign_key_constraints_table();

        let results = DatabricksRelationResultsBuilder::new()
            .with_foreign_key_constraints(fk_table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::Constraints(config)) = component {
            assert_eq!(config.set_constraints.len(), 2);

            let constraints: Vec<_> = config.set_constraints.iter().collect();
            assert!(constraints.iter().any(|c| match c {
                TypedConstraint::ForeignKey { name, to, .. } =>
                    name.as_deref() == Some("fk_user_org")
                        && to.as_deref() == Some("`main`.`default`.`organizations`"),
                _ => false,
            }));
            assert!(constraints.iter().any(|c| match c {
                TypedConstraint::ForeignKey {
                    name,
                    columns,
                    to_columns,
                    ..
                } =>
                    name.as_deref() == Some("fk_composite")
                        && columns.len() == 2
                        && to_columns.as_ref().map(|tc| tc.len()) == Some(2),
                _ => false,
            }));
        } else {
            panic!("Expected Constraints config");
        }
    }

    #[test]
    fn test_from_relation_results_all_constraint_types() {
        let processor = ConstraintsProcessor;
        let non_null_table = create_mock_non_null_constraints_table();
        let check_table = create_mock_check_constraints_table();
        let pk_table = create_mock_primary_key_constraints_table();
        let fk_table = create_mock_foreign_key_constraints_table();

        let results = DatabricksRelationResultsBuilder::new()
            .with_non_null_constraints(non_null_table)
            .with_show_tblproperties(check_table)
            .with_primary_key_constraints(pk_table)
            .with_foreign_key_constraints(fk_table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::Constraints(config)) = component {
            assert_eq!(config.set_non_nulls.len(), 2);
            assert_eq!(config.set_constraints.len(), 6); // 2 check + 2 pk + 2 fk
        } else {
            panic!("Expected Constraints config");
        }
    }

    #[test]
    fn test_from_relation_config_with_column_constraints() {
        let processor = ConstraintsProcessor;

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            DbtColumn {
                name: "id".to_string(),
                constraints: vec![Constraint {
                    type_: ConstraintType::NotNull,
                    name: None,
                    expression: None,
                    to: None,
                    to_columns: None,
                    warn_unsupported: None,
                    warn_unenforced: None,
                }],
                ..Default::default()
            },
        );
        columns.insert(
            "name".to_string(),
            DbtColumn {
                name: "name".to_string(),
                constraints: vec![Constraint {
                    type_: ConstraintType::NotNull,
                    name: None,
                    expression: None,
                    to: None,
                    to_columns: None,
                    warn_unsupported: None,
                    warn_unenforced: None,
                }],
                ..Default::default()
            },
        );

        let mock_node = create_mock_dbt_model_with_constraints(columns);
        let result = processor.from_relation_config(&mock_node);

        assert!(result.is_ok());
        let component = result.unwrap();
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::Constraints(config)) = component {
            assert_eq!(config.set_non_nulls.len(), 2);
            assert!(config.set_non_nulls.contains("id"));
            assert!(config.set_non_nulls.contains("name"));
            // Only not-null constraints from columns (model constraints would also be processed here)
            assert!(config.set_constraints.is_empty());
        } else {
            panic!("Expected Constraints config");
        }
    }

    #[test]
    fn test_from_relation_config_no_constraints() {
        let processor = ConstraintsProcessor;

        let columns = BTreeMap::new();
        let mock_node = create_mock_dbt_model_with_constraints(columns);
        let result = processor.from_relation_config(&mock_node);

        assert!(result.is_ok());
        let component = result.unwrap();
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::Constraints(config)) = component {
            assert!(config.set_non_nulls.is_empty());
            assert!(config.set_constraints.is_empty());
        } else {
            panic!("Expected Constraints config");
        }
    }

    #[test]
    fn test_parse_constraints_function() {
        use crate::databricks::constraints::parse_constraints;
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            DbtColumn {
                name: "id".to_string(),
                constraints: vec![Constraint {
                    type_: ConstraintType::NotNull,
                    name: None,
                    expression: None,
                    to: None,
                    to_columns: None,
                    warn_unsupported: None,
                    warn_unenforced: None,
                }],
                ..Default::default()
            },
        );

        let model_constraints = vec![
            ModelConstraint {
                type_: ConstraintType::Check,
                name: Some("positive_id".to_string()),
                expression: Some("id > 0".to_string()),
                to: None,
                to_columns: None,
                columns: None,
                warn_unsupported: None,
                warn_unenforced: None,
            },
            ModelConstraint {
                type_: ConstraintType::PrimaryKey,
                name: Some("pk_users".to_string()),
                expression: None,
                to: None,
                to_columns: None,
                columns: Some(vec!["id".to_string()]),
                warn_unsupported: None,
                warn_unenforced: None,
            },
        ];

        let (non_nulls, other_constraints) =
            parse_constraints(&columns, &model_constraints).unwrap();

        assert_eq!(non_nulls.len(), 1);
        assert!(non_nulls.contains("id"));

        assert_eq!(other_constraints.len(), 2);
        assert!(other_constraints.iter().any(|c| match c {
            TypedConstraint::Check {
                name, expression, ..
            } => name.as_deref() == Some("positive_id") && expression == "id > 0",
            _ => false,
        }));
        assert!(other_constraints.iter().any(|c| match c {
            TypedConstraint::PrimaryKey { name, columns, .. } =>
                name.as_deref() == Some("pk_users") && columns == &vec!["id".to_string()],
            _ => false,
        }));
    }

    #[test]
    fn test_changeset_integration() {
        let old_config = DatabricksComponentConfig::Constraints(ConstraintsConfig::new(
            BTreeSet::from(["id".to_string()]),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
        ));

        let new_config = DatabricksComponentConfig::Constraints(ConstraintsConfig::new(
            BTreeSet::from(["id".to_string(), "name".to_string()]),
            BTreeSet::new(),
            BTreeSet::from([TypedConstraint::Check {
                name: Some("positive_id".to_string()),
                expression: "id > 0".to_string(),
                columns: None,
            }]),
            BTreeSet::new(),
        ));

        let diff = new_config.get_diff(&old_config);
        assert!(diff.is_some());

        let mut changes = BTreeMap::new();
        changes.insert("constraints".to_string(), diff.unwrap());

        let changeset = DatabricksRelationChangeSet::new(changes, false);

        assert!(changeset.has_changes());
        assert!(!changeset.requires_full_refresh());
        assert!(changeset.get_change("constraints").is_some());
    }

    #[test]
    fn test_constraint_normalization_and_diff() {
        use crate::databricks::constraints::TypedConstraint;
        use std::collections::BTreeSet;

        // Test that normalization works correctly during diff calculation
        // Focus on the main Databricks-specific normalization: clearing columns for check constraints
        let check_constraint_1 = TypedConstraint::Check {
            name: Some("test_check".to_string()),
            expression: "value > 0".to_string(),
            columns: Some(vec!["value".to_string()]), // With columns (from dbt definition)
        };

        let check_constraint_2 = TypedConstraint::Check {
            name: Some("test_check".to_string()),
            expression: "value > 0".to_string(), // Same expression
            columns: None,                       // No columns (like Databricks stores it)
        };

        let config1 = ConstraintsConfig::new(
            BTreeSet::new(),
            BTreeSet::new(),
            [check_constraint_1].into_iter().collect(),
            BTreeSet::new(),
        );

        let config2 = ConstraintsConfig::new(
            BTreeSet::new(),
            BTreeSet::new(),
            [check_constraint_2].into_iter().collect(),
            BTreeSet::new(),
        );

        // The diff should be None because after normalization they should be considered equal
        // (The main difference - columns being cleared - should be normalized away)
        let diff = config1.get_diff(&config2);
        assert!(
            diff.is_none(),
            "Normalized constraints should be considered equal"
        );

        // Test expression normalization with safe cases (no quotes/strings)
        let normalized_expr1 = config1.normalize_expression("value > 0");
        let normalized_expr2 = config1.normalize_expression("  value > 0  "); // Just whitespace
        assert_eq!(
            normalized_expr1, normalized_expr2,
            "Whitespace should be normalized"
        );

        // Test that quoted identifiers are preserved (not broken)
        let quoted_expr = config1.normalize_expression(r#""user name" IS NOT NULL"#);
        assert!(
            quoted_expr.contains(r#""user name""#),
            "Quoted identifiers should be preserved"
        );
    }
}
