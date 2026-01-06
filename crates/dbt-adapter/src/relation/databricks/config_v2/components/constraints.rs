//! Constraint configuration and processing for Databricks relations
//!
//! Handles extraction and diffing of constraint configurations from database metadata
//! and dbt node definitions. Supports non-null constraints and typed constraints.
//!
//! Reference: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py

use crate::relation::config_v2::{ComponentConfig, ComponentConfigLoader};
use crate::relation::databricks::config_v2::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use crate::relation::databricks::typed_constraint::TypedConstraint;

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use indexmap::{IndexMap, IndexSet};
use minijinja::Value;
use serde::Serialize;

pub(crate) const TYPE_NAME: &str = "constraints";

#[derive(Debug, Default, Clone, Serialize)]
pub struct Constraints {
    pub set_non_nulls: IndexSet<String>,
    pub unset_non_nulls: IndexSet<String>,
    pub set_constraints: IndexSet<TypedConstraint>,
    pub unset_constraints: IndexSet<TypedConstraint>,
}

impl Constraints {
    fn new(
        set_non_nulls: IndexSet<String>,
        unset_non_nulls: IndexSet<String>,
        set_constraints: IndexSet<TypedConstraint>,
        unset_constraints: IndexSet<TypedConstraint>,
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
    fn normalize_expression(expression: &str) -> String {
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
    fn normalize_constraint(constraint: &TypedConstraint) -> TypedConstraint {
        match constraint {
            TypedConstraint::Check {
                name, expression, ..
            } => {
                // For check constraints, normalize expression and clear columns
                // since Databricks doesn't persist columns for check constraints
                TypedConstraint::Check {
                    name: name.clone(),
                    expression: Self::normalize_expression(expression),
                    columns: None, // Clear columns as Databricks doesn't persist them
                }
            }
            _ => constraint.clone(), // Other constraint types are kept as-is
        }
    }

    /// Process check constraints from table properties
    /// Based on: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L53
    fn process_check_constraints(
        table_properties: Option<&dbt_agate::AgateTable>,
    ) -> IndexSet<TypedConstraint> {
        let mut check_constraints = IndexSet::new();
        if let Some(table) = table_properties {
            for row in table.rows() {
                if let (Ok(property_name), Ok(property_value)) = (
                    row.get_attr("key")
                        .or_else(|_| row.get_attr("property_name")),
                    row.get_attr("value")
                        .or_else(|_| row.get_attr("property_value")),
                ) && let (Some(name_str), Some(value_str)) =
                    (property_name.as_str(), property_value.as_str())
                    && name_str.starts_with("delta.constraints.")
                {
                    let constraint_name = name_str.strip_prefix("delta.constraints.").unwrap();
                    check_constraints.insert(TypedConstraint::Check {
                        name: Some(constraint_name.to_string()),
                        expression: value_str.to_string(),
                        columns: None,
                    });
                }
            }
        }

        check_constraints
    }

    /// Process primary key constraints
    /// Based on: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L69
    fn process_primary_key_constraints(
        pk_table: Option<&dbt_agate::AgateTable>,
    ) -> IndexSet<TypedConstraint> {
        let mut pk_constraints = IndexSet::new();
        if let Some(table) = pk_table {
            let mut constraint_columns: IndexMap<String, Vec<String>> = IndexMap::new();

            for row in table.rows() {
                if let (Ok(constraint_name), Ok(column_name)) =
                    (row.get_attr("constraint_name"), row.get_attr("column_name"))
                    && let (Some(name_str), Some(col_str)) =
                        (constraint_name.as_str(), column_name.as_str())
                {
                    constraint_columns
                        .entry(name_str.to_string())
                        .or_default()
                        .push(col_str.to_string());
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
        fk_table: Option<&dbt_agate::AgateTable>,
    ) -> IndexSet<TypedConstraint> {
        let mut fk_constraints = IndexSet::new();
        if let Some(table) = fk_table {
            let mut fk_data: IndexMap<String, FkData> = IndexMap::new();

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
                ) && let (
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

    fn from_remote_state(results: &DatabricksRelationMetadata) -> Self {
        // Get non-null constraint columns from results
        let mut non_null_columns = results
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
                    .collect::<IndexSet<_>>()
            })
            .unwrap_or_default();

        // The engine might return them in random order so sorting to always be consistent
        non_null_columns.sort();

        // Process check constraints from table properties
        let check_constraints = Self::process_check_constraints(
            results.get(&DatabricksRelationMetadataKey::ShowTblProperties),
        );

        // Process primary key constraints
        let pk_constraints = Self::process_primary_key_constraints(
            results.get(&DatabricksRelationMetadataKey::PrimaryKeyConstraints),
        );

        // Process foreign key constraints
        let fk_constraints = Self::process_foreign_key_constraints(
            results.get(&DatabricksRelationMetadataKey::ForeignKeyConstraints),
        );

        let mut all_constraints = IndexSet::new();
        all_constraints.extend(check_constraints);
        all_constraints.extend(pk_constraints);
        all_constraints.extend(fk_constraints);

        // The engine might return them in random order so sorting to always be consistent
        all_constraints.sort();

        Self::new(
            non_null_columns,
            IndexSet::new(),
            all_constraints,
            IndexSet::new(),
        )
    }

    fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> Self {
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
        let Ok((not_null_columns, other_constraints)) =
            crate::relation::databricks::typed_constraint::parse_constraints(
                columns,
                model_constraints,
            )
        else {
            return Self::default();
        };

        let constraints_set: IndexSet<_> = other_constraints.into_iter().collect();

        // FIXME(serramatutu): make `parse_constraints` return `IndexSet` directly once old code
        // that relies on `BTreeSet` is deleted
        Self::new(
            IndexSet::from_iter(not_null_columns),
            IndexSet::new(),
            IndexSet::from_iter(constraints_set),
            IndexSet::new(),
        )
    }
}

pub(crate) struct ConstraintsLoader;

impl ConstraintsLoader {
    pub fn new(
        set_non_nulls: IndexSet<String>,
        unset_non_nulls: IndexSet<String>,
        set_constraints: IndexSet<TypedConstraint>,
        unset_constraints: IndexSet<TypedConstraint>,
    ) -> Box<dyn ComponentConfig> {
        Box::new(Constraints::new(
            set_non_nulls,
            unset_non_nulls,
            set_constraints,
            unset_constraints,
        ))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for ConstraintsLoader {
    fn type_name(&self) -> &'static str {
        TYPE_NAME
    }

    fn from_remote_state(
        &self,
        remote_state: &DatabricksRelationMetadata,
    ) -> Box<dyn ComponentConfig> {
        Box::new(Constraints::from_remote_state(remote_state))
    }

    fn from_local_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> Box<dyn ComponentConfig> {
        Box::new(Constraints::from_local_config(relation_config))
    }
}

impl ComponentConfig for Constraints {
    fn type_name(&self) -> &'static str {
        TYPE_NAME
    }

    /// Uses normalization for proper constraint comparison, matching the Python implementation.
    ///
    /// Reference: https://raw.githubusercontent.com/databricks/dbt-databricks/refs/tags/v1.10.9/dbt/adapters/databricks/relation_configs/constraints.py
    fn diff_from(
        &self,
        current_state: Option<&dyn ComponentConfig>,
    ) -> Option<Box<dyn ComponentConfig>> {
        // If the config was just introduced, we want to apply the entirety of `self`
        let Some(current_state) = current_state else {
            return Some(Box::new(self.clone()));
        };

        // The config is not if type `Constraints`, so we can't diff
        let current_state = current_state.as_any().downcast_ref::<Self>()?;

        // Normalize constraints for comparison (like Python implementation)
        let next_set_constraints_normalized: IndexSet<_> = self
            .set_constraints
            .iter()
            .map(Self::normalize_constraint)
            .collect();

        let prev_set_constraints_normalized: IndexSet<_> = current_state
            .set_constraints
            .iter()
            .map(Self::normalize_constraint)
            .collect();

        // Find constraints that need to be unset (exist in other but not in self)
        let constraints_to_unset =
            &prev_set_constraints_normalized - &next_set_constraints_normalized;

        // Find non-nulls that need to be unset (exist in other but not in self)
        let non_nulls_to_unset = &current_state.set_non_nulls - &self.set_non_nulls;

        // Find constraints that need to be set (exist in self but not in other)
        let set_constraints = &next_set_constraints_normalized - &prev_set_constraints_normalized;

        // Find non-nulls that need to be set (exist in self but not in other)
        let set_non_nulls = &self.set_non_nulls - &current_state.set_non_nulls;

        if !set_constraints.is_empty()
            || !set_non_nulls.is_empty()
            || !constraints_to_unset.is_empty()
            || !non_nulls_to_unset.is_empty()
        {
            Some(Box::new(Self::new(
                set_non_nulls,
                non_nulls_to_unset,
                set_constraints,
                constraints_to_unset,
            )))
        } else {
            None
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_jinja(&self) -> Value {
        Value::from_serialize(self)
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
    use crate::relation::config_v2::ComponentConfig;
    use crate::relation::databricks::config_v2::test_helpers;
    use crate::relation::databricks::typed_constraint::TypedConstraint;
    use arrow::array::{RecordBatch, StringArray};
    use arrow::csv::ReaderBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::{
        common::{Constraint, ConstraintType},
        nodes::DbtModel,
    };
    use dbt_test_primitives::assert_contains;
    use indexmap::{IndexMap, IndexSet};
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

    fn create_mock_dbt_model_with_constraints(
        constraints: IndexMap<&str, Vec<Constraint>>,
    ) -> DbtModel {
        let cfg = test_helpers::TestModelConfig {
            columns: constraints
                .into_iter()
                .map(|(name, constraints)| test_helpers::TestModelColumn {
                    name: name.to_string(),
                    constraints,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        test_helpers::create_mock_dbt_model(cfg)
    }

    #[test]
    fn test_constraints_config_diff_no_changes() {
        let non_nulls = IndexSet::from(["id".to_string()]);
        let constraints = IndexSet::from([TypedConstraint::Check {
            name: Some("positive_id".to_string()),
            expression: "id > 0".to_string(),
            columns: None,
        }]);
        let a = Constraints::new(non_nulls, IndexSet::new(), constraints, IndexSet::new());
        let diff = Constraints::diff_from(&a, Some(&a));

        assert!(diff.is_none());
    }

    #[test]
    fn test_constraints_config_diff_with_changes() {
        let old_non_nulls = IndexSet::from(["id".to_string()]);
        let old_constraints = IndexSet::from([TypedConstraint::Check {
            name: Some("old_check".to_string()),
            expression: "id > 0".to_string(),
            columns: None,
        }]);

        let new_non_nulls = IndexSet::from(["id".to_string(), "name".to_string()]);
        let new_constraints = IndexSet::from([
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

        let old_config = Constraints::new(
            old_non_nulls,
            IndexSet::new(),
            old_constraints,
            IndexSet::new(),
        );
        let new_config = Constraints::new(
            new_non_nulls,
            IndexSet::new(),
            new_constraints,
            IndexSet::new(),
        );

        let diff = Constraints::diff_from(&new_config, Some(&old_config));
        assert!(diff.is_some());

        let diff_config = diff.unwrap();
        let diff_config = diff_config.as_any().downcast_ref::<Constraints>().unwrap();

        assert_contains!(diff_config.set_non_nulls, "name");
        assert_eq!(diff_config.set_constraints.len(), 2);
        assert_eq!(diff_config.unset_constraints.len(), 1);
    }

    #[test]
    fn test_from_remote_state_with_non_null_constraints() {
        let non_null_table = create_mock_non_null_constraints_table();
        let results = IndexMap::from([(
            DatabricksRelationMetadataKey::NonNullConstraints,
            non_null_table,
        )]);
        let config = Constraints::from_remote_state(&results);

        assert_eq!(config.set_non_nulls.len(), 2);
        assert_contains!(config.set_non_nulls, "id");
        assert_contains!(config.set_non_nulls, "name");
        assert!(config.set_constraints.is_empty());
    }

    #[test]
    fn test_from_remote_state_with_check_constraints() {
        let check_table = create_mock_check_constraints_table();

        let results = IndexMap::from([(
            DatabricksRelationMetadataKey::ShowTblProperties,
            check_table,
        )]);
        let config = Constraints::from_remote_state(&results);

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
    }

    #[test]
    fn test_from_remote_state_with_primary_key_constraints() {
        let pk_table = create_mock_primary_key_constraints_table();

        let results = IndexMap::from([(
            DatabricksRelationMetadataKey::PrimaryKeyConstraints,
            pk_table,
        )]);
        let config = Constraints::from_remote_state(&results);

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
    }

    #[test]
    fn test_from_remote_state_with_foreign_key_constraints() {
        let fk_table = create_mock_foreign_key_constraints_table();

        let results = IndexMap::from([(
            DatabricksRelationMetadataKey::ForeignKeyConstraints,
            fk_table,
        )]);
        let config = Constraints::from_remote_state(&results);

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
    }

    #[test]
    fn test_from_remote_state_all_constraint_types() {
        let non_null_table = create_mock_non_null_constraints_table();
        let check_table = create_mock_check_constraints_table();
        let pk_table = create_mock_primary_key_constraints_table();
        let fk_table = create_mock_foreign_key_constraints_table();

        let results = IndexMap::from([
            (
                DatabricksRelationMetadataKey::NonNullConstraints,
                non_null_table,
            ),
            (
                DatabricksRelationMetadataKey::ShowTblProperties,
                check_table,
            ),
            (
                DatabricksRelationMetadataKey::PrimaryKeyConstraints,
                pk_table,
            ),
            (
                DatabricksRelationMetadataKey::ForeignKeyConstraints,
                fk_table,
            ),
        ]);
        let config = Constraints::from_remote_state(&results);

        assert_eq!(config.set_non_nulls.len(), 2);
        assert_eq!(config.set_constraints.len(), 6); // 2 check + 2 pk + 2 fk
    }

    #[test]
    fn test_from_local_config_with_column_constraints() {
        let constraints = IndexMap::from_iter([
            (
                "id",
                Vec::from([Constraint {
                    type_: ConstraintType::NotNull,
                    ..Default::default()
                }]),
            ),
            (
                "name",
                Vec::from([Constraint {
                    type_: ConstraintType::NotNull,
                    ..Default::default()
                }]),
            ),
        ]);

        let mock_node = create_mock_dbt_model_with_constraints(constraints);
        let config = Constraints::from_local_config(&mock_node);

        assert_eq!(config.set_non_nulls.len(), 2);
        assert_contains!(config.set_non_nulls, "id");
        assert_contains!(config.set_non_nulls, "name");
        // Only not-null constraints from columns (model constraints would also be processed here)
        assert!(config.set_constraints.is_empty());
    }

    #[test]
    fn test_from_local_config_no_constraints() {
        let columns = IndexMap::new();
        let mock_node = create_mock_dbt_model_with_constraints(columns);
        let config = Constraints::from_local_config(&mock_node);

        assert!(config.set_non_nulls.is_empty());
        assert!(config.set_constraints.is_empty());
    }

    #[test]
    fn test_constraint_normalization_and_diff() {
        // Test that normalization works correctly during diff calculation
        // Focus on the main Databricks-specific normalization: clearing columns for check constraints
        let prev_constraint = TypedConstraint::Check {
            name: Some("test_check".to_string()),
            expression: "value > 0".to_string(),
            columns: Some(vec!["value".to_string()]), // With columns (from dbt definition)
        };
        let next_constraint = TypedConstraint::Check {
            name: Some("test_check".to_string()),
            expression: "value > 0".to_string(), // Same expression
            columns: None,                       // No columns (like Databricks stores it)
        };

        let prev = Constraints::new(
            IndexSet::new(),
            IndexSet::new(),
            [prev_constraint].into_iter().collect(),
            IndexSet::new(),
        );
        let next = Constraints::new(
            IndexSet::new(),
            IndexSet::new(),
            [next_constraint].into_iter().collect(),
            IndexSet::new(),
        );

        // The diff should be None because after normalization they should be considered equal
        // (The main difference - columns being cleared - should be normalized away)
        let diff = Constraints::diff_from(&next, Some(&prev));
        assert!(
            diff.is_none(),
            "Normalized constraints should be considered equal"
        );

        // Test expression normalization with safe cases (no quotes/strings)
        let normalized_expr1 = Constraints::normalize_expression("value > 0");
        let normalized_expr2 = Constraints::normalize_expression("  value > 0  "); // Just whitespace
        assert_eq!(
            normalized_expr1, normalized_expr2,
            "Whitespace should be normalized"
        );

        // Test that quoted identifiers are preserved (not broken)
        let quoted_expr = Constraints::normalize_expression(r#""user name" IS NOT NULL"#);
        assert!(
            quoted_expr.contains(r#""user name""#),
            "Quoted identifiers should be preserved"
        );
    }
}
