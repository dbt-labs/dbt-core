use dbt_agate::AgateTable;
use minijinja::{Error, ErrorKind, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Represents a column definition with name, data type, and formatted information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub formatted: Option<String>,
}

impl From<ColumnDefinition> for Value {
    fn from(col: ColumnDefinition) -> Self {
        let mut map = HashMap::new();
        map.insert("name", Value::from(col.name));
        map.insert("data_type", Value::from(col.data_type));
        if let Some(formatted) = col.formatted {
            map.insert("formatted", Value::from(formatted));
        }
        Value::from(map)
    }
}

/// Gets mismatches between YAML and SQL column definitions as an AgateTable.
///
/// Returns a table with columns: column_name, definition_type, contract_type, mismatch_reason
pub fn get_contract_mismatches(
    yaml_columns: Value,
    sql_columns: Value,
) -> Result<&'static Arc<AgateTable>, Error> {
    // Convert Value to Vec<ColumnDefinition>
    let yaml_columns = convert_value_to_column_definitions(yaml_columns)?;
    let sql_columns = convert_value_to_column_definitions(sql_columns)?;

    let column_names = vec![
        "column_name".to_string(),
        "definition_type".to_string(),
        "contract_type".to_string(),
        "mismatch_reason".to_string(),
    ];

    let column_types = vec![
        "Text".to_string(),
        "Text".to_string(),
        "Text".to_string(),
        "Text".to_string(),
    ];

    let mut mismatches: Vec<HashMap<String, String>> = Vec::new();
    let mut sql_col_set = std::collections::HashSet::new();

    // Check each SQL column against YAML columns
    for sql_col in &sql_columns {
        sql_col_set.insert(&sql_col.name);

        // Find matching YAML column by name
        let mut found_match = false;
        for yaml_col in &yaml_columns {
            if sql_col.name == yaml_col.name {
                found_match = true;
                // Check if data types match
                if sql_col.data_type == yaml_col.data_type {
                    // Perfect match, don't include in mismatches
                    break;
                } else {
                    // Same name, different type
                    let mut row = HashMap::new();
                    row.insert("column_name".to_string(), sql_col.name.clone());
                    row.insert("definition_type".to_string(), sql_col.data_type.clone());
                    row.insert("contract_type".to_string(), yaml_col.data_type.clone());
                    row.insert(
                        "mismatch_reason".to_string(),
                        "data type mismatch".to_string(),
                    );
                    mismatches.push(row);
                    break;
                }
            }
        }

        // If no name match found, this column is missing in contract
        if !found_match {
            let mut row = HashMap::new();
            row.insert("column_name".to_string(), sql_col.name.clone());
            row.insert("definition_type".to_string(), sql_col.data_type.clone());
            row.insert("contract_type".to_string(), String::new());
            row.insert(
                "mismatch_reason".to_string(),
                "missing in contract".to_string(),
            );
            mismatches.push(row);
        }
    }

    // Check for YAML columns that don't have a match in SQL
    for yaml_col in &yaml_columns {
        if !sql_col_set.contains(&yaml_col.name) {
            let mut row = HashMap::new();
            row.insert("column_name".to_string(), yaml_col.name.clone());
            row.insert("definition_type".to_string(), String::new());
            row.insert("contract_type".to_string(), yaml_col.data_type.clone());
            row.insert(
                "mismatch_reason".to_string(),
                "missing in definition".to_string(),
            );
            mismatches.push(row);
        }
    }

    // Sort mismatches by column name
    mismatches.sort_by(|a, b| a["column_name"].cmp(&b["column_name"]));

    // Convert to AgateTable format
    let rows: Vec<Value> = mismatches
        .into_iter()
        .map(|row| {
            let row_values: Vec<Value> = column_names
                .iter()
                .map(|col_name| Value::from(row.get(col_name).unwrap_or(&String::new()).clone()))
                .collect();
            Value::from(row_values)
        })
        .collect();

    let table = Arc::new(AgateTable::from_rows(column_names, column_types, rows));
    Ok(Box::leak(Box::new(table)))
}

/// Helper function to convert Value to Vec<ColumnDefinition>
fn convert_value_to_column_definitions(value: Value) -> Result<Vec<ColumnDefinition>, Error> {
    if value.is_undefined() {
        return Ok(Vec::new());
    }

    let mut columns = Vec::new();

    match value.try_iter() {
        Ok(iter) => {
            for item in iter {
                let name_value = item.get_attr("name")?;
                let name = name_value
                    .as_str()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidOperation,
                            "Column 'name' must be a string",
                        )
                    })?
                    .to_string();

                let data_type_value = item.get_attr("data_type")?;
                let data_type = data_type_value
                    .as_str()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidOperation,
                            "Column 'data_type' must be a string",
                        )
                    })?
                    .to_string();

                let formatted = item
                    .get_attr("formatted")
                    .ok()
                    .and_then(|v| v.as_str().map(|s| s.to_string()));

                columns.push(ColumnDefinition {
                    name,
                    data_type,
                    formatted,
                });
            }
        }
        Err(_) => {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "Expected a list of column definitions",
            ));
        }
    }

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_agate::MappedSequence;

    #[test]
    fn test_contract_error_perfect_match() {
        let yaml_columns = vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                formatted: None,
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: "text".to_string(),
                formatted: None,
            },
        ];

        let sql_columns = vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                formatted: None,
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: "text".to_string(),
                formatted: None,
            },
        ];

        let yaml_values: Vec<Value> = yaml_columns.into_iter().map(Value::from).collect();
        let sql_values: Vec<Value> = sql_columns.into_iter().map(Value::from).collect();

        let mismatches =
            get_contract_mismatches(Value::from(yaml_values), Value::from(sql_values)).unwrap();

        assert_eq!(mismatches.num_rows(), 0);
    }

    #[test]
    fn test_contract_error_type_mismatch() {
        let yaml_columns = vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            formatted: None,
        }];

        let sql_columns = vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: "bigint".to_string(),
            formatted: None,
        }];

        let yaml_values: Vec<Value> = yaml_columns.into_iter().map(Value::from).collect();
        let sql_values: Vec<Value> = sql_columns.into_iter().map(Value::from).collect();

        let mismatches =
            get_contract_mismatches(Value::from(yaml_values), Value::from(sql_values)).unwrap();

        assert_eq!(mismatches.num_rows(), 1);

        // Check the mismatch details by accessing the first row directly
        let rows = mismatches.rows();
        let row_values = rows.values();
        let first_row = row_values.get(0).expect("at least one row");

        // The row should be a list of values in the order: column_name, definition_type, contract_type, mismatch_reason
        let row_list: Vec<_> = first_row.try_iter().unwrap().collect();
        assert_eq!(row_list[0].as_str().unwrap(), "id");
        assert_eq!(row_list[1].as_str().unwrap(), "bigint");
        assert_eq!(row_list[2].as_str().unwrap(), "integer");
        assert_eq!(row_list[3].as_str().unwrap(), "data type mismatch");
    }

    #[test]
    fn test_contract_error_missing_in_contract() {
        let yaml_columns = vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            formatted: None,
        }];

        let sql_columns = vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                formatted: None,
            },
            ColumnDefinition {
                name: "extra_col".to_string(),
                data_type: "text".to_string(),
                formatted: None,
            },
        ];

        let yaml_values: Vec<Value> = yaml_columns.into_iter().map(Value::from).collect();
        let sql_values: Vec<Value> = sql_columns.into_iter().map(Value::from).collect();

        let mismatches =
            get_contract_mismatches(Value::from(yaml_values), Value::from(sql_values)).unwrap();

        assert_eq!(mismatches.num_rows(), 1);

        // Check the mismatch details
        let rows = mismatches.rows();
        let row_values = rows.values();
        let first_row = row_values.get(0).expect("at least one row");

        let row_list: Vec<_> = first_row.try_iter().unwrap().collect();
        assert_eq!(row_list[0].as_str().unwrap(), "extra_col");
        assert_eq!(row_list[1].as_str().unwrap(), "text");
        assert_eq!(row_list[2].as_str().unwrap(), "");
        assert_eq!(row_list[3].as_str().unwrap(), "missing in contract");
    }

    #[test]
    fn test_contract_error_missing_in_definition() {
        let yaml_columns = vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                formatted: None,
            },
            ColumnDefinition {
                name: "missing_col".to_string(),
                data_type: "text".to_string(),
                formatted: None,
            },
        ];

        let sql_columns = vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            formatted: None,
        }];

        let yaml_values: Vec<Value> = yaml_columns.into_iter().map(Value::from).collect();
        let sql_values: Vec<Value> = sql_columns.into_iter().map(Value::from).collect();

        let mismatches =
            get_contract_mismatches(Value::from(yaml_values), Value::from(sql_values)).unwrap();

        assert_eq!(mismatches.num_rows(), 1);

        // Check the mismatch details
        let rows = mismatches.rows();
        let row_values = rows.values();
        let first_row = row_values.get(0).expect("at least one row");

        let row_list: Vec<_> = first_row.try_iter().unwrap().collect();
        assert_eq!(row_list[0].as_str().unwrap(), "missing_col");
        assert_eq!(row_list[1].as_str().unwrap(), "");
        assert_eq!(row_list[2].as_str().unwrap(), "text");
        assert_eq!(row_list[3].as_str().unwrap(), "missing in definition");
    }
}
