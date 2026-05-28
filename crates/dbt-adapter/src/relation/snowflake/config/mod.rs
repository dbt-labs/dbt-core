pub(crate) mod components;
pub(crate) mod relation_types;
pub(crate) mod test_helpers;

use arrow::record_batch::RecordBatch;
use arrow_array::{Array, BooleanArray, StringArray};
use std::convert::TryFrom;
use std::sync::Arc;

use dbt_agate::AgateTable;
use minijinja::Value;

use crate::record_batch::RecordBatchExt;

/// Deserialization target for macro snowflake__describe_dynamic_table
/// https://github.com/dbt-labs/dbt-adapters/blob/61221f455f5960daf80024febfae6d6fb4b46251/dbt-snowflake/src/dbt/include/snowflake/macros/relations/dynamic_table/describe.sql#L3
#[derive(Debug)]
pub struct DescribeDynamicTableResults {
    pub dynamic_table: Arc<RecordBatch>,
}

impl TryFrom<&Value> for DescribeDynamicTableResults {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let dynamic_table = value
            .get_item(&Value::from_safe_string("dynamic_table".into()))
            .map_err(|e| format!("Expected key `dynamic_table`: {e}"))?
            .downcast_object::<AgateTable>()
            .ok_or("Failed to convert dynamic_table to AgateTable")?
            .original_record_batch();

        Ok(Self { dynamic_table })
    }
}

// Helper function to get a bool value from a RecordBatch by column name.
// Returns None if the column is absent.
fn get_bool_by_name_from_record_batch(batch: &Arc<RecordBatch>, col_name: &str) -> Option<bool> {
    let col = batch.column_values::<BooleanArray>(col_name).ok()?;
    if col.len() != 1 {
        return None;
    }
    col.is_valid(0).then(|| col.value(0))
}

// Helper function to get a string value from a Record Batch by column name
fn get_string_by_name_from_record_batch(
    batch: &Arc<RecordBatch>,
    col_name: &str,
) -> Result<String, String> {
    if let Ok(column_values) = batch.column_values::<StringArray>(col_name) {
        if column_values.len() != 1 {
            return Err(format!(
                "Describe dynamic_table returned an unexpected number of values for {col_name}."
            ));
        }

        Ok(column_values.value(0).to_string())
    } else {
        Err(format!("Describe dynamic_table is missing {col_name}."))
    }
}
