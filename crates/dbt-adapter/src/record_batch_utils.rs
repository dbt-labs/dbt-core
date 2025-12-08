use std::collections::HashMap;
use std::sync::Arc;

use crate::AdapterResult;
use crate::errors::{AdapterError, AdapterErrorKind};

use arrow::array::{
    Array, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

pub fn extract_first_value_as_i64(batch: &RecordBatch) -> Option<i64> {
    let column = batch.column(0);

    match batch.schema().field(0).data_type() {
        DataType::Int8 => column
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|arr| arr.value(0) as i64),
        DataType::Int16 => column
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|arr| arr.value(0) as i64),
        DataType::Int32 => column
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| arr.value(0) as i64),
        DataType::Int64 => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| arr.value(0)),
        DataType::UInt8 => column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|arr| arr.value(0) as i64),
        DataType::UInt16 => column
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|arr| arr.value(0) as i64),
        DataType::UInt32 => column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|arr| arr.value(0) as i64),
        DataType::UInt64 => column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|arr| arr.value(0) as i64),
        DataType::Decimal128(_, 0) => column
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|arr| arr.value(0) as i64),
        _ => None,
    }
}

pub fn column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> AdapterResult<&'a Arc<dyn Array>> {
    batch.column_by_name(name).ok_or_else(|| {
        let schema = batch.schema();
        let columns = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
        AdapterError::new(
            AdapterErrorKind::Internal,
            format!("expected column {name} not found, available are: {columns:?}"),
        )
    })
}

pub fn get_column_values<T>(record_batch: &RecordBatch, column_name: &str) -> AdapterResult<T>
where
    T: std::any::Any + Clone,
{
    Ok(column_by_name(record_batch, column_name)?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            let schema = record_batch.schema();
            let field = schema.fields().iter().find(|f| f.name() == column_name);
            AdapterError::new(
                AdapterErrorKind::Internal,
                format!(
                    "expected column of type: {} not found, available are: {field:?}",
                    std::any::type_name::<T>()
                ),
            )
        })?
        .to_owned())
}

/// Deduplicate column names in a RecordBatch by appending `_2`, `_3`, etc. to duplicate names.
///
/// This mirrors the behavior of dbt-core's `process_results` method in `SQLConnectionManager`
/// which renames duplicate columns to ensure each column has a unique name.
///
/// For example, columns `["A", "B", "A", "A"]` become `["A", "B", "A_2", "A_3"]`.
pub fn deduplicate_column_names(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let fields = schema.fields();

    // Track occurrences of each column name
    let mut name_counts: HashMap<&str, usize> = HashMap::new();
    let mut new_names: Vec<String> = Vec::with_capacity(fields.len());

    for field in fields.iter() {
        let name = field.name().as_str();
        let count = name_counts.entry(name).or_insert(0);
        *count += 1;

        if *count > 1 {
            new_names.push(format!("{}_{}", name, count));
        } else {
            new_names.push(name.to_string());
        }
    }

    // Check if any names changed - if not, return original batch to avoid unnecessary allocation
    let names_changed = new_names
        .iter()
        .zip(fields.iter())
        .any(|(new_name, field)| new_name != field.name());

    if !names_changed {
        return batch;
    }

    // Build new schema with deduplicated names
    let new_fields: Vec<_> = fields
        .iter()
        .zip(new_names.iter())
        .map(|(field, new_name)| Arc::new(field.as_ref().clone().with_name(new_name.clone())))
        .collect();

    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));

    // Create new RecordBatch with the new schema but same column data
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .expect("deduplicate_column_names: schema and columns should be compatible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use dbt_test_primitives::assert_contains;
    use std::sync::LazyLock;

    static TEST_DATA: LazyLock<RecordBatch> = LazyLock::new(|| {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]);

        let name_array = StringArray::from(vec!["FOO"]);
        let score_array = Float64Array::from(vec![42.0]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(name_array), Arc::new(score_array)],
        )
        .unwrap()
    });

    #[test]
    fn test_get_column_values_success() {
        let result: AdapterResult<StringArray> = get_column_values(&TEST_DATA, "name");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_column_values_column_not_found() {
        let result: AdapterResult<Int32Array> = get_column_values(&TEST_DATA, "nonexistent");

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), AdapterErrorKind::Internal);
        assert_contains!(error.message(), "expected column nonexistent not found");
        assert_contains!(error.message(), "available are");
        assert_contains!(error.message(), "name");
        assert_contains!(error.message(), "score");
    }

    #[test]
    fn test_get_column_values_wrong_type() {
        // Try to get "name" column (which is StringArray) as Int32Array
        let result: AdapterResult<Int32Array> = get_column_values(&TEST_DATA, "name");

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), AdapterErrorKind::Internal);
        assert_contains!(error.message(), "expected column of type");
        assert!(error.message().contains(
            "arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::Int32Type>"
        ));
    }

    #[test]
    fn test_deduplicate_column_names_no_duplicates() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let a = Int32Array::from(vec![1, 2, 3]);
        let b = Int32Array::from(vec![4, 5, 6]);
        let c = Int32Array::from(vec![7, 8, 9]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )
        .unwrap();

        let result = deduplicate_column_names(batch);
        let schema = result.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_deduplicate_column_names_with_duplicates() {
        // Test case from dbt-core: ["A", "B", "A", "A"] -> ["A", "B", "A_2", "A_3"]
        let schema = Schema::new(vec![
            Field::new("A", DataType::Int32, false),
            Field::new("B", DataType::Int32, false),
            Field::new("A", DataType::Int32, false),
            Field::new("A", DataType::Int32, false),
        ]);
        let a1 = Int32Array::from(vec![1, 2, 3]);
        let b = Int32Array::from(vec![4, 5, 6]);
        let a2 = Int32Array::from(vec![7, 8, 9]);
        let a3 = Int32Array::from(vec![10, 11, 12]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a1), Arc::new(b), Arc::new(a2), Arc::new(a3)],
        )
        .unwrap();

        let result = deduplicate_column_names(batch);
        let schema = result.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["A", "B", "A_2", "A_3"]);
    }

    #[test]
    fn test_deduplicate_column_names_multiple_duplicates() {
        // Test with multiple different duplicate column names
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("x", DataType::Int32, false),
        ]);
        let cols: Vec<_> = (0..5)
            .map(|_| Arc::new(Int32Array::from(vec![1])) as _)
            .collect();
        let batch = RecordBatch::try_new(Arc::new(schema), cols).unwrap();

        let result = deduplicate_column_names(batch);
        let schema = result.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["x", "y", "x_2", "y_2", "x_3"]);
    }
}
