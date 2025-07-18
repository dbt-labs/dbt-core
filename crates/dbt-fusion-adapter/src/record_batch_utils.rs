use crate::AdapterResult;
use crate::errors::{AdapterError, AdapterErrorKind};

use arrow::array::{
    Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
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

pub fn get_column_values<T>(record_batch: &RecordBatch, column_name: &str) -> AdapterResult<T>
where
    T: std::any::Any + Clone,
{
    Ok(record_batch
        .column_by_name(column_name)
        .ok_or_else(|| {
            let schema = record_batch.schema();
            let columns = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
            AdapterError::new(
                AdapterErrorKind::Internal,
                format!("expected column {column_name} not found, available are: {columns:?}"),
            )
        })?
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::{Arc, LazyLock};

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
        assert!(
            error
                .message()
                .contains("expected column nonexistent not found")
        );
        assert!(error.message().contains("available are"));
        assert!(error.message().contains("name"));
        assert!(error.message().contains("score"));
    }

    #[test]
    fn test_get_column_values_wrong_type() {
        // Try to get "name" column (which is StringArray) as Int32Array
        let result: AdapterResult<Int32Array> = get_column_values(&TEST_DATA, "name");

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), AdapterErrorKind::Internal);
        assert!(error.message().contains("expected column of type"));
        assert!(error.message().contains(
            "arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::Int32Type>"
        ));
    }
}
