use arrow::array::{
    Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
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

pub fn get_column_values<T>(record_batch: &RecordBatch, column_name: &str) -> T
where
    T: std::any::Any + Clone,
{
    let res = record_batch
        .column_by_name(column_name)
        .unwrap_or_else(|| panic!("expected column {column_name} not found"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("expected column of type: {}", std::any::type_name::<T>()));
    res.to_owned()
}
