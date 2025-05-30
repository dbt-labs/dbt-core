use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use dbt_schemas::schemas::columns::postgres::PostgresColumn;

pub fn postgres_columns_from_batch(batch: RecordBatch) -> Vec<PostgresColumn> {
    let column = batch.column_by_name("column_name").unwrap();
    let name_string_array = column.as_any().downcast_ref::<StringArray>().unwrap();

    let dtype = batch.column_by_name("data_type").unwrap();
    let dtype_string_array = dtype.as_any().downcast_ref::<StringArray>().unwrap();

    let char_size = batch.column_by_name("character_maximum_length").unwrap();
    let char_size_string_array = char_size.as_any().downcast_ref::<Int32Array>().unwrap();

    (0..name_string_array.len())
        .map(|i| PostgresColumn {
            name: name_string_array.value(i).to_string(),
            dtype: dtype_string_array.value(i).to_string(),
            char_size: Some(char_size_string_array.value(i) as u32),
        })
        .collect()
}
