use std::collections::HashMap;
use std::sync::Arc;

use dbt_common::FsResult;
use dbt_common::constants::*;
use dbt_common::io_args::IoArgs;
use dbt_metadata_parquet::utils::read_parquet_with_batch;

use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// ------------------------------------------------------------------------------------------------
// CAS: generate write, read
pub fn generate_cas_record_batch(cas: &HashMap<String, String>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("hash", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ]);

    // Builders for the columns
    let mut hash_builder = StringBuilder::new();
    let mut content_builder = StringBuilder::new();

    // Populate the builders
    for (hash, content) in cas {
        hash_builder.append_value(hash);
        content_builder.append_value(content);
    }

    // Create the RecordBatch
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(hash_builder.finish()),
            Arc::new(content_builder.finish()),
        ],
    )
    .expect("Failed to create RecordBatch")
}

pub fn parse_cas_batch(batch: &RecordBatch) -> FsResult<Vec<(String, String)>> {
    let mut cas = Vec::with_capacity(batch.num_rows());
    let hash_column = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("Expected StringArray for hash column");
    let content_column = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("Expected StringArray for content column");
    for i in 0..batch.num_rows() {
        let hash = hash_column.value(i).to_string();
        let content = content_column.value(i).to_string();
        cas.push((hash, content));
    }
    Ok(cas)
}

pub fn read_cas(io: &IoArgs) -> FsResult<HashMap<String, String>> {
    read_parquet_with_batch(io, "cas.parquet", CAS_RD, parse_cas_batch)
        .map(|vec| vec.into_iter().collect())
}
