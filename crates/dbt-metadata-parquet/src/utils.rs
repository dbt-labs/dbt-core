use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, FieldRef, Fields},
};
use dbt_common::{
    ErrorCode, FsError, FsResult,
    constants::DBT_METADATA_DIR_NAME,
    err,
    io_args::{BuildCacheMode, IoArgs},
    stdfs,
};
use dbt_common::{io_utils::StatusReporter, tracing::emit::emit_info_progress_message};
use dbt_telemetry::ProgressMessage;
use dbt_yaml::Value;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReader},
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_arrow::to_record_batch;
use serde_json;
use std::fs::File;
// ------------------------------------------------------------------------------------------------
// progress reporting helper

fn emit_progress_relative(
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
    action: &str,
    out_dir: &Path,
    file_path: &Path,
    description: &str,
) {
    let display_path = pathdiff::diff_paths(file_path, out_dir)
        .unwrap_or_else(|| file_path.to_path_buf())
        .display()
        .to_string();
    emit_info_progress_message(
        ProgressMessage::new_with_description(
            action.to_string(),
            display_path,
            description.to_string(),
        ),
        status_reporter,
    );
}

// ------------------------------------------------------------------------------------------------
// generate arrow schema types

pub fn generate_map_type(value_type: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", value_type, true),
            ])),
            false,
        )),
        false,
    )
}

pub fn generate_list_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item_type, true)))
}

pub fn generate_struct_type(fields: Fields) -> DataType {
    DataType::Struct(fields)
}

// ------------------------------------------------------------------------------------------------
// file handlding utilities

/// Resolve the effective file path for a Parquet file, given an optional override URL/path,
/// the output directory, and the default file name.
/// - If `override_url` is Some and absolute or S3, use as-is.
/// - If `override_url` is Some and relative, join with out_dir (and check for traversal).
/// - If None, use out_dir/DBT_METADATA_DIR_NAME/default_file_name.
pub fn resolve_parquet_path(
    out_dir: &Path,
    override_url: &Option<String>,
    default_file_name: &str,
) -> FsResult<PathBuf> {
    if let Some(url) = override_url
        && !url.is_empty()
    {
        let path = Path::new(url);
        if path.is_absolute() || url.starts_with("s3://") {
            // Use absolute or S3 path as-is
            return Ok(PathBuf::from(url));
        } else {
            // Join relative path with out_dir
            let joined = out_dir.join(path);
            // Optional: Prevent directory traversal
            let canonical_out_dir =
                stdfs::canonicalize(out_dir).unwrap_or_else(|_| out_dir.to_path_buf());
            let canonical_joined = stdfs::canonicalize(&joined).unwrap_or_else(|_| joined.clone());
            if !canonical_joined.starts_with(&canonical_out_dir) {
                return err!(
                    ErrorCode::IoError,
                    "Refusing to use path outside of target dir: {}",
                    canonical_joined.display()
                );
            }
            return Ok(joined);
        }
    }
    Ok(out_dir.join(DBT_METADATA_DIR_NAME).join(default_file_name))
}

// ------------------------------------------------------------------------------------------------
// convert YAML to JSON map
pub fn yaml_map_to_json_map(args: &BTreeMap<String, Value>) -> BTreeMap<String, serde_json::Value> {
    args.iter()
        .map(|(k, v)| (k.clone(), yaml_value_to_json_value(v)))
        .collect()
}

pub fn yaml_value_to_json_value(yaml: &Value) -> serde_json::Value {
    use serde_json::Value as JsonValue;
    match yaml {
        Value::Null(_) => JsonValue::Null,
        Value::Bool(b, _) => JsonValue::Bool(*b),
        Value::Number(n, _) => {
            if let Some(i) = n.as_i64() {
                JsonValue::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                JsonValue::Number(u.into())
            } else if let Some(f) = n.as_f64() {
                // serde_json::Number only supports finite f64
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            } else {
                JsonValue::Null
            }
        }
        Value::String(s, _) => JsonValue::String(s.clone()),
        Value::Sequence(seq, _) => {
            JsonValue::Array(seq.iter().map(yaml_value_to_json_value).collect())
        }
        Value::Mapping(map, _) => {
            let obj = map
                .iter()
                .filter_map(|(k, v)| {
                    // Only use string keys for JSON objects
                    if let Value::String(key, _) = k {
                        Some((key.clone(), yaml_value_to_json_value(v)))
                    } else {
                        None
                    }
                })
                .collect();
            JsonValue::Object(obj)
        }
        _ => JsonValue::Null,
    }
}
// ------------------------------------------------------------------------------------------------
// type based read/write parquet files

pub fn read_parquet_file<T: DeserializeOwned>(
    io: &IoArgs,
    default_filename: &str,
    progress_code: &str,
) -> FsResult<Vec<T>> {
    let parquet_url: &Option<String> = &io.build_cache_url;

    let start = std::time::Instant::now();

    if let Some(cache) = io.build_cache_mode
        && !matches!(cache, BuildCacheMode::Read | BuildCacheMode::ReadWrite)
    {
        return Ok(vec![]);
    }
    let meta_data_file = resolve_parquet_path(&io.out_dir, parquet_url, default_filename)?;
    let path = meta_data_file.as_path();

    if !stdfs::exists(path)? {
        return Ok(vec![]);
    }

    let file = stdfs::File::open(path)?;

    let record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to create ParquetRecordBatchReader: {e}"),
        )
    })?;

    let mut items = Vec::new();
    for batch in record_batch_reader {
        let batch = batch.map_err(|e| {
            FsError::new(
                ErrorCode::IoError,
                format!("Failed to read RecordBatch: {e}"),
            )
        })?;
        let batch_items = serde_arrow::from_record_batch::<Vec<T>>(&batch)
            .expect("Failed to deserialize from row");
        items.extend(batch_items);
    }
    let num_items = items.len();
    let desc = if io.show_timings {
        &format!(
            "read {num_items} entries in {} ms",
            start.elapsed().as_millis()
        )
    } else {
        &format!("read {num_items} entries",)
    };
    emit_progress_relative(
        io.status_reporter.as_ref(),
        progress_code,
        &io.out_dir,
        path,
        desc,
    );

    Ok(items)
}

pub fn write_parquet_file<T: Serialize>(
    io: &IoArgs,
    default_filename: &str,
    progress_code: &str,
    schema_fields: Vec<Field>,
    items: Vec<T>,
) -> FsResult<()> {
    let parquet_url: &Option<String> = &io.build_cache_url;

    if let Some(cache) = io.build_cache_mode
        && !matches!(cache, BuildCacheMode::Write | BuildCacheMode::ReadWrite)
    {
        return Ok(()); // no write if not in write mode
    }
    let meta_data_file = resolve_parquet_path(&io.out_dir, parquet_url, default_filename)?;
    let path = meta_data_file.as_path();

    // Create directory if it doesn't exist
    if let Some(parent) = path.parent() {
        stdfs::create_dir_all(parent)?;
    }

    let num_items = items.len();
    let start = std::time::Instant::now();

    let fields = schema_fields
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<FieldRef>>();
    let batch = to_record_batch(&fields, &items)
        .map_err(|e| FsError::new(ErrorCode::IoError, format!("Arrow serialize: {e}")))?;

    let file = stdfs::File::create(path)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to create ArrowWriter: {e}"),
        )
    })?;

    writer.write(&batch).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to write RecordBatch: {e}"),
        )
    })?;

    writer.close().map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to close ArrowWriter: {e}"),
        )
    })?;

    let desc = if io.show_timings {
        &format!(
            "Wrote {num_items} entries in {} ms",
            start.elapsed().as_millis()
        )
    } else {
        &format!("Wrote {num_items} entries",)
    };
    emit_progress_relative(
        io.status_reporter.as_ref(),
        progress_code,
        &io.out_dir,
        path,
        desc,
    );
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// batch oriented based read/write parquet files

pub fn write_parquet_with_batch<F>(
    io: &IoArgs,
    default_filename: &str,
    progress_code: &str,
    batch_fn: F,
) -> FsResult<()>
where
    F: FnOnce() -> FsResult<RecordBatch>,
{
    let parquet_url: &Option<String> = &io.build_cache_url;
    if let Some(cache) = io.build_cache_mode
        && !matches!(cache, BuildCacheMode::Write | BuildCacheMode::ReadWrite)
    {
        return Ok(());
    }
    let start = std::time::Instant::now();
    stdfs::create_dir_all(io.out_dir.join(DBT_METADATA_DIR_NAME))?;
    let file_path = resolve_parquet_path(&io.out_dir, parquet_url, default_filename)?;
    let batch = batch_fn()?;

    let file = File::create(&file_path)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to create ArrowWriter: {e}"),
        )
    })?;

    writer.write(&batch).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to write RecordBatch: {e}"),
        )
    })?;

    writer.close().map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to close ArrowWriter: {e}"),
        )
    })?;

    let num_items = batch.num_rows();
    let desc = if io.show_timings {
        &format!(
            "Wrote {num_items} entries in {} ms",
            start.elapsed().as_millis()
        )
    } else {
        &format!("Wrote {num_items} entries")
    };
    emit_progress_relative(
        io.status_reporter.as_ref(),
        progress_code,
        &io.out_dir,
        &file_path,
        desc,
    );
    Ok(())
}

pub fn read_parquet_with_batch<T, F>(
    io: &IoArgs,
    default_filename: &str,
    progress_code: &str,
    batch_fn: F,
) -> FsResult<Vec<T>>
where
    F: Fn(&RecordBatch) -> FsResult<Vec<T>>,
{
    let parquet_url: &Option<String> = &io.build_cache_url;

    if let Some(cache) = io.build_cache_mode
        && !matches!(cache, BuildCacheMode::Read | BuildCacheMode::ReadWrite)
    {
        return Ok(vec![]);
    }
    let start = std::time::Instant::now();
    let file_path = resolve_parquet_path(&io.out_dir, parquet_url, default_filename)?;

    if !stdfs::exists(&file_path).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!(
                "Failed to check existence of file at {}: {}",
                file_path.display(),
                e
            ),
        )
    })? {
        return Ok(vec![]);
    }

    let file = File::open(&file_path).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to open file at {}: {}", file_path.display(), e),
        )
    })?;

    let record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).map_err(|e| {
        FsError::new(
            ErrorCode::IoError,
            format!("Failed to create ParquetRecordBatchReader: {e}"),
        )
    })?;

    let mut items = Vec::new();
    for batch in record_batch_reader {
        let batch = batch.map_err(|e| {
            FsError::new(
                ErrorCode::IoError,
                format!("Failed to read RecordBatch: {e }"),
            )
        })?;
        let mut batch_items = batch_fn(&batch)?;
        items.append(&mut batch_items);
    }
    let num_items = items.len();
    let desc = if io.show_timings {
        &format!(
            "Read {num_items} entries in {} ms",
            start.elapsed().as_millis()
        )
    } else {
        &format!("Read {num_items} entries",)
    };
    emit_progress_relative(
        io.status_reporter.as_ref(),
        progress_code,
        &io.out_dir,
        &file_path,
        desc,
    );

    Ok(items)
}
