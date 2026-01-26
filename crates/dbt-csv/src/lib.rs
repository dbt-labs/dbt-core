//! Custom CSV reading for dbt seeds
//!
//! This crate is a fork of arrow-csv, designed for reading CSV files with
//! Python/dbt/agate-compatible inference and parsing logic.
//!
//! # Usage
//!
//! ```ignore
//! use dbt_csv::reader::{Format, ReaderBuilder};
//! use std::io::Cursor;
//!
//! let data = std::fs::read("path/to/file.csv")?;
//! let format = Format::new(b',', true); // delimiter, has_header
//! let (agate_schema, _) = format.infer_agate_schema(Cursor::new(&data), None)?;
//! let reader = ReaderBuilder::new(agate_schema)
//!     .with_header(true)
//!     .build(Cursor::new(data))?;
//! let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
//! ```
//!
//! # Type Inference
//!
//! Type inference follows Python's agate library behavior:
//! - Type priority: Integer > Number > Date > DateTime > ISODateTime > Boolean > Text
//! - Only "true"/"false" are valid booleans (not "yes"/"no", "1"/"0")
//! - Null values are "" and "null" (case-insensitive)

pub mod reader;
pub mod type_tester;

pub use self::reader::Format;
pub use self::reader::NullRegex;
pub use self::reader::Reader;
pub use self::reader::ReaderBuilder;
pub use self::type_tester::{AgateColumn, AgateSchema, AgateType};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use std::io::Cursor;
use std::path::Path;

fn map_csv_error(error: csv::Error) -> ArrowError {
    match error.kind() {
        csv::ErrorKind::Io(error) => ArrowError::CsvError(error.to_string()),
        csv::ErrorKind::Utf8 { pos, err } => ArrowError::CsvError(format!(
            "Encountered UTF-8 error while reading CSV file: {}{}",
            err,
            pos.as_ref()
                .map(|pos| format!(" at line {}", pos.line()))
                .unwrap_or_default(),
        )),
        csv::ErrorKind::UnequalLengths {
            pos,
            expected_len,
            len,
        } => ArrowError::CsvError(format!(
            "Encountered unequal lengths between records on CSV file. Expected {} \
                 records, found {} records{}",
            expected_len,
            len,
            pos.as_ref()
                .map(|pos| format!(" at line {}", pos.line()))
                .unwrap_or_default(),
        )),
        _ => ArrowError::CsvError("Error reading CSV file".to_string()),
    }
}

/// Options for custom CSV reading.
#[derive(Debug, Clone)]
pub struct CustomCsvOptions {
    /// Field delimiter
    pub delimiter: u8,
    /// Whether the CSV has a header row
    pub has_header: bool,
    /// Max records to use for schema inference
    pub max_records_for_inference: Option<usize>,
    /// Column names to force as Text type (case-sensitive exact match).
    /// These columns skip type inference and are always treated as UTF8 strings.
    /// This matches agate's behavior when users specify column types in dbt's YAML config.
    pub text_columns: Vec<String>,
}

impl Default for CustomCsvOptions {
    fn default() -> Self {
        CustomCsvOptions {
            delimiter: b',',
            has_header: true,
            max_records_for_inference: Some(1000),
            text_columns: Vec::new(),
        }
    }
}

impl CustomCsvOptions {
    /// Set the field delimiter
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set whether the CSV has a header row
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set column names to force as Text type (case-sensitive exact match).
    /// These columns skip type inference and are always treated as UTF8 strings.
    pub fn with_text_columns(mut self, text_columns: Vec<String>) -> Self {
        self.text_columns = text_columns;
        self
    }
}

/// Result of reading a CSV file into Arrow record batches.
#[derive(Debug)]
pub struct CsvReadResult {
    /// The Arrow schema (guaranteed even if batches is empty)
    pub schema: SchemaRef,
    /// The record batches containing the data
    pub batches: Vec<RecordBatch>,
    /// Column names from `text_columns` that didn't match any CSV header
    pub unmatched_text_columns: Vec<String>,
}

pub fn read_to_arrow_records(
    path: &Path,
    options: &CustomCsvOptions,
) -> Result<CsvReadResult, ArrowError> {
    let data = std::fs::read(path).map_err(|e| ArrowError::CsvError(e.to_string()))?;
    // Use Python-compatible type inference (returns AgateSchema)
    // Pass text_columns to force those columns as Text type
    let format = Format::new(options.delimiter, options.has_header);
    let (agate_schema, _, unmatched_text_columns) = format.infer_agate_schema_with_text_columns(
        Cursor::new(&data),
        options.max_records_for_inference,
        &options.text_columns,
    )?;

    // Get Arrow schema (available even if CSV has no data rows)
    let schema = agate_schema.to_arrow_schema_ref();

    // Read data with flexible row handling (matches Python csv module)
    // ReaderBuilder now takes AgateSchema and parses according to AgateType semantics
    let reader = ReaderBuilder::new(agate_schema)
        .with_header(options.has_header)
        .with_delimiter(options.delimiter)
        .with_truncated_rows(true)
        .build(Cursor::new(data))?;

    let batches = reader.collect::<Result<Vec<_>, _>>()?;

    Ok(CsvReadResult {
        schema,
        batches,
        unmatched_text_columns,
    })
}
