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
use arrow_schema::ArrowError;

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
