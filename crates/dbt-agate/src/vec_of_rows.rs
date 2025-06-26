//! The vec-of-rows representation of Agate tables.
//!
//!

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use minijinja::{Error as MinijinjaError, Value};

use crate::converters::make_array_converter;
use crate::flat_record_batch::FlatRecordBatch;

/// Internal table representation that stores rows as a vector of minijinja values.
#[derive(Debug)]
pub(crate) struct VecOfRows {
    /// The schema of the table.
    ///
    /// TODO(felipecrv): just column names for now, but will evolve.
    schema: Vec<String>,
    rows: Vec<Value>,
    /// Original Arrow data from which these rows were derived.
    source: Option<FlatRecordBatch>,
}

impl VecOfRows {
    pub fn from_flat_record_batch(batch: FlatRecordBatch) -> Result<Self, ArrowError> {
        let column_names: Vec<_> = batch
            .flat()
            .as_ref()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();

        let converters = batch
            .columns()
            .iter()
            .map(|array| make_array_converter(&**array))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        let nrows = batch.num_rows();
        let ncols = batch.num_columns();
        let mut rows = Vec::with_capacity(nrows);
        for row_idx in 0..nrows {
            let mut row = Vec::with_capacity(ncols);
            for col_converter in converters.iter() {
                let value = (*col_converter).to_value(row_idx);
                row.push(value);
            }
            rows.push(Value::from(row));
        }

        let vec_of_rows = VecOfRows {
            schema: column_names,
            rows,
            source: Some(batch),
        };
        Ok(vec_of_rows)
    }

    /// Create a VecOfRows from an Arrow RecordBatch.
    #[allow(dead_code)]
    pub fn from_record_batch(batch: Arc<RecordBatch>) -> Result<Self, ArrowError> {
        let flat = FlatRecordBatch::new(batch);
        Self::from_flat_record_batch(flat)
    }

    pub fn new(column_names: Vec<String>, rows: Vec<Value>) -> Self {
        debug_assert!(
            rows.is_empty() || column_names.len() == rows[0].len().unwrap_or(0),
            "number of columns doesn't match the number of values in the first row"
        );
        VecOfRows {
            schema: column_names,
            rows,
            source: None,
        }
    }

    pub fn with_single_column(&self, idx: usize) -> Result<VecOfRows, MinijinjaError> {
        let results = (|| -> Result<Vec<Value>, MinijinjaError> {
            let mut rows = Vec::new();
            for row in self.rows() {
                row.get_item_by_index(idx).map(|value| rows.push(value))?;
            }
            Ok(rows)
        })();
        let rows = results?;
        let schema = vec![self.schema[idx].clone()];
        let source = self
            .source
            .as_ref()
            .map(|batch| batch.with_single_column(idx));
        let new_vec_of_rows = VecOfRows {
            schema,
            rows,
            source,
        };
        Ok(new_vec_of_rows)
    }

    pub fn schema(&self) -> &Vec<String> {
        &self.schema
    }

    pub fn to_record_batch(&self) -> Arc<RecordBatch> {
        if let Some(source) = &self.source {
            source.flat().clone()
        } else {
            // TODO(alex): THIS IS JUST AN EMPTY RECORD BATCH
            //             FULL CONVERSION NEEDS TO BE IMPLEMENTED
            let batch = RecordBatch::new_empty(Arc::new(Schema::new(
                self.schema
                    .iter()
                    .map(|s| Field::new(s, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            )));
            Arc::new(batch)
        }
    }

    /// Get the rows.
    pub fn rows(&self) -> &Vec<Value> {
        &self.rows
    }

    /// Get the number of colums
    pub fn num_columns(&self) -> usize {
        self.schema.len()
    }

    pub(crate) fn with_renamed_columns(&self, renamed_columns: Vec<String>) -> Self {
        debug_assert!(renamed_columns.len() == self.schema.len());
        // rename at the source as well if it exists
        let source = self
            .source
            .as_ref()
            .map(|flat| flat.with_renamed_columns(&renamed_columns));
        VecOfRows {
            schema: renamed_columns,
            rows: self.rows.clone(),
            source,
        }
    }
}
