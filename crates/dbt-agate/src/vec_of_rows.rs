//! The vec-of-rows representation of Agate tables.
//!
//!

use std::sync::Arc;

use arrow::array::{BooleanBuilder, Decimal128Builder, RecordBatch, StringBuilder};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use minijinja::{Error as MinijinjaError, Value};

use crate::converters::make_array_converter;
use crate::flat_record_batch::FlatRecordBatch;

/// Internal table representation that stores rows as a vector of minijinja values.
#[derive(Debug)]
pub(crate) struct VecOfRows {
    /// The schema of the table.
    column_names: Vec<String>,
    column_types: Option<Vec<String>>,
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
            column_names,
            column_types: None,
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

    pub fn new(column_names: Vec<String>, column_types: Vec<String>, rows: Vec<Value>) -> Self {
        debug_assert!(
            rows.is_empty() || column_names.len() == rows[0].len().unwrap_or(0),
            "number of columns doesn't match the number of values in the first row"
        );
        VecOfRows {
            column_names,
            column_types: Some(column_types),
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
        let column_names = vec![self.column_names[idx].clone()];
        let column_types = self
            .column_types
            .as_ref()
            .map(|types| vec![types[idx].clone()]);
        let source = self
            .source
            .as_ref()
            .map(|batch| batch.with_single_column(idx));
        let new_vec_of_rows = VecOfRows {
            column_names,
            column_types,
            rows,
            source,
        };
        Ok(new_vec_of_rows)
    }

    pub fn schema(&self) -> &Vec<String> {
        &self.column_names
    }

    pub fn to_record_batch(&self) -> Arc<RecordBatch> {
        if let Some(source) = &self.source {
            source.flat().clone()
        } else {
            // TODO(alex): THIS IS JUST AN EMPTY RECORD BATCH
            //             FULL CONVERSION NEEDS TO BE IMPLEMENTED
            let batch = RecordBatch::new_empty(Arc::new(Schema::new(
                self.column_names
                    .iter()
                    .map(|s| Field::new(s, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            )));
            Arc::new(batch)
        }
    }

    pub fn to_record_batch_with_type_parser(
        &self,
        type_parser: impl Fn(&str) -> DataType,
    ) -> Arc<RecordBatch> {
        if let Some(source) = &self.source {
            source.flat().clone()
        } else {
            // For now, create a RecordBatch with proper column types based on the first row
            // This ensures we get the right column types instead of all strings
            if self.rows.is_empty() {
                // Create an empty RecordBatch with string columns
                let fields: Vec<Field> = self
                    .column_names
                    .iter()
                    .map(|s| Field::new(s, DataType::Utf8, true))
                    .collect();
                let schema = Arc::new(Schema::new(fields));
                let columns: Vec<Arc<dyn arrow::array::Array>> = self
                    .column_names
                    .iter()
                    .map(|_| {
                        Arc::new(arrow::array::StringArray::from(Vec::<String>::new()))
                            as Arc<dyn arrow::array::Array>
                    })
                    .collect();
                return Arc::new(RecordBatch::try_new(schema, columns).unwrap());
            }

            // Create a RecordBatch with proper column types
            // For now, we'll use a simple approach: create columns with inferred types
            let mut fields = Vec::new();
            let mut columns = Vec::new();

            // Get the first row to infer types
            if let Some(first_row) = self.rows.first() {
                if let Ok(first_row_values) = first_row.try_iter() {
                    let first_row_values: Vec<Value> = first_row_values.collect();

                    for (i, ((col_name, col_data_type), _)) in self
                        .column_names
                        .iter()
                        .zip(self.column_types.as_ref().unwrap().iter())
                        .zip(first_row_values.iter())
                        .enumerate()
                    {
                        // use the provided type parser function to convert the data_type string into a proper data_type
                        let data_type = type_parser(col_data_type);

                        fields.push(Field::new(col_name, data_type.clone(), true));

                        // Create a column with the inferred type
                        match data_type {
                            DataType::Boolean => {
                                let mut builder = BooleanBuilder::new();
                                for row in &self.rows {
                                    if let Ok(row_values) = row.try_iter() {
                                        let row_values: Vec<Value> = row_values.collect();
                                        if i < row_values.len() {
                                            if let Some(s) = row_values[i].as_str() {
                                                match s.to_lowercase().as_str() {
                                                    "true" => builder.append_value(true),
                                                    "false" => builder.append_value(false),
                                                    _ => builder.append_null(),
                                                }
                                            } else if row_values[i].kind()
                                                == minijinja::value::ValueKind::Bool
                                            {
                                                builder.append_value(row_values[i].is_true());
                                            } else {
                                                builder.append_null();
                                            }
                                        } else {
                                            builder.append_null();
                                        }
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                columns.push(
                                    Arc::new(builder.finish()) as Arc<dyn arrow::array::Array>
                                );
                            }
                            DataType::Decimal128(precision, scale) => {
                                let mut builder = Decimal128Builder::new()
                                    .with_data_type(DataType::Decimal128(precision, scale));
                                for row in &self.rows {
                                    if let Ok(row_values) = row.try_iter() {
                                        let row_values: Vec<Value> = row_values.collect();
                                        if i < row_values.len() {
                                            if let Some(val) = row_values[i].as_i64() {
                                                builder.append_value(val as i128);
                                            } else {
                                                builder.append_null();
                                            }
                                        } else {
                                            builder.append_null();
                                        }
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                columns.push(
                                    Arc::new(builder.finish()) as Arc<dyn arrow::array::Array>
                                );
                            }
                            _ => {
                                // Default to string for other types
                                let mut builder = StringBuilder::new();
                                for row in &self.rows {
                                    if let Ok(row_values) = row.try_iter() {
                                        let row_values: Vec<Value> = row_values.collect();
                                        if i < row_values.len() {
                                            if let Some(s) = row_values[i].as_str() {
                                                builder.append_value(s);
                                            } else {
                                                builder.append_value(row_values[i].to_string());
                                            }
                                        } else {
                                            builder.append_null();
                                        }
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                columns.push(
                                    Arc::new(builder.finish()) as Arc<dyn arrow::array::Array>
                                );
                            }
                        }
                    }
                }
            }

            let schema = Arc::new(Schema::new(fields));
            Arc::new(RecordBatch::try_new(schema, columns).expect("Failed to create RecordBatch"))
        }
    }

    /// Get the rows.
    pub fn rows(&self) -> &Vec<Value> {
        &self.rows
    }

    /// Get the number of colums
    pub fn num_columns(&self) -> usize {
        self.column_names.len()
    }

    pub(crate) fn with_renamed_columns(&self, renamed_columns: Vec<String>) -> Self {
        debug_assert!(renamed_columns.len() == self.column_names.len());
        // rename at the source as well if it exists
        let source = self
            .source
            .as_ref()
            .map(|flat| flat.with_renamed_columns(&renamed_columns));
        VecOfRows {
            column_names: renamed_columns,
            column_types: self.column_types.clone(),
            rows: self.rows.clone(),
            source,
        }
    }
}
