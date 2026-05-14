use std::collections::HashMap;
use std::sync::Arc;

use crate::AdapterResult;
use crate::AdapterType;
use crate::errors::{AdapterError, AdapterErrorKind};

use arrow::array::{Array, Int64Array};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

pub(crate) const SNOWFLAKE_DML_COLUMNS: &[&str] = &[
    "number of rows inserted",
    "number of rows updated",
    "number of rows deleted",
];

/// Information about a column that was renamed during disambiguation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenamedColumn<'a> {
    /// The original column name (duplicate).
    pub original: &'a str,
    /// The new unique column name (e.g., "col_2", "col_3").
    pub renamed: &'a str,
}

pub trait RecordBatchExt {
    fn first_value_as_i64(&self) -> Option<i64>;
    fn named_value_as_i64(&self, column_name: &str) -> Option<i64>;
    fn column_typed<'a>(&'a self, name: &str) -> AdapterResult<&'a Arc<dyn Array>>;
    fn column_values<T>(&self, column_name: &str) -> AdapterResult<T>
    where
        T: std::any::Any + Clone;
    fn rows_affected(&self, adapter_type: AdapterType) -> i64;
    fn query_id(&self, adapter_type: AdapterType) -> Option<String>;
    fn disambiguate_column_names(
        self,
        on_disambiguate: Option<impl FnOnce(&[RenamedColumn<'_>])>,
    ) -> RecordBatch;
}

impl RecordBatchExt for RecordBatch {
    fn first_value_as_i64(&self) -> Option<i64> {
        cast_column_to_i64(self.columns().first()?.as_ref())
    }

    fn named_value_as_i64(&self, column_name: &str) -> Option<i64> {
        let idx = self.schema().index_of(column_name).ok()?;
        cast_column_to_i64(self.column(idx).as_ref())
    }

    fn rows_affected(&self, adapter_type: AdapterType) -> i64 {
        if self.num_rows() == 0 {
            return 0;
        }
        if self.schema().has_dml_columns(adapter_type) {
            return SNOWFLAKE_DML_COLUMNS
                .iter()
                .filter_map(|col| self.named_value_as_i64(col))
                .sum();
        }
        self.num_rows() as i64
    }

    fn query_id(&self, adapter_type: AdapterType) -> Option<String> {
        let meta = self.schema();
        let meta = meta.metadata();
        match adapter_type {
            AdapterType::Snowflake => meta.get("SNOWFLAKE_QUERY_ID").cloned(),
            AdapterType::Bigquery => meta.get("BIGQUERY:query_id").cloned(),
            AdapterType::Databricks => meta.get("DATABRICKS_QUERY_ID").cloned(),
            _ => None,
        }
    }

    fn column_typed<'a>(&'a self, name: &str) -> AdapterResult<&'a Arc<dyn Array>> {
        self.column_by_name(name).ok_or_else(|| {
            let schema = self.schema();
            let columns = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
            AdapterError::new(
                AdapterErrorKind::Internal,
                format!("expected column {name} not found, available are: {columns:?}"),
            )
        })
    }

    fn column_values<T>(&self, column_name: &str) -> AdapterResult<T>
    where
        T: std::any::Any + Clone,
    {
        Ok(self
            .column_typed(column_name)?
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| {
                let schema = self.schema();
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

    fn disambiguate_column_names(
        self,
        on_disambiguate: Option<impl FnOnce(&[RenamedColumn<'_>])>,
    ) -> RecordBatch {
        let schema = self.schema();
        let fields = schema.fields();

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

        let renamed_columns: Vec<_> = fields
            .iter()
            .zip(new_names.iter())
            .filter(|(field, new_name)| field.name() != *new_name)
            .map(|(field, new_name)| RenamedColumn {
                original: field.name().as_str(),
                renamed: new_name.as_str(),
            })
            .collect();

        if renamed_columns.is_empty() {
            return self;
        }

        if let Some(callback) = on_disambiguate {
            callback(&renamed_columns);
        }

        let new_fields: Vec<_> = fields
            .iter()
            .zip(new_names.iter())
            .map(|(field, new_name)| Arc::new(field.as_ref().clone().with_name(new_name.clone())))
            .collect();

        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));

        RecordBatch::try_new(new_schema, self.columns().to_vec())
            .expect("disambiguate_column_names: schema and columns should be compatible")
    }
}

pub trait SchemaExt {
    fn has_dml_columns(&self, adapter_type: AdapterType) -> bool;
}

impl SchemaExt for Schema {
    fn has_dml_columns(&self, adapter_type: AdapterType) -> bool {
        match adapter_type {
            AdapterType::Snowflake => self
                .fields()
                .iter()
                .any(|f| SNOWFLAKE_DML_COLUMNS.contains(&f.name().as_str())),
            _ => false,
        }
    }
}

fn cast_column_to_i64(column: &dyn Array) -> Option<i64> {
    if column.is_empty() {
        return None;
    }
    let casted = cast_with_options(column, &DataType::Int64, &CastOptions::default())
        .inspect_err(|_| {
            debug_assert!(
                false,
                "cast_column_to_i64: unsupported data type {:?}",
                column.data_type()
            );
        })
        .ok()?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()?
        .iter()
        .next()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use dbt_test_primitives::assert_contains;

    #[test]
    fn test_has_dml_columns() {
        let dml_schema = Schema::new(vec![Field::new(
            "number of rows inserted",
            DataType::Int64,
            false,
        )]);
        assert!(dml_schema.has_dml_columns(AdapterType::Snowflake));
        assert!(!dml_schema.has_dml_columns(AdapterType::Bigquery));
        let select_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert!(!select_schema.has_dml_columns(AdapterType::Snowflake));
    }

    #[test]
    fn test_named_value_as_i64_missing_column_returns_none() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![99]))])
                .unwrap();
        assert!(batch.named_value_as_i64("nonexistent").is_none());
    }

    #[test]
    fn test_snowflake_merge_sums_dml_counts() {
        let schema = Schema::new(vec![
            Field::new("number of rows inserted", DataType::Int64, false),
            Field::new("number of rows updated", DataType::Int64, false),
            Field::new("number of rows deleted", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![50])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        assert_eq!(batch.rows_affected(AdapterType::Snowflake), 160);
        assert_eq!(batch.rows_affected(AdapterType::Bigquery), 1);
    }

    #[test]
    fn test_snowflake_merge_decimal128_high_precision() {
        let schema = Schema::new(vec![
            Field::new(
                "number of rows inserted",
                DataType::Decimal128(38, 0),
                false,
            ),
            Field::new("number of rows updated", DataType::Decimal128(38, 0), false),
            Field::new("number of rows deleted", DataType::Decimal128(38, 0), false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![200])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![75])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![25])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        assert_eq!(batch.rows_affected(AdapterType::Snowflake), 300);
    }

    #[test]
    fn test_snowflake_insert_only_partial_dml_columns() {
        let schema = Schema::new(vec![Field::new(
            "number of rows inserted",
            DataType::Int64,
            false,
        )]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![42]))])
                .unwrap();
        assert_eq!(batch.rows_affected(AdapterType::Snowflake), 42);
    }

    #[test]
    fn test_snowflake_empty_batch_returns_zero() {
        let schema = Schema::new(vec![
            Field::new("number of rows inserted", DataType::Int64, false),
            Field::new("number of rows updated", DataType::Int64, false),
            Field::new("number of rows deleted", DataType::Int64, false),
        ]);
        assert_eq!(
            RecordBatch::new_empty(Arc::new(schema)).rows_affected(AdapterType::Snowflake),
            0
        );
    }

    #[test]
    fn test_snowflake_null_dml_values_treated_as_zero() {
        let schema = Schema::new(vec![
            Field::new("number of rows inserted", DataType::Int64, true),
            Field::new("number of rows updated", DataType::Int64, true),
            Field::new("number of rows deleted", DataType::Int64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(50)])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
            ],
        )
        .unwrap();
        assert_eq!(batch.rows_affected(AdapterType::Snowflake), 50);
    }

    #[test]
    fn test_snowflake_select_uses_num_rows() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        assert_eq!(batch.rows_affected(AdapterType::Snowflake), 3);
    }
    use std::sync::LazyLock;

    static TEST_DATA: LazyLock<RecordBatch> = LazyLock::new(|| {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["FOO"])),
                Arc::new(Float64Array::from(vec![42.0])),
            ],
        )
        .unwrap()
    });

    #[test]
    fn test_column_values_success() {
        let result: AdapterResult<StringArray> = TEST_DATA.column_values("name");
        assert!(result.is_ok());
    }

    #[test]
    fn test_column_values_column_not_found() {
        let result: AdapterResult<Int32Array> = TEST_DATA.column_values("nonexistent");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), AdapterErrorKind::Internal);
        assert_contains!(error.message(), "expected column nonexistent not found");
        assert_contains!(error.message(), "available are");
        assert_contains!(error.message(), "name");
        assert_contains!(error.message(), "score");
    }

    #[test]
    fn test_column_values_wrong_type() {
        let result: AdapterResult<Int32Array> = TEST_DATA.column_values("name");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), AdapterErrorKind::Internal);
        assert_contains!(error.message(), "expected column of type");
        assert!(error.message().contains(
            "arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::Int32Type>"
        ));
    }

    #[test]
    fn test_disambiguate_no_duplicates() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();

        let callback_invoked = std::cell::Cell::new(false);
        let result = batch.disambiguate_column_names(Some(|_: &[RenamedColumn]| {
            callback_invoked.set(true);
        }));
        assert!(!callback_invoked.get());
        let schema = result.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_disambiguate_with_duplicates() {
        let schema = Schema::new(vec![
            Field::new("A", DataType::Int32, false),
            Field::new("B", DataType::Int32, false),
            Field::new("A", DataType::Int32, false),
            Field::new("A", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![10, 11, 12])),
            ],
        )
        .unwrap();

        let captured = std::cell::RefCell::new(Vec::new());
        let result = batch.disambiguate_column_names(Some(|renamed: &[RenamedColumn]| {
            captured.borrow_mut().extend(
                renamed
                    .iter()
                    .map(|r| (r.original.to_string(), r.renamed.to_string())),
            );
        }));

        let renamed = captured.into_inner();
        assert_eq!(renamed.len(), 2);
        assert_eq!(renamed[0], ("A".to_string(), "A_2".to_string()));
        assert_eq!(renamed[1], ("A".to_string(), "A_3".to_string()));
        let schema = result.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["A", "B", "A_2", "A_3"]);
    }

    #[test]
    fn test_disambiguate_multiple_duplicates() {
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
        let result = batch.disambiguate_column_names(None::<fn(&[RenamedColumn])>);
        let schema = result.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["x", "y", "x_2", "y_2", "x_3"]);
    }
}
