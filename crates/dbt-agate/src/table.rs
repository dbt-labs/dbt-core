use crate::Tuple;
use crate::column::Column;
use crate::columns::ColumnNamesAsTuple;
use crate::columns::Columns;
use crate::flat_record_batch::FlatRecordBatch;
use crate::print_table::print_table;
use crate::row::Row;
use crate::rows::Rows;
use crate::vec_of_rows::VecOfRows;

use arrow::record_batch::RecordBatch;
use arrow_schema::{ArrowError, Schema};
use minijinja::Value;
use minijinja::arg_utils::ArgsIter;
use minijinja::listener::RenderingEventListener;
use minijinja::value::Kwargs;
use minijinja::value::ValueMap;
use minijinja::value::mutable_map::MutableMap;
use minijinja::value::{Enumerator, Object};
use minijinja::{Error as MinijinjaError, State};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::OnceLock;

/// Internal table representation.
///
/// An AgateTable can be internally represented as an Arrow RecordBatch and,
/// optionally, a vector of Jinja objects -- one iterable per row.
///
/// Both representations are immutable.
#[derive(Debug)]
pub(crate) struct TableRepr {
    /// Arrow representation of the table.
    flat: FlatRecordBatch,
    /// Lazy-computed representation of the table as a vector of rows.
    row_table: OnceLock<Result<Arc<VecOfRows>, Arc<ArrowError>>>,
}

impl TableRepr {
    fn new(flat: FlatRecordBatch, row_table: Option<Arc<VecOfRows>>) -> Self {
        let row_table = match row_table {
            Some(vec_of_rows) => OnceLock::from(Ok(vec_of_rows)),
            None => OnceLock::new(),
        };
        Self { flat, row_table }
    }

    /// Force the lazy initialization of table as a [VecOfRows].
    ///
    /// We try to delay the conversion from the Arrow-based [FlatRecordBatch] representation
    /// to [VecOfRows] until we actually need it. This means we can work with the Arrow-based
    /// representation for as long as possible, which is more efficient and structured.
    ///
    /// Reasons to call this function:
    /// - We don't want or don't have the time to implement the functionality against the
    ///   Arrow-based representation.
    /// - We must have the values as Jinja objects (e.g., for passing values to a Jinja
    ///   template)
    ///
    /// It's OK to call this function multiple times, it will only convert the table once.
    ///
    /// We *always* have the Arrow-based representation, so if you can implement Agate
    /// operations delegating to arrow-compute or some custom Arrow-based logic, you should
    /// do so.
    #[allow(dead_code)]
    pub fn force_row_table(&self) -> Result<&Arc<VecOfRows>, MinijinjaError> {
        let res = self.row_table.get_or_init(|| {
            let vec_of_rows = VecOfRows::from_flat_record_batch(&self.flat)?;
            Ok(Arc::new(vec_of_rows))
        });
        match res {
            Ok(table) => Ok(table),
            Err(e) => {
                let e = MinijinjaError::new(minijinja::ErrorKind::InvalidOperation, e.to_string());
                Err(e)
            }
        }
    }

    /// Peek at the row table without forcing its initialization.
    pub fn peek_row_table(&self) -> Option<&Arc<VecOfRows>> {
        self.row_table.get().and_then(|res| res.as_ref().ok())
    }

    pub fn to_record_batch(&self) -> Arc<RecordBatch> {
        Arc::clone(self.flat.inner())
    }

    pub fn adjusted_index(idx: isize, len: usize) -> Option<usize> {
        // Convert len to isize for consistent comparisons
        let len = len as isize;

        // Handle negative indices (e.g., -1 means last element)
        let adjusted = if idx < 0 { len + idx } else { idx };

        // Check if the adjusted index is within bounds
        if adjusted >= 0 && adjusted < len {
            Some(adjusted as usize)
        } else {
            None
        }
    }

    fn adjusted_column_index(&self, idx: isize) -> Option<usize> {
        Self::adjusted_index(idx, self.num_columns())
    }

    fn adjusted_row_index(&self, idx: isize) -> Option<usize> {
        Self::adjusted_index(idx, self.num_rows())
    }

    // Columns ----------------------------------------------------------------

    pub fn num_columns(&self) -> usize {
        self.flat.num_columns()
    }

    pub fn get_column(self: &Arc<Self>, idx: isize) -> Option<Column> {
        let idx = self.adjusted_column_index(idx)?;
        let col = Column::new(idx, Arc::clone(self));
        Some(col)
    }

    pub fn column_name(&self, idx: isize) -> Option<&String> {
        let idx = self.adjusted_column_index(idx)?;
        let name = self.flat.schema_ref().field(idx).name();
        Some(name)
    }

    pub fn columns(self: &Arc<Self>) -> Columns {
        Columns::new(Arc::clone(self))
    }

    pub fn column_names(&self) -> impl Iterator<Item = &String> + '_ {
        self.flat
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name())
    }

    pub fn single_column_table(&self, idx: isize) -> Option<Arc<TableRepr>> {
        let idx = self.adjusted_column_index(idx)?;
        let flat_with_single_column = self.flat.with_single_column(idx);
        let repr = TableRepr::new(flat_with_single_column, None);
        Some(Arc::new(repr))
    }

    /// Return a single-column table with the distinct values in this column.
    pub fn column_distinct(&self, col_idx: isize) -> Arc<Self> {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!("column_distinct")
    }

    pub fn column_without_nulls(&self, col_idx: isize) -> Arc<Self> {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!("column_without_nulls")
    }

    pub fn column_sorted(&self, col_idx: isize) -> Arc<Self> {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!("column_sorted")
    }

    pub fn column_without_nulls_sorted(&self, col_idx: isize) -> Arc<Self> {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!("column_without_nulls_sorted")
    }

    pub fn count_occurrences_of_value_in_column(&self, _needle: &Value, col_idx: isize) -> usize {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!("count_occurrences_of_value_in_column")
    }

    pub fn index_of_value_in_column(&self, _needle: &Value, col_idx: isize) -> Option<usize> {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!("index_of_value_in_column")
    }

    fn with_renamed_columns(&self, renamed_columns: Vec<String>) -> Arc<Self> {
        debug_assert!(renamed_columns.len() == self.num_columns());
        let new_batch = self.flat.with_renamed_columns(&renamed_columns);
        let new_vec_of_rows = self.peek_row_table().map(Arc::clone);
        let repr = TableRepr::new(new_batch, new_vec_of_rows);
        Arc::new(repr)
    }

    // Rows -------------------------------------------------------------------

    pub fn num_rows(&self) -> usize {
        self.flat.num_rows()
    }

    pub fn row_by_index(self: &Arc<Self>, idx: isize) -> Option<Value> {
        self.adjusted_row_index(idx).map(|i| {
            let row = Row::new(i, Arc::clone(self));
            Value::from_object(row)
        })
    }

    pub fn rows(self: &Arc<Self>) -> Rows {
        Rows::new(Arc::clone(self))
    }

    pub fn count_occurrences_of_row(&self, _needle: &Value) -> usize {
        todo!("count_occurrences_of_row")
    }

    pub fn index_of_row(&self, _needle: &Value) -> Option<usize> {
        todo!("index_of_row")
    }

    pub fn count_occurrences_of_value_in_row(
        self: &Arc<Self>,
        _needle: &Value,
        row_idx: isize,
    ) -> usize {
        let _row = self.row_by_index(row_idx).unwrap();
        todo!("count_occurrences_of_value_in_row")
    }

    pub fn index_of_value_in_row(
        self: &Arc<Self>,
        _needle: &Value,
        row_idx: isize,
    ) -> Option<usize> {
        let _row = self.row_by_index(row_idx).unwrap();
        todo!("index_of_value_in_row")
    }

    // Cells ------------------------------------------------------------------

    pub fn cell(&self, row_idx: isize, col_idx: isize) -> Option<Value> {
        let row_idx = self.adjusted_row_index(row_idx)?;
        let col_idx = self.adjusted_column_index(col_idx)?;
        self.peek_row_table().map_or_else(
            || {
                let value = self.flat.column_converter(col_idx).to_value(row_idx);
                Some(value)
            },
            |vec_of_rows| {
                let row: &Value = vec_of_rows.rows_ref().get(row_idx)?;
                match row.get_item_by_index(col_idx) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        debug_assert!(false, "Unexpected error: {e}");
                        None
                    }
                }
            },
        )
    }
}

/// The AgateTable object.
///
/// Tables are immutable. Instead of modifying the data, various methods can be used to
/// create new, derivative tables.
///
/// Tables are not themselves iterable, but the columns of the table can be
/// accessed via [`AgateTable::columns`] and the rows via [`AgateTable::rows`]. Both
/// sequences can be accessed either by numeric index or by name. (In the case of
/// rows, row names are optional.)
#[derive(Debug, Clone)]
pub struct AgateTable {
    /// The internal representation of the table.
    repr: Arc<TableRepr>,
}

impl AgateTable {
    /// Create an AgateTable from an Arrow RecordBatch.
    pub fn new(batch: Arc<RecordBatch>) -> Self {
        Self::from_record_batch(batch)
    }

    /// Create an AgateTable from an Arrow RecordBatch.
    pub fn from_record_batch(batch: Arc<RecordBatch>) -> Self {
        let flat = FlatRecordBatch::try_new(batch).unwrap();
        let repr = TableRepr::new(flat, None);
        Self::from_repr(Arc::new(repr))
    }

    pub(crate) fn from_repr(repr: Arc<TableRepr>) -> Self {
        Self { repr }
    }

    /// Converts this AgateTable into an Arrow RecordBatch.
    pub fn to_record_batch(&self) -> Arc<RecordBatch> {
        self.repr.to_record_batch()
    }

    /// Get the internal representation of the table.
    pub fn cell(&self, row_idx: isize, col_idx: isize) -> Option<Value> {
        self.repr.cell(row_idx, col_idx)
    }

    // Columns ----------------------------------------------------------------

    /// Get the number of columns.
    pub fn num_columns(&self) -> usize {
        self.repr.num_columns()
    }

    /// Get the columns.
    pub fn columns(&self) -> Columns {
        self.repr.columns()
    }

    /// Get a single column name.
    pub fn column_name(&self, idx: isize) -> Option<&String> {
        self.repr.column_name(idx)
    }

    /// Get the column names.
    pub fn column_names(&self) -> Vec<String> {
        self.repr.column_names().map(|s| s.to_owned()).collect()
    }

    // Rows -------------------------------------------------------------------

    /// Get the number of rows.
    pub fn num_rows(&self) -> usize {
        self.repr.num_rows()
    }

    /// Get the rows as Jinja value.
    pub fn rows(&self) -> Rows {
        self.repr.rows()
    }

    /// Get the row names.
    pub fn row_names(&self) -> Option<Tuple> {
        // TODO(felipecrv): implement row names logic
        None
    }

    // Rest of API ------------------------------------------------------------

    fn rename(
        &self,
        column_names: Option<&Value>, // array or map
        row_names: Option<&Value>,    // array or map
        slug_columns: bool,
        slug_rows: bool,
        _kwargs: &Kwargs,
    ) -> Result<AgateTable, MinijinjaError> {
        let renamed_columns = column_names.map(|v| {
            let old = self.column_names();
            if let Some(map) = v.downcast_object_ref::<ValueMap>() {
                let mut renamed = old.clone();
                for (key, value) in map {
                    for (i, col) in old.iter().enumerate() {
                        if key.as_str().is_some_and(|k| k == col) {
                            renamed[i] = value.to_string();
                        }
                    }
                }
                renamed
            } else if let Some(map) = v.downcast_object_ref::<MutableMap>() {
                let map: ValueMap = map.clone().into();
                let mut renamed = old.clone();
                for (key, value) in map {
                    for (i, col) in old.iter().enumerate() {
                        if key.as_str().is_some_and(|k| k == col) {
                            renamed[i] = value.to_string();
                        }
                    }
                }
                renamed
            } else if let Some(array) = v.downcast_object_ref::<Vec<String>>() {
                let mut renamed = old;
                for (i, col) in array.iter().enumerate() {
                    renamed[i] = col.to_string();
                }
                renamed
            } else {
                unreachable!("Agate.rename: column_names must be a map or an array");
            }
        });
        if row_names.is_some() {
            return Err(MinijinjaError::new(
                minijinja::ErrorKind::InvalidOperation,
                "Agate.rename: renaming row names is not implemented yet",
            ));
        }
        if slug_columns || slug_rows {
            return Err(MinijinjaError::new(
                minijinja::ErrorKind::InvalidOperation,
                "Agate.rename: slugging columns or rows is not implemented yet",
            ));
        }

        if let Some(renamed_columns) = renamed_columns {
            let repr = self.repr.with_renamed_columns(renamed_columns);
            Ok(AgateTable::from_repr(repr))
        } else {
            Ok(self.clone())
        }
    }
}

impl Default for AgateTable {
    fn default() -> Self {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        Self::from_record_batch(Arc::new(batch))
    }
}

// TODO(felipecrv): implement the AgateTable Python API
// https://github.com/wireservice/agate/blob/master/agate/table/__init__.py#L34
impl Object for AgateTable {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        // TODO(venka): update state to be aware of phase so we don't duplicate functions for each
        // phase with minor differences
        // This is to implement 'for row in table' enumeration
        if let Some(idx) = key.as_i64() {
            return self.repr.row_by_index(idx as isize);
        }
        match key.as_str()? {
            "columns" => {
                let columns = self.columns();
                Some(Value::from_object(columns))
            }
            "column_names" => {
                let names = self.column_names();
                let repr = ColumnNamesAsTuple::new(names);
                let tuple = Tuple(Box::new(repr));
                Some(Value::from_object(tuple))
            }
            "rows" => {
                let rows = self.rows();
                Some(Value::from_object(rows))
            }
            "row_names" => {
                let names = self.row_names()?;
                Some(Value::from_object(names))
            }
            // TODO(venkaa28, felipecrv): return NoOp only at Parsetime
            _ => Some(Value::UNDEFINED),
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Seq(self.num_rows())
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match name {
            "print_table" => {
                // Parse arguments or use defaults matching Python implementation:
                //
                //     def print_table(self, max_rows=20, max_columns=6,
                //         output=sys.stdout, max_column_width=20, locale=None,
                //         max_precision=3):
                //
                // TODO: implement output, locale and max_precision
                let iter = ArgsIter::new("Table.print_table", &[], args);
                let max_rows = iter.next_kwarg::<Option<i64>>("max_rows")?.unwrap_or(20) as usize;
                let max_columns =
                    iter.next_kwarg::<Option<i64>>("max_columns")?.unwrap_or(6) as usize;
                let _output = iter.next_kwarg::<Option<&Value>>("output")?;
                let max_column_width = iter
                    .next_kwarg::<Option<i64>>("max_column_width")?
                    .unwrap_or(20) as usize;
                let _locale = iter.next_kwarg::<Option<&Value>>("locale")?;
                let _max_precision = iter.next_kwarg::<Option<&Value>>("max_precision")?;
                iter.finish()?;

                print_table(self, max_rows, max_columns, max_column_width)
            }
            "rename" => {
                //     def rename(column_names=None, row_names=None,
                //                slug_columns=False, slug_rows=False,
                //                **kwargs)
                //
                //     column_names: array | dict | None
                //     row_names:    array | dict | None
                //     slug_columns: bool
                //     slug_rows:    bool
                let iter = ArgsIter::new("Table.rename", &[], args);
                let column_names = iter.next_kwarg::<Option<&Value>>("column_names")?;
                let row_names = iter.next_kwarg::<Option<&Value>>("row_names")?;
                let slug_columns = iter
                    .next_kwarg::<Option<bool>>("slug_columns")?
                    .unwrap_or(false);
                let slug_rows = iter
                    .next_kwarg::<Option<bool>>("slug_rows")?
                    .unwrap_or(false);
                let kwargs = iter.trailing_kwargs()?;

                let table = self.as_ref().rename(
                    column_names,
                    row_names,
                    slug_columns,
                    slug_rows,
                    kwargs,
                )?;
                Ok(Value::from_object(table))
            }
            other => unimplemented!("AgateTable::{}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::flat_record_batch::FlatRecordBatch;
    use crate::*;
    use arrow::array::{
        ArrayRef, BooleanBuilder, DictionaryArray, Float64Builder, Int32Array, Int32Builder,
        ListBuilder, StringBuilder, StructBuilder,
    };
    use arrow::array::{GenericListArray, StringArray};
    use arrow::csv::reader::ReaderBuilder;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::{Array, ListArray, RecordBatchOptions};
    use arrow_schema::Fields;
    use minijinja::Environment;
    use minijinja::value::ValueMap;
    use minijinja::value::mutable_map::MutableMap;
    use std::io;
    use std::sync::Arc;

    fn simple_record_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ]));
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(42), Some(43), Some(44)]));
        let country_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Brazil"),
            Some("USA"),
            Some("Canada"),
        ]));
        RecordBatch::try_new(schema, vec![id_array, country_array]).unwrap()
    }

    #[test]
    fn test_columns() {
        let batch = Arc::new(simple_record_batch());
        let table = Arc::new(AgateTable::from_record_batch(batch));

        // there are 2 columns
        let columns = table.columns();
        let values = columns.values();
        assert_eq!(values.len(), 2);

        let id = values.get(0).unwrap();
        let country = values.get(1).unwrap();

        let id = id.as_object().unwrap();
        let country = country.as_object().unwrap();

        // each column contains 3 values
        assert_eq!(id.enumerator_len().unwrap(), 3);
        assert_eq!(country.enumerator_len().unwrap(), 3);
    }

    #[test]
    fn test_rows() {
        let table = AgateTable::from_record_batch(Arc::new(simple_record_batch()));
        let rows = table.rows();
        let values = rows.values();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_agate_table_from_value() {
        let file = io::Cursor::new(
            "grantee,privilege_type\n\
 dbt_test_user_1,SELECT\n\
 dbt_test_user_2,SELECT\n\
 dbt_test_user_3,SELECT\n",
        );
        let csv_schema = Schema::new(vec![
            Field::new("grantee", DataType::Utf8, true),
            Field::new("privilege_type", DataType::Utf8, true),
        ]);
        let mut reader = ReaderBuilder::new(Arc::new(csv_schema))
            .with_header(true)
            .build(file)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let table = AgateTable::from_record_batch(Arc::new(batch));

        let table_value = Value::from_object(table);
        let downcasted = table_value.downcast_object::<AgateTable>().unwrap();
        assert_eq!(downcasted.num_columns(), 2);
        assert_eq!(downcasted.num_rows(), 3);
        let record_batch = downcasted.to_record_batch();
        assert_eq!(record_batch.num_columns(), 2);
        assert_eq!(record_batch.num_rows(), 3);
    }

    /// Create a nested record batch with different data types.
    ///
    /// NOTE: other tests may use a JSON->Arrow parser to create record batches more
    /// easily, but let's keep this one as an example on how to use builders to create
    /// record batches imperatively.
    ///
    /// The data in the record batch is what the following SQL would generate:
    ///
    /// ```sql
    /// INSERT INTO user_events (id, user_name, event_tags, event_meta, groups) VALUES
    ///   (1, 'alice',   ARRAY['login', 'mobile'],   '{"device": "iPhone", "success": true}',
    ///     ARRAY[
    ///       ARRAY[1, 2, 3],
    ///       ARRAY[4, 5],
    ///       ARRAY[6]
    ///     ]),
    ///   (2, 'bob',     ARRAY['purchase'],          '{"item_id": 1234, "amount": 49.99}',
    ///     ARRAY[
    ///       ARRAY[10, 20],
    ///       ARRAY[30, 40, 50],
    ///       ARRAY[60, 70],
    ///       ARRAY[80]
    ///     ]),
    ///   (3, 'charlie', ARRAY['logout', 'timeout'], '{"duration_sec": 300}',
    ///     ARRAY[
    ///       ARRAY[7],
    ///       NULL,
    ///       ARRAY[8, 9]
    ///     ]),
    ///   (4, 'dana',    ARRAY[]::TEXT[],            '{"device": "desktop"}',
    ///     ARRAY[]::INTEGER[][]),  -- Empty outer list
    ///   (5, 'eve',     NULL,                       '{"success": false}',
    ///     NULL)
    ///   );
    /// ```
    fn nested_record_batch() -> RecordBatch {
        const CAPACITY: usize = 5;
        // all the missing fields become NULL in the record batch
        let event_type_fields = Fields::from(vec![
            Field::new("device", DataType::Utf8, true),
            Field::new("item_id", DataType::Int32, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("duration_sec", DataType::Int32, true),
            Field::new("success", DataType::Boolean, true),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "event_tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                true,
            ),
            Field::new(
                "event_meta",
                DataType::Struct(event_type_fields.clone()),
                false,
            ),
            Field::new(
                "groups",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ))),
                true,
            ),
        ]));
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));
        let user_name_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("alice"),
            Some("bob"),
            Some("charlie"),
            Some("dana"),
            Some("eve"),
        ]));
        let event_tags_array = {
            let mut event_tags_builder = {
                let values_builder = StringBuilder::with_capacity(CAPACITY, CAPACITY * 10);
                ListBuilder::<StringBuilder>::with_capacity(values_builder, CAPACITY)
            };
            event_tags_builder.append_value(vec![Some("login"), Some("mobile")]);
            event_tags_builder.append_value(vec![Some("purchase")]);
            event_tags_builder.append_value(vec![Some("logout"), Some("timeout")]);
            event_tags_builder.append_value(Vec::<Option<String>>::new());
            event_tags_builder.append_null();

            let list_array = event_tags_builder.finish();
            // re-create the list array with a non-nullable field because finish()
            // doesn't let us specify the nullability of the list field
            let new_list_field = Field::new_list_field(
                list_array.values().data_type().clone(),
                false, // the values are non-nullable!
            );
            let event_tags_array = GenericListArray::new(
                Arc::new(new_list_field),
                list_array.offsets().clone(),
                list_array.values().clone(),
                None,
            );
            Arc::new(event_tags_array)
        };

        let events_array = {
            let mut event_builder = StructBuilder::from_fields(event_type_fields, CAPACITY);
            let mut append = |device: Option<&str>,
                              item_id: Option<i32>,
                              amount: Option<f64>,
                              duration_sec: Option<i32>,
                              success: Option<bool>| {
                event_builder
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_option(device.to_owned());
                event_builder
                    .field_builder::<Int32Builder>(1)
                    .unwrap()
                    .append_option(item_id);
                event_builder
                    .field_builder::<Float64Builder>(2)
                    .unwrap()
                    .append_option(amount);
                event_builder
                    .field_builder::<Int32Builder>(3)
                    .unwrap()
                    .append_option(duration_sec);
                event_builder
                    .field_builder::<BooleanBuilder>(4)
                    .unwrap()
                    .append_option(success);
                event_builder.append(true);
            };
            append(Some("iPhone"), None, None, None, Some(true));
            append(None, Some(1234), Some(49.99), None, None);
            append(None, None, None, Some(300), None);
            append(Some("Desktop"), None, None, None, None);
            append(None, None, None, None, Some(false));
            Arc::new(event_builder.finish())
        };

        let groups_array = {
            let mut groups_builder = {
                let inner_values_builder = Int32Builder::new();
                let inner_list_builder = ListBuilder::<Int32Builder>::new(inner_values_builder);
                ListBuilder::<ListBuilder<Int32Builder>>::with_capacity(
                    inner_list_builder,
                    CAPACITY,
                )
            };
            let inner_list = groups_builder.values();
            inner_list.append_value(vec![Some(1), Some(2), Some(3)]);
            inner_list.append_value(vec![Some(4), Some(5)]);
            inner_list.append_value(vec![Some(6)]);
            groups_builder.append(true); // groups 0

            let inner_list = groups_builder.values();
            inner_list.append_value(vec![Some(10), Some(20)]);
            inner_list.append_value(vec![Some(30), Some(40), Some(50)]);
            inner_list.append_value(vec![Some(60), Some(70)]);
            inner_list.append_value(vec![Some(80)]);
            groups_builder.append(true); // groups 1

            let inner_list = groups_builder.values();
            inner_list.append_value(vec![Some(7)]);
            inner_list.append_null();
            inner_list.append_value(vec![Some(8), Some(9)]);
            groups_builder.append(true); // groups 2

            // []   -- Empty list of groups (non-NULL)
            groups_builder.append(true); // groups 3

            // NULL -- Null list of groups
            groups_builder.append(false); // groups 4

            Arc::new(groups_builder.finish())
        };

        let columns = vec![
            id_array,
            user_name_array,
            event_tags_array,
            events_array,
            groups_array,
        ];
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_record_batch_flattening() {
        let batch = nested_record_batch();
        let _batch = FlatRecordBatch::try_new(Arc::new(batch)).unwrap();
        // TODO(felipcrv); implement CSV serialization to assert here
    }

    /// Take a 5-element column and make a dictionary-encoded version of it
    /// using the first two elements as dictionary values.
    fn dict_encoded_example(col: &ArrayRef) -> ArrayRef {
        let dictionary_values = col.slice(0, 2);
        let indices_array = Int32Array::from(vec![Some(0), Some(1), Some(0), Some(1), Some(0)]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(indices_array, dictionary_values).unwrap();
        Arc::new(dict_array) as ArrayRef
    }

    fn batch_with_replaced_column(
        batch: &RecordBatch,
        col_idx: usize,
        new_col: ArrayRef,
    ) -> RecordBatch {
        let new_columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if i == col_idx {
                    new_col.clone()
                } else {
                    col.clone()
                }
            })
            .collect::<Vec<_>>();
        let new_schema = {
            let old_schema = batch.schema();
            let fields = new_columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    old_schema
                        .field(i)
                        .clone()
                        .with_data_type(col.data_type().clone())
                })
                .collect::<Vec<_>>();
            Arc::new(Schema::new(fields))
        };
        RecordBatch::try_new(new_schema, new_columns).unwrap()
    }

    #[test]
    fn test_record_batch_flattening_with_dict_encoded_struct() {
        let batch = nested_record_batch();

        let event_meta = batch.column(3);
        assert!(matches!(event_meta.data_type(), DataType::Struct(_)));

        // Build a dictionary-encoded version of the "events" column
        let dict_event_meta = dict_encoded_example(event_meta);

        let new_batch = batch_with_replaced_column(&batch, 3, dict_event_meta);
        let _flat_batch = FlatRecordBatch::try_new(Arc::new(new_batch)).unwrap();
        // TODO(felipcrv); implement CSV serialization to assert here
    }

    #[test]
    fn test_record_batch_flattening_with_dict_encoded_list() {
        let batch = nested_record_batch();

        let event_tags = batch.column(2);
        assert!(matches!(event_tags.data_type(), DataType::List(_)));

        // Build a dictionary-encoded version of the "event_tags" column
        let dict_event_tags = dict_encoded_example(event_tags);

        let new_batch = batch_with_replaced_column(&batch, 2, dict_event_tags);
        let _flat_batch = FlatRecordBatch::try_new(Arc::new(new_batch)).unwrap();
        // TODO(felipcrv); implement CSV serialization to assert here
    }

    #[test]
    fn test_record_batch_flattening_with_nested_dict_encoded() {
        let batch = nested_record_batch();

        let event_meta = batch.column(3);
        assert!(matches!(event_meta.data_type(), DataType::Struct(_)));

        // Build a dictionary-encoded version of the "events" column...
        let dict_event_meta = dict_encoded_example(event_meta);
        // ...and dictionary-encode it again.
        let dict_dict_event_meta = dict_encoded_example(&dict_event_meta);

        let new_batch = batch_with_replaced_column(&batch, 3, dict_dict_event_meta);
        let _flat_batch = FlatRecordBatch::try_new(Arc::new(new_batch)).unwrap();
        // TODO(felipcrv); implement CSV serialization to assert here
    }

    #[test]
    fn test_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "event_tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                true,
            ),
        ]));
        let opts = RecordBatchOptions::default().with_row_count(Some(0));
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![
                Arc::new(Int32Array::new_null(0)) as ArrayRef,
                Arc::new(StringArray::new_null(0)) as ArrayRef,
                Arc::new(ListArray::new_null(
                    Arc::new(Field::new_list_field(
                        DataType::Utf8,
                        false, // the values are non-nullable!
                    )),
                    0,
                )) as ArrayRef,
            ],
            &opts,
        )
        .unwrap();
        let table = AgateTable::from_record_batch(Arc::new(batch));
        let column_names = table.column_names();
        assert_eq!(column_names, vec!["id", "name", "event_tags.0"]);
    }

    #[test]
    fn test_column_renaming() {
        let batch = Arc::new(nested_record_batch());
        let agate_table = AgateTable::from_record_batch(batch);
        let col_names = agate_table.column_names();
        let table = Value::from_object(agate_table);

        let env = Environment::new();
        let state = env.empty_state();
        let rename = |table: &Value, args: &[Value]| -> Result<Value, MinijinjaError> {
            table.call_method(&state, "rename", args, &[])
        };

        // Original column names:
        //   "id", "name", "event_tags.0", "event_tags.1", "event_meta/device",
        //   "event_meta/item_id", "event_meta/amount", "event_meta/duration_sec",
        //   "event_meta/success", "groups.0", "groups.1", "groups.2", "groups.3"

        // Renaming with a map
        let map = ValueMap::from_iter([
            ("groups.0.0".into(), "first_group_cell".into()),
            ("groups.1.0".into(), "second_group_cell".into()),
            ("groups.2.0".into(), "third_group_cell".into()),
            ("groups.3.0".into(), "fourth_group_cell".into()),
            ("nonexistent".into(), "should_not_exist".into()),
        ]);
        let new_names = rename(&table, &[Value::from_object(map.clone())])
            .unwrap()
            .downcast_object::<AgateTable>()
            .unwrap()
            .column_names();
        assert_eq!(
            new_names[0..new_names.len() - 12],
            col_names[0..col_names.len() - 12]
        );
        static EXPECTED_12_LAST_COL_NAMES: [&str; 12] = [
            "first_group_cell",
            "groups.0.1",
            "groups.0.2",
            "second_group_cell",
            "groups.1.1",
            "groups.1.2",
            "third_group_cell",
            "groups.2.1",
            "groups.2.2",
            "fourth_group_cell",
            "groups.3.1",
            "groups.3.2",
        ];
        assert_eq!(
            new_names[new_names.len() - 12..],
            EXPECTED_12_LAST_COL_NAMES
        );

        // Renaming with a mutable map
        let map: MutableMap = map.into();
        let new_names = rename(&table, &[Value::from_object(map)])
            .unwrap()
            .downcast_object::<AgateTable>()
            .unwrap()
            .column_names();
        assert_eq!(
            new_names[0..new_names.len() - 12],
            col_names[0..col_names.len() - 12]
        );
        assert_eq!(
            new_names[new_names.len() - 12..],
            EXPECTED_12_LAST_COL_NAMES
        );

        // Renaming with an array
        let array = {
            let mut array = col_names[0..col_names.len() - 12].to_vec();
            array.extend_from_slice(EXPECTED_12_LAST_COL_NAMES.map(|s| s.to_string()).as_slice());
            Value::from_object(array)
        };
        let new_names = rename(&table, &[array])
            .unwrap()
            .downcast_object::<AgateTable>()
            .unwrap()
            .column_names();
        assert_eq!(
            new_names[0..new_names.len() - 12],
            col_names[0..col_names.len() - 12]
        );
        assert_eq!(
            new_names[new_names.len() - 12..],
            EXPECTED_12_LAST_COL_NAMES
        );
    }
}
