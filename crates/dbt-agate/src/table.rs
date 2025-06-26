use crate::column::Column;
use crate::columns::ColumnNamesAsTuple;
use crate::columns::Columns;
use crate::converters::make_array_converter;
use crate::flat_record_batch::FlatRecordBatch;
use crate::print_table::print_table;
use crate::row::Row;
use crate::rows::Rows;
use crate::vec_of_rows::VecOfRows;
use crate::Tuple;

use arrow::record_batch::RecordBatch;
use arrow_schema::{ArrowError, Schema};
use minijinja::arg_utils::ArgsIter;
use minijinja::listener::RenderingEventListener;
use minijinja::value::Kwargs;
use minijinja::value::{Enumerator, Object};
use minijinja::Value;
use minijinja::{Error as MinijinjaError, State};
use std::rc::Rc;
use std::sync::Arc;

/// Internal table representation.
///
/// An AgateTable can be internally represented as either an Arrow RecordBatch or
/// a vector of Jinja objects -- one per row.
///
/// Both representations are immutable, so they can be reference-counted and shared
/// without copying.
#[derive(Debug)]
pub(crate) enum TableRepr {
    /// Arrow representation of the table.
    Arrow(FlatRecordBatch),
    /// RowTable representation of the table.
    RowTable(Arc<VecOfRows>),
}

impl Clone for TableRepr {
    /// Clone the TableRepr by cloning one Arc pointer.
    fn clone(&self) -> Self {
        match self {
            TableRepr::Arrow(batch) => TableRepr::Arrow(batch.clone()),
            TableRepr::RowTable(table) => TableRepr::RowTable(table.clone()),
        }
    }
}

impl TableRepr {
    /// Force the table to be represented as a RowTable.
    ///
    /// This is useful because we don't want to, at first, implement all the functions
    /// against the Arrow-based table representation. Instead, we can implement them
    /// against the RowTable representation and incrementally migrate the implementations
    /// to also support Arrow later.
    pub fn force_row_table(&self) -> Result<TableRepr, ArrowError> {
        match &self {
            TableRepr::Arrow(batch) => {
                let vec_of_rows = Arc::new(VecOfRows::from_flat_record_batch(batch.clone())?);
                let row_table = TableRepr::RowTable(vec_of_rows);
                Ok(row_table)
            }
            TableRepr::RowTable(_) => Ok(self.clone()),
        }
    }

    pub fn to_record_batch(&self) -> Arc<RecordBatch> {
        match self {
            TableRepr::Arrow(batch) => batch.flat().clone(),
            TableRepr::RowTable(vec_of_rows) => vec_of_rows.to_record_batch(),
        }
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
        match self {
            TableRepr::Arrow(batch) => batch.num_columns(),
            TableRepr::RowTable(table) => table.num_columns(),
        }
    }

    pub fn get_column(&self, idx: isize) -> Option<Column> {
        let idx = self.adjusted_column_index(idx)?;
        let col = Column::new(idx, self.clone());
        Some(col)
    }

    pub fn column_name(&self, idx: isize) -> Option<String> {
        let idx = self.adjusted_column_index(idx)?;
        match self {
            TableRepr::Arrow(batch) => {
                let name = batch.schema().field(idx).name().clone();
                Some(name)
            }
            TableRepr::RowTable(table) => table.schema().get(idx).cloned(),
        }
    }

    pub fn columns(&self) -> Columns {
        Columns::new(self.clone())
    }

    pub fn column_names(&self) -> Vec<String> {
        match &self {
            TableRepr::Arrow(batch) => batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            TableRepr::RowTable(row_table) => row_table.schema().to_vec(),
        }
    }

    pub fn single_column_table(&self, idx: isize) -> Option<TableRepr> {
        let idx = self.adjusted_column_index(idx)?;
        match self {
            TableRepr::Arrow(batch) => {
                let repr = TableRepr::Arrow(batch.with_single_column(idx));
                Some(repr)
            }
            TableRepr::RowTable(vec_of_rows) => {
                let result = vec_of_rows.with_single_column(idx);
                debug_assert!(
                    result.is_ok(),
                    "Unexpected error: {}",
                    result.err().unwrap()
                );
                let repr = TableRepr::RowTable(Arc::new(result.unwrap()));
                Some(repr)
            }
        }
    }

    /// Return a single-column table with the distinct values in this column.
    pub fn column_distinct(&self, col_idx: isize) -> Self {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!()
    }

    pub fn column_without_nulls(&self, col_idx: isize) -> Self {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!()
    }

    pub fn column_sorted(&self, col_idx: isize) -> Self {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!()
    }

    pub fn column_without_nulls_sorted(&self, col_idx: isize) -> Self {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!()
    }

    pub fn count_occurrences_of_value_in_column(&self, _needle: &Value, col_idx: isize) -> usize {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!()
    }

    pub fn index_of_value_in_column(&self, _needle: &Value, col_idx: isize) -> Option<usize> {
        let _col = self.single_column_table(col_idx).unwrap();
        todo!()
    }

    // Rows -------------------------------------------------------------------

    pub fn num_rows(&self) -> usize {
        match &self {
            TableRepr::Arrow(batch) => batch.num_rows(),
            TableRepr::RowTable(vec_of_rows) => vec_of_rows.rows().len(),
        }
    }

    pub fn row_by_index(&self, idx: isize) -> Option<Value> {
        self.adjusted_row_index(idx).map(|i| {
            let row = Row::new(i, self.clone());
            Value::from_object(row)
        })
    }

    pub fn rows(&self) -> Rows {
        match self {
            TableRepr::Arrow(batch) => {
                let repr = TableRepr::Arrow((*batch).clone());
                Rows::new(repr)
            }
            TableRepr::RowTable(vec_of_rows) => {
                let repr = TableRepr::RowTable(vec_of_rows.clone());
                Rows::new(repr)
            }
        }
    }

    pub fn count_occurrences_of_row(&self, _needle: &Value) -> usize {
        todo!()
    }

    pub fn index_of_row(&self, _needle: &Value) -> Option<usize> {
        todo!()
    }

    pub fn count_occurrences_of_value_in_row(&self, _needle: &Value, row_idx: isize) -> usize {
        let _row = self.row_by_index(row_idx).unwrap();
        todo!()
    }

    pub fn index_of_value_in_row(&self, _needle: &Value, row_idx: isize) -> Option<usize> {
        let _row = self.row_by_index(row_idx).unwrap();
        todo!()
    }

    // Cells ------------------------------------------------------------------

    pub fn cell(&self, row_idx: isize, col_idx: isize) -> Option<Value> {
        let row_idx = self.adjusted_row_index(row_idx)?;
        let col_idx = self.adjusted_column_index(col_idx)?;
        match self {
            TableRepr::Arrow(batch) => {
                let column = batch.column(col_idx);
                make_array_converter(column)
                    .map(|converter| converter.to_value(row_idx))
                    .map_err(|e| {
                        debug_assert!(false, "Unexpected Arrow error: {e}");
                        e
                    })
                    .ok()
            }
            TableRepr::RowTable(vec_of_rows) => {
                let row: &Value = vec_of_rows.rows().get(row_idx)?;
                match row.get_item_by_index(col_idx) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        debug_assert!(false, "Unexpected error: {e}");
                        None
                    }
                }
            }
        }
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
    repr: TableRepr,
}

impl AgateTable {
    pub(crate) fn new(repr: TableRepr) -> Self {
        Self { repr }
    }

    /// Create an AgateTable from an Arrow RecordBatch.
    pub fn from_record_batch(batch: Arc<RecordBatch>) -> Self {
        let flat = FlatRecordBatch::new(batch);
        let repr = TableRepr::Arrow(flat);
        Self::new(repr)
    }

    /// Create an AgateTable from column names and rows.
    ///
    /// Each row is a single minijiinja::Value that contains the row data.
    pub fn from_rows(column_names: Vec<String>, rows: Vec<Value>) -> Self {
        let vec_of_rows = VecOfRows::new(column_names, rows);
        let repr = TableRepr::RowTable(Arc::new(vec_of_rows));
        Self::new(repr)
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
    pub fn column_name(&self, idx: isize) -> Option<String> {
        self.repr.column_name(idx)
    }

    /// Get the column names.
    pub fn column_names(&self) -> Vec<String> {
        self.repr.column_names()
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
                let mut iter = ArgsIter::new("Table.print_table", 0, args);
                let mut max_rows: Option<&Value> = None;
                let mut max_columns: Option<&Value> = None;
                // output is not implemented yet
                let mut max_column_width: Option<&Value> = None;
                // locale is not implemented yet
                // max_precision is not implemented yet
                if let Some(arg) = iter.next() {
                    max_rows.replace(arg?);
                    if let Some(arg) = iter.next() {
                        max_columns.replace(arg?);
                        if let Some(_) = iter.next() {
                            // output is not implemented yet
                            if let Some(arg) = iter.next() {
                                max_column_width.replace(arg?);
                                if let Some(_) = iter.next() {
                                    // locale is not implemented yet
                                    if let Some(_) = iter.next() {
                                        // max_precision is not implemented yet
                                    }
                                }
                            }
                        }
                    }
                }
                let kwargs = iter.trailing_kwargs()?;

                let max_rows = max_rows
                    .and_then(|v| v.as_i64()) // XXX: silently falling back to 20 rows on conversion error
                    .or(kwargs.get::<Option<i64>>("max_rows")?)
                    .unwrap_or(20) as usize;
                let max_columns = max_columns
                    .and_then(|v| v.as_i64())
                    .or(kwargs.get::<Option<i64>>("max_columns")?)
                    .unwrap_or(6) as usize;
                let _output = kwargs.get::<Option<&Value>>("output")?;
                let max_column_width = max_column_width
                    .and_then(|v| v.as_i64())
                    .or(kwargs.get::<Option<i64>>("max_column_width")?)
                    .unwrap_or(20) as usize;
                let _locale = kwargs.get::<Option<&Value>>("locale")?;
                let _max_precision = kwargs.get::<Option<&Value>>("max_precision")?;
                kwargs.assert_all_used()?;

                print_table(self, max_rows, max_columns, max_column_width)
            }
            other => unimplemented!("{}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::flat_record_batch::FlatRecordBatch;
    use crate::*;
    use arrow::array::{
        ArrayRef, BooleanBuilder, Float64Builder, Int32Array, Int32Builder, ListBuilder,
        StringBuilder, StructBuilder,
    };
    use arrow::array::{GenericListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::Fields;
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
    #[ignore = "https://github.com/dbt-labs/fs/issues/1887"]
    fn test_agate_table_from_value() {
        let table = AgateTable::from_rows(
            vec!["grantee".to_string(), "privilege_type".to_string()],
            vec![
                Value::from(vec!["dbt_test_user_1".to_string(), "SELECT".to_string()]),
                Value::from(vec!["dbt_test_user_2".to_string(), "SELECT".to_string()]),
                Value::from(vec!["dbt_test_user_3".to_string(), "SELECT".to_string()]),
            ],
        );
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
        let _batch = FlatRecordBatch::new(Arc::new(batch));
        // TODO(felipcrv); implement CSV serialization to assert here
    }
}
