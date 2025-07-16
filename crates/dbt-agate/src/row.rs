use crate::columns::ColumnNamesAsTuple;
use crate::table::TableRepr;
use crate::{MappedSequence, Tuple, TupleRepr};
use minijinja::Value;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object, ObjectRepr};
use minijinja::{Error as MinijinjaError, State};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug)]
pub struct RowAsTuple {
    /// Always-valid index into the table's rows.
    index: usize,
    of_table: TableRepr,
}

impl TupleRepr for RowAsTuple {
    fn get_item_by_index(&self, col_idx: isize) -> Option<Value> {
        self.of_table.cell(self.index as isize, col_idx)
    }

    fn len(&self) -> usize {
        self.of_table.num_columns()
    }

    fn count_occurrences_of(&self, value: &Value) -> usize {
        self.of_table
            .count_occurrences_of_value_in_row(value, self.index as isize)
    }

    fn index_of(&self, value: &Value) -> Option<usize> {
        self.of_table
            .index_of_value_in_row(value, self.index as isize)
    }

    fn clone_repr(&self) -> Box<dyn TupleRepr> {
        Box::new(RowAsTuple {
            index: self.index,
            of_table: self.of_table.clone(),
        })
    }
}

/// A row from an Agate table.
///
/// https://agate.readthedocs.io/en/latest/api/columns_and_rows.html#agate.Row
#[derive(Debug)]
pub struct Row {
    /// Always-valid index into the table's rows.
    index: usize,
    /// Internal representation of the table that contains this row.
    of_table: TableRepr,
}

impl Row {
    /// Create a new row from an index and a table.
    pub(crate) fn new(index: usize, of_table: TableRepr) -> Self {
        Self { index, of_table }
    }
}

impl MappedSequence for Row {
    fn type_name(&self) -> &str {
        "Row"
    }

    fn values(&self) -> Tuple {
        let row = RowAsTuple {
            index: self.index,
            of_table: self.of_table.clone(),
        };
        let repr = Box::new(row);
        Tuple(repr)
    }

    fn keys(&self) -> Option<Tuple> {
        let column_names = self.of_table.column_names();
        let repr = ColumnNamesAsTuple::new(column_names);
        Some(Tuple(Box::new(repr)))
    }
}

impl Object for Row {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        MappedSequence::repr(self)
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        MappedSequence::get_value(self, key)
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        MappedSequence::enumerate(self)
    }

    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        MappedSequence::call_method(self, state, method, args, listeners)
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        MappedSequence::render(self, f)
    }
}
