use core::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::table::TableRepr;
use crate::{MappedSequence, Tuple, TupleRepr};
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object, ObjectRepr};
use minijinja::{Error as MinijinjaError, State, Value};

#[derive(Debug)]
struct RowsAsTuple {
    of_table: TableRepr,
}

impl TupleRepr for RowsAsTuple {
    fn get_item_by_index(&self, idx: isize) -> Option<Value> {
        self.of_table.row_by_index(idx)
    }

    fn len(&self) -> usize {
        self.of_table.num_rows()
    }

    fn count_occurrences_of(&self, value: &Value) -> usize {
        self.of_table.count_occurrences_of_row(value)
    }

    fn index_of(&self, value: &Value) -> Option<usize> {
        self.of_table.index_of_row(value)
    }

    fn clone_repr(&self) -> Box<dyn TupleRepr> {
        Box::new(RowsAsTuple {
            of_table: self.of_table.clone(),
        })
    }
}

/// Iterator for Rows that maintains its own cursor state.
#[derive(Debug)]
pub struct RowsIterator {
    of_table: TableRepr,
    cursor: usize,
}

impl IntoIterator for Rows {
    type Item = Value;
    type IntoIter = RowsIterator;

    fn into_iter(self) -> Self::IntoIter {
        // Since we know the caller is about to iterate over the rows, we can
        // force the table to be represented as a RowTable and avoid the overhead
        // of casting on every row access.
        let row_table = self.of_table.force_row_table().ok();
        RowsIterator {
            of_table: row_table.unwrap_or_else(|| self.of_table.clone()),
            cursor: 0,
        }
    }
}

impl Iterator for RowsIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.of_table.num_rows() {
            let row = self.of_table.row_by_index(self.cursor as isize);
            self.cursor += 1;
            row
        } else {
            None
        }
    }
}

/// Represents an instance of a `MappedSequence` populated by a list of rows.
///
/// https://github.com/wireservice/agate/blob/7023e35b51e8abfe9784fe292a23dd4d7d983c63/agate/table/__init__.py#L168
#[derive(Debug)]
pub struct Rows {
    /// Internal representation of the list of rows is the table representation itself.
    of_table: TableRepr,
}

impl Rows {
    pub(crate) fn new(table: TableRepr) -> Self {
        Self { of_table: table }
    }
}

impl MappedSequence for Rows {
    fn values(&self) -> Tuple {
        let rows = RowsAsTuple {
            of_table: self.of_table.clone(),
        };
        let repr = Box::new(rows);
        Tuple(repr)
    }

    fn keys(&self) -> Option<Tuple> {
        // TODO(felipecrv): implement row_names logic
        // See https://github.com/wireservice/agate/blob/7023e35b51e8abfe9784fe292a23dd4d7d983c63/agate/table/__init__.py#L144
        todo!()
    }
}

impl Object for Rows {
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
        state: &State,
        name: &str,
        args: &[Value],
        listener: Rc<dyn RenderingEventListener>,
    ) -> Result<Value, MinijinjaError> {
        MappedSequence::call_method(self, state, name, args, listener)
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        MappedSequence::render(self, f)
    }
}
