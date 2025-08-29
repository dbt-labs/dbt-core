use core::fmt;
use std::rc::Rc;
use std::sync::Arc;

use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object, ObjectRepr};
use minijinja::{Error as MinijinjaError, State, Value};

use crate::table::TableRepr;
use crate::{MappedSequence, Tuple, TupleRepr, ZippedTupleRepr};

#[derive(Debug)]
pub(crate) struct ColumnNamesAsTuple {
    names: Vec<String>,
}

impl ColumnNamesAsTuple {
    pub fn new(names: Vec<String>) -> Self {
        Self { names }
    }

    pub fn of_table(table: &Arc<TableRepr>) -> Box<dyn TupleRepr> {
        let column_names = table
            .column_names()
            .map(|name| name.to_owned())
            .collect::<Vec<_>>();
        // NOTE: a dedicated TupleRepr could be used here to avoid the intermediate Vec<String>
        // of column names, but it would require more work to implement and it's not worth it (yet).
        let repr = ColumnNamesAsTuple::new(column_names);
        Box::new(repr)
    }
}

impl TupleRepr for ColumnNamesAsTuple {
    fn get_item_by_index(&self, idx: isize) -> Option<Value> {
        let idx = {
            if idx < 0 {
                self.names.len() as isize + idx
            } else {
                idx
            }
        };
        self.names
            .get(idx as usize)
            .map(|name| Value::from(name.clone()))
    }

    fn len(&self) -> usize {
        self.names.len()
    }

    fn count_occurrences_of(&self, needle: &Value) -> usize {
        if let Some(name) = needle.as_str() {
            self.names.iter().filter(|n| n == &name).count()
        } else {
            0
        }
    }

    fn index_of(&self, needle: &Value) -> Option<usize> {
        if let Some(name) = needle.as_str() {
            self.names.iter().position(|n| n == name)
        } else {
            None
        }
    }

    fn clone_repr(&self) -> Box<dyn TupleRepr> {
        Box::new(ColumnNamesAsTuple {
            names: self.names.clone(),
        })
    }
}

#[derive(Debug)]
struct ColumnsAsTuple {
    of_table: Arc<TableRepr>,
}

impl TupleRepr for ColumnsAsTuple {
    fn get_item_by_index(&self, idx: isize) -> Option<Value> {
        let column = self.of_table.get_column(idx)?;
        Some(Value::from_object(column))
    }

    fn len(&self) -> usize {
        self.of_table.num_columns()
    }

    fn count_occurrences_of(&self, _needle: &Value) -> usize {
        todo!("ColumnsAsTuple::count_occurrences_of")
    }

    fn index_of(&self, _needle: &Value) -> Option<usize> {
        todo!("ColumnsAsTuple::index_of")
    }

    fn clone_repr(&self) -> Box<dyn TupleRepr> {
        Box::new(ColumnsAsTuple {
            of_table: Arc::clone(&self.of_table),
        })
    }
}

/// Represents an instance of a `MappedSequence` populated by a list of columns.
///
/// https://github.com/wireservice/agate/blob/7023e35b51e8abfe9784fe292a23dd4d7d983c63/agate/table/__init__.py#L181
#[derive(Debug)]
pub struct Columns {
    /// Internal representation of the columns sequence is the table representation itself.
    of_table: Arc<TableRepr>,
}

impl Columns {
    pub(crate) fn new(of_table: Arc<TableRepr>) -> Self {
        Self { of_table }
    }
}

impl MappedSequence for Columns {
    fn values(&self) -> Tuple {
        let columns = ColumnsAsTuple {
            of_table: Arc::clone(&self.of_table),
        };
        let repr = Box::new(columns);
        Tuple(repr)
    }

    fn keys(&self) -> Option<Tuple> {
        let column_names = ColumnNamesAsTuple::of_table(&self.of_table);
        Some(Tuple(column_names))
    }

    fn items(&self) -> Option<Tuple> {
        let column_names = ColumnNamesAsTuple::of_table(&self.of_table);
        let columns = ColumnsAsTuple {
            of_table: Arc::clone(&self.of_table),
        };
        let zipped = ZippedTupleRepr::new(column_names, Box::new(columns));
        let items = Tuple(Box::new(zipped));
        Some(items)
    }
}

impl Object for Columns {
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
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        MappedSequence::call_method(self, state, name, args, listeners)
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        MappedSequence::render(self, f)
    }
}
