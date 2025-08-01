use serde::Serialize;
use tracing::{Event, field::ValueSet, span::Record};

pub(super) enum Recordable<'a> {
    ValueSet(&'a ValueSet<'a>),
    Record(&'a Record<'a>),
    Event(&'a Event<'a>),
}

impl<'a> From<&'a ValueSet<'a>> for Recordable<'a> {
    fn from(value: &'a ValueSet<'a>) -> Self {
        Recordable::ValueSet(value)
    }
}

impl<'a> From<&'a Record<'a>> for Recordable<'a> {
    fn from(value: &'a Record<'a>) -> Self {
        Recordable::Record(value)
    }
}

impl<'a> From<&'a Event<'a>> for Recordable<'a> {
    fn from(value: &'a Event<'a>) -> Self {
        Recordable::Event(value)
    }
}

impl<'a> Recordable<'a> {
    pub fn record(&self, visitor: &mut dyn tracing::field::Visit) {
        match self {
            Recordable::ValueSet(values) => values.record(visitor),
            Recordable::Record(record) => record.record(visitor),
            Recordable::Event(event) => event.record(visitor),
        }
    }
}

/// Tracing does not allow implementing `Value` directly, so we use this trait
/// to convert types to a `tracing::field::Value`.
pub trait ToTracingValue {
    fn to_tracing_value(self) -> impl tracing::field::Value + Send + Sync;
}

impl<T> ToTracingValue for T
where
    T: Serialize,
{
    fn to_tracing_value(self) -> impl tracing::field::Value + Send + Sync {
        // Serialize the value to a string and return it as a tracing value.
        serde_json::to_string(&self).expect("Failed to serialize value")
    }
}
