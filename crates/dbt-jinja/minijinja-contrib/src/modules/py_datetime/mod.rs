use std::collections::BTreeMap;

use minijinja::Value;

pub mod date;
pub mod datetime;
pub mod time;
pub mod timedelta;
pub mod tzinfo;

pub fn create_datetime_module() -> BTreeMap<String, Value> {
    let mut datetime_module = BTreeMap::new();

    datetime_module.insert(
        "datetime".to_string(),
        Value::from_object(datetime::PyDateTimeClass),
    );
    datetime_module.insert("date".to_string(), Value::from_object(date::PyDateClass));
    datetime_module.insert("time".to_string(), Value::from_object(time::PyTimeClass));
    datetime_module.insert(
        "timedelta".to_string(),
        Value::from_object(timedelta::PyTimeDeltaClass),
    );
    datetime_module.insert(
        "tzinfo".to_string(),
        Value::from_object(tzinfo::PyTzInfoClass),
    );
    datetime_module
}
