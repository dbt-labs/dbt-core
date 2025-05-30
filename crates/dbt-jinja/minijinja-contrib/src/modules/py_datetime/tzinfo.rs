use minijinja::{value::Object, Error, ErrorKind, Value};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct PyTzInfoClass;

impl PyTzInfoClass {
    fn utc(_args: &[Value]) -> Result<Value, Error> {
        Ok(Value::from("TODO: implement tz_utc"))
    }

    fn local(_args: &[Value]) -> Result<Value, Error> {
        Ok(Value::from("TODO: implement tz_local"))
    }

    fn timezone(_args: &[Value]) -> Result<Value, Error> {
        Ok(Value::from("TODO: implement tz_timezone"))
    }

    fn available_timezones(_args: &[Value]) -> Result<Value, Error> {
        Ok(Value::from("TODO: implement available_timezones"))
    }
}

impl Object for PyTzInfoClass {
    // If someone does: {{ tzinfo(...) }} in templates, you can either
    //  1) forbid that call, or
    //  2) treat it as a constructor. Here we just error out.
    fn call(
        self: &Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        _args: &[Value],
        _listener: Rc<dyn minijinja::listener::RenderingEventListener>,
    ) -> Result<Value, Error> {
        Err(Error::new(
            ErrorKind::InvalidOperation,
            "tzinfo(...) is not callable; use tzinfo.utc(), tzinfo.local(), etc.",
        ))
    }

    // If someone does: {{ tzinfo.method(...) }} for one of the known methods,
    // we can return the appropriate function reference here.
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "utc" => Some(Value::from_function(Self::utc)),
            "local" => Some(Value::from_function(Self::local)),
            "timezone" => Some(Value::from_function(Self::timezone)),
            "available_timezones" => Some(Value::from_function(Self::available_timezones)),

            // Or if you want to provide some constants:
            "UTC" => Some(Value::from("UTC")),
            "EST" => Some(Value::from("America/New_York")),
            "PST" => Some(Value::from("America/Los_Angeles")),

            _ => None,
        }
    }

    // If someone directly renders {{ tzinfo }}, we can produce a placeholder
    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<tzinfo module: TODO: implement methods>")
    }
}
