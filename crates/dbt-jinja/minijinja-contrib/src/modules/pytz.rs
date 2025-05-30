use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use minijinja::{Error, ErrorKind, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// A Python-like "pytz" timezone object that wraps a `chrono_tz::Tz`.
#[derive(Debug, Clone)]
pub struct PytzTimezone {
    pub tz: Tz,
}

impl PytzTimezone {
    /// Constructor that simply stores a chrono_tz::Tz
    pub fn new(tz: Tz) -> Self {
        PytzTimezone { tz }
    }

    /// Convert a naive local datetime to a `DateTime<Tz>`, returning `None` if it's invalid/ambiguous.
    pub fn from_local(&self, naive: &NaiveDateTime) -> Option<DateTime<Tz>> {
        match self.tz.from_local_datetime(naive) {
            LocalResult::None => None,
            LocalResult::Single(dt) => Some(dt),
            LocalResult::Ambiguous(_earliest, _latest) => {
                // Decide how you want to handle an ambiguous time (DST overlap).
                // For demonstration, we'll just return None to indicate "ambiguous."
                None
            }
        }
    }

    /// Convert a naive UTC datetime to a `DateTime<Tz>`, returning `None` if invalid.
    pub fn from_utc(&self, naive_utc: &NaiveDateTime) -> DateTime<Tz> {
        self.tz.from_utc_datetime(naive_utc)
    }

    /// Return an offset from UTC as a `chrono::Duration` for the given local DateTime in this tz.
    pub fn utcoffset(&self, _local_dt: &DateTime<Tz>) -> Value {
        // Get the seconds offset directly from the datetime's offset
        Value::from("TODO IMPLEMENT")
    }

    /// Return the DST offset if any. If you just want to return None or 0, do so.
    pub fn dst(&self, _local_dt: &DateTime<Tz>) -> Option<chrono::Duration> {
        // Real DST logic is more nuanced, and Chrono doesn't directly expose it.
        // If you prefer to always return None or Some(0), do so:
        None
    }

    /// Return the timezone name for the given local DateTime. This might be "EST", "EDT", or a raw offset, etc.
    pub fn tzname(&self, local_dt: &DateTime<Tz>) -> String {
        local_dt.offset().to_string()
    }
}

/// Build the "pytz" module namespace for your Jinja environment.
///   e.g. `pytz.timezone("America/New_York")` => PytzTimezone
pub fn create_pytz_namespace() -> BTreeMap<String, Value> {
    let mut pytz_module = BTreeMap::new();
    // Register `timezone(...)`
    pytz_module.insert("timezone".to_string(), Value::from_function(timezone));
    pytz_module.insert(
        "utc".to_string(),
        Value::from_object(PytzTimezone::new(chrono_tz::UTC)),
    );
    // You could also add "utc", "country", etc.

    pytz_module
}

/// The top-level function for `pytz.timezone("America/New_York")`.
fn timezone(args: &[Value]) -> Result<Value, Error> {
    // Must provide a string zone name.
    let tz_name = args.first().and_then(|v| v.as_str()).ok_or_else(|| {
        Error::new(
            ErrorKind::MissingArgument,
            "timezone() requires a string name argument",
        )
    })?;

    // Try to parse it as a Chrono Tz.
    match Tz::from_str(tz_name) {
        Ok(tz) => Ok(Value::from_object(PytzTimezone::new(tz))),
        Err(_) => Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("Invalid timezone name: {}", tz_name),
        )),
    }
}

/// If you want your `PytzTimezone` to also be a minijinja Object for calls like
///   {{ pytz_obj.method(...) }}
/// implement `Object`.
impl minijinja::value::Object for PytzTimezone {
    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        method: &str,
        _args: &[Value],
        _listener: std::rc::Rc<dyn minijinja::listener::RenderingEventListener>,
    ) -> Result<Value, Error> {
        // For example, if you want a "localize" method:
        match method {
            "localize" => Ok(Value::from("TODO: implement localize()")),
            _ => Err(Error::new(
                ErrorKind::UnknownMethod("PytzTimeZone".to_string(), method.to_string()),
                format!("Timezone object has no method named '{}'", method),
            )),
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If you do {{ tz_obj }} in Jinja, it prints out the name
        write!(f, "{}", self.tz)
    }
}
