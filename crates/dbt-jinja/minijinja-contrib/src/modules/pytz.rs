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
            format!("Invalid timezone name: {tz_name}"),
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
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        match method {
            "localize" => {
                // Get the datetime argument
                let dt_val = args.first().ok_or_else(|| {
                    Error::new(
                        ErrorKind::MissingArgument,
                        "localize() requires a datetime argument",
                    )
                })?;

                // Get the PyDateTime object
                let dt = dt_val
                    .downcast_object_ref::<crate::modules::py_datetime::datetime::PyDateTime>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidArgument,
                            "localize() expects a datetime object",
                        )
                    })?;

                // Get the naive datetime
                use crate::modules::py_datetime::datetime::DateTimeState;
                let naive_dt = match &dt.state {
                    DateTimeState::Naive(ndt) => *ndt,
                    DateTimeState::Aware(_) => {
                        return Err(Error::new(
                            ErrorKind::InvalidOperation,
                            "localize() requires a naive datetime (tzinfo must be None)",
                        ));
                    }
                    DateTimeState::FixedOffset(_) => {
                        return Err(Error::new(
                            ErrorKind::InvalidOperation,
                            "localize() requires a naive datetime (tzinfo must be None)",
                        ));
                    }
                };

                // Create an aware datetime by interpreting the naive datetime in this timezone
                let aware_dt =
                    self.tz
                        .from_local_datetime(&naive_dt)
                        .single()
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::InvalidOperation,
                                "ambiguous or invalid local time for localize()",
                            )
                        })?;

                // Return a new PyDateTime object
                let result = crate::modules::py_datetime::datetime::PyDateTime {
                    state: DateTimeState::Aware(aware_dt),
                    tzinfo: Some(self.as_ref().clone()),
                };

                Ok(Value::from_object(result))
            }
            _ => Err(Error::new(
                ErrorKind::UnknownMethod,
                format!("Timezone object has no method named '{method}'"),
            )),
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If you do {{ tz_obj }} in Jinja, it prints out the name
        write!(f, "{}", self.tz)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::py_datetime::datetime::{DateTimeState, PyDateTime};
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};

    #[test]
    fn test_localize_naive_datetime() {
        // Create a naive datetime
        let naive = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
            NaiveTime::from_hms_opt(12, 30, 0).unwrap(),
        );

        // Localize to UTC
        let utc_tz = PytzTimezone::new(chrono_tz::UTC);

        // Manually call localize logic
        let aware_dt_result = utc_tz
            .tz
            .from_local_datetime(&naive)
            .single()
            .ok_or_else(|| Error::new(ErrorKind::InvalidOperation, "ambiguous"));

        assert!(aware_dt_result.is_ok());
        let aware_dt = aware_dt_result.unwrap();

        // The time should be the same (localize doesn't convert)
        assert_eq!(aware_dt.hour(), 12);
        assert_eq!(aware_dt.minute(), 30);

        // Create the result PyDateTime
        let result = PyDateTime {
            state: DateTimeState::Aware(aware_dt),
            tzinfo: Some(utc_tz),
        };

        // Should be aware with UTC timezone
        assert!(result.tzinfo.is_some());
        assert_eq!(result.tzinfo.as_ref().unwrap().tz, chrono_tz::UTC);
    }

    #[test]
    fn test_localize_different_timezone() {
        // Create a naive datetime
        let naive = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
            NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
        );

        // Localize to US/Pacific
        let pacific_tz = PytzTimezone::new(chrono_tz::US::Pacific);
        let aware_dt_result = pacific_tz
            .tz
            .from_local_datetime(&naive)
            .single()
            .ok_or_else(|| Error::new(ErrorKind::InvalidOperation, "ambiguous"));

        assert!(aware_dt_result.is_ok());
        let aware_dt = aware_dt_result.unwrap();

        // The time should remain 15:00 (localize doesn't convert, just adds timezone)
        assert_eq!(aware_dt.hour(), 15);
        assert_eq!(aware_dt.minute(), 0);

        // Create the result PyDateTime
        let result = PyDateTime {
            state: DateTimeState::Aware(aware_dt),
            tzinfo: Some(pacific_tz),
        };

        // Should be aware with Pacific timezone
        assert!(result.tzinfo.is_some());
        assert_eq!(result.tzinfo.as_ref().unwrap().tz, chrono_tz::US::Pacific);
    }
}
