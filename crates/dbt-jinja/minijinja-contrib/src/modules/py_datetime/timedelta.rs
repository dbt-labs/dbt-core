use chrono::Duration;
use minijinja::{arg_utils::ArgParser, value::Object, Error, ErrorKind, Value};
use std::fmt;
use std::sync::Arc;

use super::{
    date::PyDate,
    datetime::{DateTimeState, PyDateTime},
    time::PyTime,
};

#[derive(Clone, Debug)]
pub(crate) struct PyTimeDeltaClass;

impl PyTimeDeltaClass {
    fn timedelta_new(args: &[Value]) -> Result<PyTimeDelta, Error> {
        let mut parser = ArgParser::new(args, None);
        let days: i64 = parser.get("days").unwrap_or(0);
        let seconds: i64 = parser.get("seconds").unwrap_or(0);
        let microseconds: i64 = parser.get("microseconds").unwrap_or(0);
        let milliseconds: i64 = parser.get("milliseconds").unwrap_or(0);
        let minutes: i64 = parser.get("minutes").unwrap_or(0);
        let hours: i64 = parser.get("hours").unwrap_or(0);
        let weeks: i64 = parser.get("weeks").unwrap_or(0);

        let duration = Duration::weeks(weeks)
            + Duration::days(days)
            + Duration::hours(hours)
            + Duration::minutes(minutes)
            + Duration::seconds(seconds)
            + Duration::milliseconds(milliseconds)
            + Duration::microseconds(microseconds);
        Ok(PyTimeDelta::new(duration))
    }

    fn min() -> PyTimeDelta {
        PyTimeDelta::new(Duration::days(-999_999_999))
    }

    fn max() -> PyTimeDelta {
        PyTimeDelta::new(
            Duration::days(999_999_999) + Duration::seconds(86399) + Duration::microseconds(999999),
        )
    }

    fn resolution() -> PyTimeDelta {
        PyTimeDelta::new(Duration::microseconds(1))
    }
}

impl Object for PyTimeDeltaClass {
    fn call(
        self: &std::sync::Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        Self::timedelta_new(args).map(Value::from_object)
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "min" => Some(Value::from_object(Self::min())),
            "max" => Some(Value::from_object(Self::max())),
            "resolution" => Some(Value::from_object(Self::resolution())),
            _ => None,
        }
    }
}

// ----------------------------------------------------------------
// PyTimeDelta definition
// ----------------------------------------------------------------
#[derive(Clone, Debug)]
pub(crate) struct PyTimeDelta {
    pub duration: Duration,
}

impl PyTimeDelta {
    pub fn new(duration: Duration) -> Self {
        PyTimeDelta { duration }
    }

    // Instance attributes
    pub fn days(&self) -> Option<Value> {
        Some(Value::from(self.duration.num_days()))
    }

    pub fn seconds(&self) -> Option<Value> {
        Some(Value::from(self.duration.num_seconds() % 86400))
    }

    pub fn microseconds(&self) -> Option<Value> {
        Some(Value::from(
            self.duration.num_microseconds().unwrap_or(0) % 1_000_000,
        ))
    }

    pub fn total_seconds(&self) -> Option<Value> {
        Some(Value::from(self.duration.num_seconds() as f64))
    }

    // ----------------------------------------------------------------
    // __add__(rhs)
    //
    //  1) timedelta + timedelta => timedelta
    //  2) timedelta + datetime  => datetime
    //  3) timedelta + time => time
    //  4) timedelta + date => date
    //  5) otherwise error
    // ----------------------------------------------------------------
    fn add(&self, args: &[Value]) -> Result<Value, Error> {
        let mut parser = ArgParser::new(args, None);
        let rhs: Value = parser.next_positional()?;

        // 1) timedelta + timedelta = timedelta
        if let Some(other_delta) = rhs.downcast_object_ref::<PyTimeDelta>() {
            let new_duration = self.duration + other_delta.duration;
            return Ok(Value::from_object(PyTimeDelta::new(new_duration)));
        }
        // 2) timedelta + datetime = datetime
        else if let Some(dt) = rhs.downcast_object_ref::<PyDateTime>() {
            match &dt.state {
                // If dt is naive, produce a naive result
                DateTimeState::Naive(ndt) => {
                    let new_naive = *ndt + self.duration;
                    return Ok(Value::from_object(PyDateTime::new_naive(new_naive)));
                }
                // If dt is aware, produce an aware result with the same Tz
                DateTimeState::Aware(adt) => {
                    let new_aware = *adt + self.duration; // chrono::DateTime<Tz> + chrono::Duration
                    return Ok(Value::from_object(PyDateTime::new_aware(
                        new_aware,
                        dt.tzinfo.clone(),
                    )));
                }
                // If dt has a fixed offset, preserve it
                DateTimeState::FixedOffset(fdt) => {
                    let new_fixed = *fdt + self.duration;
                    return Ok(Value::from_object(PyDateTime {
                        state: DateTimeState::FixedOffset(new_fixed),
                        tzinfo: dt.tzinfo.clone(),
                    }));
                }
            }
        }
        // 3) timedelta + time = time
        else if let Some(time) = rhs.downcast_object_ref::<PyTime>() {
            let new_time = time.time + self.duration;
            return Ok(Value::from_object(PyTime::new(new_time, None)));
        }
        // 4) timedelta + date = date
        else if let Some(date) = rhs.downcast_object_ref::<PyDate>() {
            let new_date = date.date + self.duration;
            return Ok(Value::from_object(PyDate::new(new_date)));
        }

        Err(Error::new(
            ErrorKind::InvalidOperation,
            "Cannot add timedelta to this type",
        ))
    }

    // ----------------------------------------------------------------
    // __sub__(rhs)
    //
    //  1) timedelta - timedelta => timedelta
    //  (In Python, there's no direct 'timedelta - datetime' or such.)
    // ----------------------------------------------------------------
    fn sub(&self, args: &[Value]) -> Result<Value, Error> {
        let mut parser = ArgParser::new(args, None);
        let rhs: Value = parser.next_positional()?;

        // 1) timedelta - timedelta = timedelta
        if let Some(other_delta) = rhs.downcast_object_ref::<PyTimeDelta>() {
            let new_duration = self.duration - other_delta.duration;
            return Ok(Value::from_object(PyTimeDelta::new(new_duration)));
        }

        // Python doesn't allow `timedelta - datetime`.
        // "datetime" - "timedelta" is fine, but that would be handled by
        // the PyDateTime's __sub__ method, not here.

        Err(Error::new(
            ErrorKind::InvalidOperation,
            "Cannot subtract this type from a timedelta",
        ))
    }
}

// ----------------------------------------------------------------
// Implementation of Object for PyTimeDelta
// ----------------------------------------------------------------
impl Object for PyTimeDelta {
    fn is_true(self: &Arc<Self>) -> bool {
        self.duration.num_seconds() != 0
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "days" => self.days(),
            "seconds" => self.seconds(),
            "microseconds" => self.microseconds(),
            "total_seconds" => self.total_seconds(),
            _ => None,
        }
    }

    fn call_method(
        self: &std::sync::Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        match method {
            "__add__" => self.add(args),
            "__sub__" => self.sub(args),
            _ => Err(Error::new(
                ErrorKind::UnknownMethod("PyTimeDelta".to_string(), method.to_string()),
                format!("timedelta has no method named '{method}'"),
            )),
        }
    }

    /// e.g.  "2 days, 05:00:00.000123"
    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let days = self.duration.num_days();
        let total_secs = self.duration.num_seconds().abs();
        let seconds_in_day = total_secs % 86400;
        let hours = seconds_in_day / 3600;
        let minutes = (seconds_in_day % 3600) / 60;
        let seconds = seconds_in_day % 60;
        // handle the sign for negative durations
        let sign = if self.duration.num_seconds() < 0 {
            "-"
        } else {
            ""
        };

        let microseconds = self.duration.num_microseconds().unwrap_or(0).abs() % 1_000_000;

        if days != 0 {
            write!(
                f,
                "{}{} days, {:02}:{:02}:{:02}.{:06}",
                sign,
                days.abs(),
                hours,
                minutes,
                seconds,
                microseconds
            )
        } else {
            write!(
                f,
                "{sign}{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::args;
    use minijinja::Environment;
    use minijinja::Value;

    #[test]
    fn test_timedelta_creation() {
        let td = PyTimeDeltaClass::timedelta_new(&[]).unwrap();
        assert_eq!(td.duration.num_seconds(), 0);

        let td =
            PyTimeDeltaClass::timedelta_new(args!(days => 2, hours => 3, minutes => 30)).unwrap();
        assert_eq!(td.duration.num_days(), 2);
        assert_eq!(td.duration.num_hours() % 24, 3);
        assert_eq!(td.duration.num_minutes() % 60, 30);
    }

    #[test]
    fn test_timedelta_attributes() {
        let td = PyTimeDelta::new(
            Duration::days(2)
                + Duration::hours(3)
                + Duration::minutes(30)
                + Duration::microseconds(123456),
        );
        assert_eq!(td.days().unwrap().as_i64().unwrap(), 2);
        assert_eq!(td.seconds().unwrap().as_i64().unwrap(), 12600); // 3h30m = 12600s
        assert_eq!(td.microseconds().unwrap().as_i64().unwrap(), 123456);
    }

    #[test]
    fn test_timedelta_arithmetic() {
        let td1 = PyTimeDelta::new(Duration::days(2));
        let td2 = PyTimeDelta::new(Duration::days(1));
        let td1_arc = Arc::new(td1);

        // Test addition
        let binding = td1_arc.add(&[Value::from_object(td2.clone())]).unwrap();
        let result = binding.downcast_object_ref::<PyTimeDelta>().unwrap();
        assert_eq!(result.duration.num_days(), 3);

        // Test subtraction
        let binding = td1_arc.sub(&[Value::from_object(td2)]).unwrap();
        let result = binding.downcast_object_ref::<PyTimeDelta>().unwrap();
        assert_eq!(result.duration.num_days(), 1);
    }

    #[test]
    fn test_timedelta_in_template() {
        let mut env = Environment::new();
        env.add_global("timedelta", Value::from_object(PyTimeDeltaClass));

        // Test creation and attributes
        let template = env
            .template_from_str(
                "{{ timedelta(days=2, hours=3).days }}, {{ timedelta(minutes=90).seconds }}",
                &[],
            )
            .unwrap();
        let result = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(result, "2, 5400");

        // Test positional arguments creation
        let template = env
            .template_from_str("{{ timedelta(4).days }}", &[])
            .unwrap();
        let result = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(result, "4");

        // Test arithmetic
        let template = env
            .template_from_str("{{ (timedelta(days=2) + timedelta(days=1)).days }}", &[])
            .unwrap();
        let result = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(result, "3");
    }

    #[test]
    fn test_timedelta_constants() {
        let min_td = PyTimeDeltaClass::min();
        assert_eq!(min_td.duration.num_days(), -999_999_999);

        let max_td = PyTimeDeltaClass::max();
        assert_eq!(max_td.duration.num_days(), 999_999_999);
        assert_eq!(max_td.duration.num_seconds() % 86400, 86399);

        let resolution = PyTimeDeltaClass::resolution();
        assert_eq!(resolution.duration.num_microseconds().unwrap(), 1);
    }
}
