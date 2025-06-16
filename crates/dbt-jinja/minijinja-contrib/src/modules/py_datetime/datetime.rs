use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use minijinja::{arg_utils::ArgParser, value::Object, Error, ErrorKind, Value};

use crate::modules::py_datetime::date::PyDate; // your date
use crate::modules::py_datetime::time::PyTime;
use crate::modules::py_datetime::timedelta::PyTimeDelta;
use crate::modules::pytz::PytzTimezone; // your timedelta // your trait // your time

/// An enum storing either a naive datetime or an aware datetime with a known timezone.
#[derive(Clone, Debug)]
pub enum DateTimeState {
    Naive(NaiveDateTime),
    Aware(DateTime<Tz>),
    FixedOffset(DateTime<chrono::FixedOffset>), // Add this variant
}

/// The user-facing "datetime" constructor object (Python's `datetime.datetime`).
#[derive(Clone, Debug)]
pub struct PyDateTime {
    pub state: DateTimeState,
    /// If `Some(...)`, this is an aware datetime with the given tzinfo object (pytz or fixed offset).
    /// If `None`, this is naive.
    pub tzinfo: Option<PytzTimezone>,
}

/// The module object that the user calls as `datetime(...)`, or `datetime.now()`, etc.
#[derive(Clone, Debug)]
pub struct PyDateTimeClass;

impl PyDateTimeClass {
    // ------------------------------------------------------------------
    // datetime(...)  =>  naive or aware, depending on kwarg tzinfo
    // ------------------------------------------------------------------
    fn new_datetime(args: &[Value]) -> Result<PyDateTime, Error> {
        // We accept signature like:
        //   datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        let mut parser = ArgParser::new(args, None);

        let year: i32 = parser.get::<i32>("year")?;
        let month: u32 = parser.get::<u32>("month")?;
        let day: u32 = parser.get::<u32>("day")?;

        let hour: u32 = parser.get_optional::<u32>("hour").unwrap_or(0);
        let minute: u32 = parser.get_optional::<u32>("minute").unwrap_or(0);
        let second: u32 = parser.get_optional::<u32>("second").unwrap_or(0);
        let microsecond: u32 = parser.get_optional::<u32>("microsecond").unwrap_or(0);

        // Optionally parse a tzinfo from kwargs
        // In real Python, it's a kwarg, so we can do:
        let tz_val = parser.get_optional::<Value>("tzinfo");

        // Build the naive date/time
        let date = NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "Invalid date components"))?;
        let time = NaiveTime::from_hms_micro_opt(hour, minute, second, microsecond)
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "Invalid time components"))?;
        let naive_dt = NaiveDateTime::new(date, time);

        // If tzinfo is provided, we interpret it as an aware datetime
        if let Some(tz_val) = tz_val {
            if tz_val.is_none() {
                // tzinfo=None => naive
                Ok(PyDateTime {
                    state: DateTimeState::Naive(naive_dt),
                    tzinfo: None,
                })
            } else if let Some(tz) = tz_val.downcast_object_ref::<PytzTimezone>() {
                // We have a chrono_tz::Tz in tz.tz
                // Convert naive to aware
                let aware_dt = tz
                    .tz
                    .from_local_datetime(&naive_dt)
                    .single()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidArgument,
                            "ambiguous or invalid local time in that timezone",
                        )
                    })?;
                Ok(PyDateTime {
                    state: DateTimeState::Aware(aware_dt),
                    tzinfo: Some(tz.clone()),
                })
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "tzinfo must be a pytz timezone or None",
                ));
            }
        } else {
            // no tzinfo => naive
            Ok(PyDateTime {
                state: DateTimeState::Naive(naive_dt),
                tzinfo: None,
            })
        }
    }

    // ------------------------------------------------------------------
    // datetime.now(tz=None)
    //   If tz=None => naive local
    //   If tz=some => aware in that tz
    // ------------------------------------------------------------------
    fn now(args: &[Value]) -> Result<PyDateTime, Error> {
        let mut parser = ArgParser::new(args, None);
        let tz_val = parser.get_optional::<Value>("tz");

        let local_now = chrono::Local::now(); // DateTime<Local>

        if tz_val.is_none() || (tz_val.is_some() && tz_val.as_ref().unwrap().is_none()) {
            // tz is None, fall through to default case
            let local_tz = iana_time_zone::get_timezone().expect("Could not determine timezone.");
            let local_tz =
                chrono_tz::Tz::from_str(local_tz.as_str()).expect("Could not determine timezone.");
            let py_dt = PyDateTime {
                state: DateTimeState::Aware(local_now.with_timezone(&local_tz)),
                tzinfo: Some(PytzTimezone { tz: local_tz }),
            };
            Ok(py_dt)
        } else if let Some(tz) = tz_val
            .as_ref()
            .unwrap() // we checked that it's not None above
            .downcast_object_ref::<PytzTimezone>()
        {
            // Convert local_now (which we can interpret as local) to UTC, then to tz
            let dt_utc = local_now.with_timezone(&chrono::Utc);
            let new_aware = dt_utc.with_timezone(&tz.tz);
            let py_dt = PyDateTime {
                state: DateTimeState::Aware(new_aware),
                tzinfo: Some(tz.clone()),
            };
            Ok(py_dt)
        } else {
            Err(Error::new(
                ErrorKind::InvalidArgument,
                "tz must be a pytz timezone or None",
            ))
        }
    }

    // ------------------------------------------------------------------
    // datetime.utcnow()
    //   naive UTC
    // ------------------------------------------------------------------
    fn utcnow(_args: &[Value]) -> Result<PyDateTime, Error> {
        let now_utc = Utc::now();
        let py_dt = PyDateTime {
            state: DateTimeState::Naive(now_utc.naive_utc()),
            tzinfo: None,
        };
        Ok(py_dt)
    }

    // ------------------------------------------------------------------
    // datetime.today()
    //   naive local, at midnight
    // ------------------------------------------------------------------
    fn today(_args: &[Value]) -> Result<PyDateTime, Error> {
        let local_now = chrono::Local::now(); // DateTime<Local>
        let naive_local = local_now.naive_local();
        let py_dt = PyDateTime {
            state: DateTimeState::Naive(naive_local.date().and_hms_opt(0, 0, 0).unwrap()),
            tzinfo: None,
        };
        Ok(py_dt)
    }

    // ------------------------------------------------------------------
    // datetime.fromtimestamp(timestamp, tz=None)
    //   If tz=None => naive local
    //   If tz => interpret as aware in that tz
    // ------------------------------------------------------------------
    fn from_timestamp(args: &[Value]) -> Result<PyDateTime, Error> {
        let mut parser = ArgParser::new(args, None);
        let timestamp = parser.next_positional::<f64>()?;
        let tz_val = parser.get_optional::<Value>("tz");

        let secs = timestamp.trunc() as i64;
        let nanos = (timestamp.fract() * 1e9).round() as u32;

        if let Some(tz_val) = tz_val {
            if tz_val.is_none() {
                // interpret as naive local
                let local_dt = chrono::Local
                    .timestamp_opt(secs, nanos)
                    .single()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidArgument,
                            "ambiguous or invalid local time for that timestamp",
                        )
                    })?;
                let py_dt = PyDateTime {
                    state: DateTimeState::Naive(local_dt.naive_local()),
                    tzinfo: None,
                };
                Ok(py_dt)
            } else if let Some(tz) = tz_val.downcast_object_ref::<PytzTimezone>() {
                // interpret as UTC, then convert to tz
                let dt_utc = chrono::Utc
                    .timestamp_opt(secs, nanos)
                    .single()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidArgument,
                            "invalid or out of range timestamp",
                        )
                    })?;
                let new_aware = dt_utc.with_timezone(&tz.tz);
                let py_dt = PyDateTime {
                    state: DateTimeState::Aware(new_aware),
                    tzinfo: Some(tz.clone()),
                };
                Ok(py_dt)
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "tz must be a pytz timezone or None",
                ));
            }
        } else {
            // no tz => naive local
            let local_dt = chrono::Local
                .timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidArgument,
                        "ambiguous or invalid local time for that timestamp",
                    )
                })?;
            let py_dt = PyDateTime {
                state: DateTimeState::Naive(local_dt.naive_local()),
                tzinfo: None,
            };
            Ok(py_dt)
        }
    }

    // ------------------------------------------------------------------
    // datetime.combine(date, time[, tzinfo])
    //   Returns a new datetime object
    // ------------------------------------------------------------------
    fn combine(args: &[Value]) -> Result<PyDateTime, Error> {
        let mut parser = ArgParser::new(args, None);
        // date param
        let date_val = parser.next_positional::<Value>()?;
        // time param
        let time_val = parser.next_positional::<Value>()?;
        // optional tzinfo
        let tz_val = parser.get_optional::<Value>("tzinfo");

        let py_date = date_val
            .downcast_object_ref::<PyDate>()
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "combine expects a date"))?;
        let py_time = time_val
            .downcast_object_ref::<PyTime>()
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "combine expects a time"))?;

        let naive_dt = NaiveDateTime::new(py_date.date, py_time.time);

        if let Some(tz_val) = tz_val {
            if tz_val.is_none() {
                // naive
                let py_dt = PyDateTime {
                    state: DateTimeState::Naive(naive_dt),
                    tzinfo: None,
                };
                Ok(py_dt)
            } else if let Some(tz) = tz_val.downcast_object_ref::<PytzTimezone>() {
                let aware_dt = tz
                    .tz
                    .from_local_datetime(&naive_dt)
                    .single()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidArgument,
                            "ambiguous or invalid local time in that timezone",
                        )
                    })?;
                let py_dt = PyDateTime {
                    state: DateTimeState::Aware(aware_dt),
                    tzinfo: Some(tz.clone()),
                };
                Ok(py_dt)
            } else {
                Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "tzinfo must be a pytz timezone or None",
                ))
            }
        } else {
            // naive
            let py_dt = PyDateTime {
                state: DateTimeState::Naive(naive_dt),
                tzinfo: None,
            };
            Ok(py_dt)
        }
    }

    // ------------------------------------------------------------------
    // datetime.strptime(date_string, format)
    // ------------------------------------------------------------------
    fn strptime(args: &[Value]) -> Result<PyDateTime, Error> {
        let mut parser = ArgParser::new(args, None);
        let date_str: String = parser.next_positional()?;
        let fmt_str: String = parser.next_positional()?;

        let naive = Self::parse_datetime_with_fallback(&date_str, &fmt_str).map_err(|e| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("strptime parsing error: {}", e),
            )
        })?;

        // This yields a naive datetime. If you want to let user supply tz=..., parse it.
        Ok(PyDateTime {
            state: DateTimeState::Naive(naive),
            tzinfo: None,
        })
    }

    fn fromisoformat(args: &[Value]) -> Result<PyDateTime, Error> {
        let mut parser = ArgParser::new(args, None);
        let date_str: String = parser.next_positional()?;

        // First try parsing with timezone offset
        let error = match DateTime::parse_from_str(&date_str, "%Y-%m-%dT%H:%M:%S%.f%:z")
            .or_else(|_| DateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S%.f%:z"))
        {
            Ok(dt) => {
                return Ok(PyDateTime {
                    state: DateTimeState::FixedOffset(dt), // Keep as DateTime<FixedOffset>
                    tzinfo: Some(PytzTimezone { tz: Tz::UTC }), // Use UTC for tzinfo
                });
            }
            Err(e) => e,
        };

        // If no timezone, try parsing as naive
        if let Ok(naive) = NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S%.f"))
        {
            return Ok(PyDateTime {
                state: DateTimeState::Naive(naive),
                tzinfo: None,
            });
        }

        // If none of the above worked, try parsing as date
        if let Ok(date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
            return Ok(PyDateTime {
                state: DateTimeState::Naive(date.and_hms_opt(0, 0, 0).unwrap()),
                tzinfo: None,
            });
        }

        Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("fromisoformat parsing error: {}: {}", date_str, error),
        ))
    }

    /// Attempts to parse a datetime string, filling in missing date or time parts.
    fn parse_datetime_with_fallback(input: &str, fmt: &str) -> Result<NaiveDateTime, String> {
        // First, try parsing the full datetime directly
        if let Ok(dt) = NaiveDateTime::parse_from_str(input, fmt) {
            return Ok(dt);
        }

        // Try parsing just the date
        if let Ok(date) = NaiveDate::parse_from_str(input, fmt) {
            return Ok(date.and_hms_opt(0, 0, 0).unwrap());
        }

        // Try parsing just the time
        if let Ok(time) = NaiveTime::parse_from_str(input, fmt) {
            let default_date = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
            return Ok(default_date.and_time(time));
        }

        // Otherwise, return the error
        Err("Could not parse input as datetime, date, or time".to_string())
    }
}

/// The actual module object, so user can do:
///   {{ datetime(...) }}, {{ datetime.now() }}, {{ datetime.fromtimestamp(...) }}, etc.
impl Object for PyDateTimeClass {
    fn call(
        self: &Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        Ok(Value::from_object(Self::new_datetime(args)?))
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        match method {
            "now" => Ok(Value::from_object(Self::now(args)?)),
            "utcnow" => Ok(Value::from_object(Self::utcnow(args)?)),
            "today" => Ok(Value::from_object(Self::today(args)?)),
            "fromtimestamp" => Ok(Value::from_object(Self::from_timestamp(args)?)),
            "combine" => Ok(Value::from_object(Self::combine(args)?)),
            "strptime" => Ok(Value::from_object(Self::strptime(args)?)),
            "fromisoformat" => Ok(Value::from_object(Self::fromisoformat(args)?)),
            "strftime" => {
                // Handle strftime(datetime, format) case
                let mut parser = ArgParser::new(args, None);
                let datetime_val = parser.next_positional::<Value>()?;
                let format_val = parser.next_positional::<Value>()?;

                // Check if the first argument is a PyDateTime
                if let Some(datetime) = datetime_val.downcast_object_ref::<PyDateTime>() {
                    return datetime.strftime(&[format_val]);
                }

                // Check if it's a PyDate
                if let Some(date) = datetime_val.downcast_object_ref::<super::date::PyDate>() {
                    return date.strftime(&[format_val]);
                }

                // Check if it's a PyTime
                if let Some(time) = datetime_val.downcast_object_ref::<super::time::PyTime>() {
                    return time.strftime(&[format_val]);
                }

                // If we get here, the argument is not a valid datetime-like object
                Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "strftime expects a datetime, date, or time object as first argument",
                ))
            }
            _ => Err(Error::new(
                ErrorKind::UnknownMethod("PyDateTimeClass".to_string(), method.to_string()),
                format!("datetime has no method named '{}'", method),
            )),
        }
    }
}

//
// Implementation of PyDateTime object
//
impl PyDateTime {
    // convenience "naive" constructor
    pub fn new_naive(dt: NaiveDateTime) -> Self {
        PyDateTime {
            state: DateTimeState::Naive(dt),
            tzinfo: None,
        }
    }

    // convenience "aware" constructor
    pub fn new_aware(dt: DateTime<Tz>, tzinfo: Option<PytzTimezone>) -> Self {
        PyDateTime {
            state: DateTimeState::Aware(dt),
            tzinfo: Some(tzinfo.unwrap_or(PytzTimezone { tz: Tz::UTC })),
        }
    }

    /// Return naive or aware's .year
    pub fn year(&self) -> Option<Value> {
        Some(Value::from(self.chrono_dt().year()))
    }

    pub fn month(&self) -> Option<Value> {
        Some(Value::from(self.chrono_dt().month()))
    }

    pub fn day(&self) -> Option<Value> {
        Some(Value::from(self.chrono_dt().day()))
    }

    pub fn hour(&self) -> Option<Value> {
        Some(Value::from(self.chrono_dt().hour()))
    }

    pub fn minute(&self) -> Option<Value> {
        Some(Value::from(self.chrono_dt().minute()))
    }

    pub fn second(&self) -> Option<Value> {
        Some(Value::from(self.chrono_dt().second()))
    }

    /// Return .tzinfo. If naive => None
    pub fn tzinfo(&self) -> Option<Value> {
        self.tzinfo.clone().map(Value::from_object)
    }

    /// "chrono_dt" is a helper method that returns a naive DateTime if we're naive,
    /// or the local datetime if we're aware. This is mostly for read-only field access.
    pub fn chrono_dt(&self) -> chrono::NaiveDateTime {
        match &self.state {
            DateTimeState::Naive(ndt) => *ndt,
            DateTimeState::Aware(adt) => adt.naive_local(),
            DateTimeState::FixedOffset(fdt) => fdt.naive_local(),
        }
    }

    /// strftime(format)
    pub fn strftime(&self, args: &[Value]) -> Result<Value, Error> {
        let fmt = args.first().and_then(|v| v.as_str()).ok_or_else(|| {
            Error::new(
                ErrorKind::MissingArgument,
                "strftime requires one string argument",
            )
        })?;
        let s = match &self.state {
            DateTimeState::Naive(ndt) => ndt.format(fmt).to_string(),
            DateTimeState::Aware(adt) => adt.format(fmt).to_string(),
            DateTimeState::FixedOffset(fdt) => fdt.format(fmt).to_string(),
        };
        Ok(Value::from(s))
    }

    /// isoformat() -> "YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]"
    pub fn isoformat(&self) -> String {
        match &self.state {
            DateTimeState::Naive(ndt) => {
                // naive => omit offset
                // Use the same separator (T or space) that was in the input
                let formatted = ndt.format("%Y-%m-%dT%H:%M:%S.%6f").to_string();
                // Only include decimal point and microseconds if they are non-zero
                if formatted.ends_with(".000000") {
                    formatted[..formatted.len() - 7].to_string()
                } else {
                    formatted // Keep all microsecond digits
                }
            }
            DateTimeState::Aware(adt) => {
                // aware => include offset
                let formatted = adt.format("%Y-%m-%dT%H:%M:%S.%6f%:z").to_string(); // Always use T for aware datetimes
                if formatted.contains(".000000") {
                    formatted.replace(".000000", "")
                } else {
                    formatted // Keep all microsecond digits
                }
            }
            DateTimeState::FixedOffset(dt) => {
                // Fixed offset case - use original format
                let formatted = dt.format("%Y-%m-%dT%H:%M:%S.%6f%:z").to_string();
                if formatted.contains(".000000") {
                    formatted.replace(".000000", "")
                } else {
                    formatted // Keep all microsecond digits
                }
            }
        }
    }

    /// .timestamp() -> float
    /// If naive, interpret as local in Python, or raise error. We'll do local for demo:
    pub fn timestamp(&self) -> f64 {
        match &self.state {
            DateTimeState::Naive(ndt) => {
                // interpret as local
                let local_dt = chrono::Local
                    .from_local_datetime(ndt)
                    .single()
                    .expect("ambiguous local time");
                local_dt.timestamp() as f64 + (local_dt.timestamp_subsec_nanos() as f64 * 1e-9)
            }
            DateTimeState::Aware(adt) => {
                // convert to UTC, then get timestamp
                let utc_dt = adt.with_timezone(&chrono::Utc);
                utc_dt.timestamp() as f64 + (utc_dt.timestamp_subsec_nanos() as f64 * 1e-9)
            }
            DateTimeState::FixedOffset(fdt) => {
                // fixed offset => interpret as local
                let local_dt = fdt.with_timezone(&chrono::Local);
                local_dt.timestamp() as f64 + (local_dt.timestamp_subsec_nanos() as f64 * 1e-9)
            }
        }
    }

    /// __add__(timedelta) or __sub__(timedelta or datetime)
    fn add_op(&self, args: &[Value], is_add: bool) -> Result<Value, Error> {
        let mut parser = ArgParser::new(args, None);
        let rhs: Value = parser.next_positional()?;

        // If it's a PyTimeDelta
        if let Some(delta) = rhs.downcast_object_ref::<PyTimeDelta>() {
            // datetime + timedelta => datetime
            let dur = if is_add {
                delta.duration
            } else {
                -delta.duration
            };

            match &self.state {
                DateTimeState::Naive(ndt) => {
                    let new_naive = *ndt + dur;
                    Ok(Value::from_object(PyDateTime {
                        state: DateTimeState::Naive(new_naive),
                        tzinfo: self.tzinfo.clone(),
                    }))
                }
                DateTimeState::Aware(adt) => {
                    let new_aware = *adt + dur;
                    // same tzinfo
                    Ok(Value::from_object(PyDateTime {
                        state: DateTimeState::Aware(new_aware),
                        tzinfo: self.tzinfo.clone(),
                    }))
                }
                DateTimeState::FixedOffset(fdt) => {
                    let new_fdt = *fdt + dur;
                    Ok(Value::from_object(PyDateTime {
                        state: DateTimeState::FixedOffset(new_fdt),
                        tzinfo: self.tzinfo.clone(),
                    }))
                }
            }
        }
        // If it's another PyDateTime => return a timedelta
        else if let Some(other_dt) = rhs.downcast_object_ref::<PyDateTime>() {
            // datetime - datetime => timedelta
            if !is_add {
                // we do self - other
                let self_chrono = match &self.state {
                    DateTimeState::Naive(ndt) => chrono::Local
                        .from_local_datetime(ndt)
                        .single()
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::InvalidArgument,
                                "ambiguous local time for naive datetime",
                            )
                        })?
                        .with_timezone(&chrono::Utc), // interpret naive as local -> utc
                    DateTimeState::Aware(adt) => adt.with_timezone(&chrono::Utc),
                    DateTimeState::FixedOffset(fdt) => fdt.with_timezone(&chrono::Utc),
                };

                let other_chrono = match &other_dt.state {
                    DateTimeState::Naive(ndt) => chrono::Local
                        .from_local_datetime(ndt)
                        .single()
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::InvalidArgument,
                                "ambiguous local time for naive datetime",
                            )
                        })?
                        .with_timezone(&chrono::Utc),
                    DateTimeState::Aware(adt) => adt.with_timezone(&chrono::Utc),
                    DateTimeState::FixedOffset(fdt) => fdt.with_timezone(&chrono::Utc),
                };

                let diff = self_chrono.signed_duration_since(other_chrono);
                let td = PyTimeDelta::new(diff);
                Ok(Value::from_object(td))
            } else {
                // datetime + datetime not allowed in Python
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "Cannot add two datetime objects",
                ))
            }
        } else {
            Err(Error::new(
                ErrorKind::InvalidArgument,
                "Expected a timedelta or datetime on the right-hand side",
            ))
        }
    }

    pub fn weekday(&self) -> u32 {
        // Python's weekday() returns 0 for Monday, ... 6 for Sunday
        self.chrono_dt().weekday().num_days_from_monday()
    }

    pub fn isoweekday(&self) -> u32 {
        // Python's isoweekday() returns 1 for Monday, ... 7 for Sunday
        self.chrono_dt().weekday().num_days_from_monday() + 1
    }

    /// dt.date() => returns a PyDate
    pub fn date(&self) -> PyDate {
        let d = self.chrono_dt().date();
        PyDate::new(d)
    }

    /// dt.time() => returns a PyTime (naive time)
    pub fn time(&self) -> PyTime {
        let t = self.chrono_dt().time();
        PyTime::new(t, self.tzinfo.clone())
    }

    /// dt.replace(year=?, month=?, day=?, hour=?, minute=?, second=?, microsecond=?, tzinfo=?)
    pub fn replace(&self, args: &[Value]) -> Result<PyDateTime, Error> {
        let mut parser = ArgParser::new(args, None);

        let mut year = self.chrono_dt().year();
        let mut month = self.chrono_dt().month();
        let mut day = self.chrono_dt().day();
        let mut hour = self.chrono_dt().hour();
        let mut minute = self.chrono_dt().minute();
        let mut second = self.chrono_dt().second();
        let mut microsecond = self.chrono_dt().nanosecond() / 1000;

        // in Python, tzinfo can also be replaced
        let new_tzinfo_val = parser.consume_optional_only_from_kwargs::<Value>("tzinfo");

        if let Some(y) = parser.consume_optional_only_from_kwargs::<i32>("year") {
            year = y;
        }
        if let Some(m) = parser.consume_optional_only_from_kwargs::<u32>("month") {
            month = m;
        }
        if let Some(d) = parser.consume_optional_only_from_kwargs::<u32>("day") {
            day = d;
        }
        if let Some(h) = parser.consume_optional_only_from_kwargs::<u32>("hour") {
            hour = h;
        }
        if let Some(mi) = parser.consume_optional_only_from_kwargs::<u32>("minute") {
            minute = mi;
        }
        if let Some(s) = parser.consume_optional_only_from_kwargs::<u32>("second") {
            second = s;
        }
        if let Some(us) = parser.consume_optional_only_from_kwargs::<u32>("microsecond") {
            microsecond = us;
        }

        let new_date = NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "Invalid date components"))?;
        let new_time = NaiveTime::from_hms_micro_opt(hour, minute, second, microsecond)
            .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "Invalid time components"))?;
        let new_naive = NaiveDateTime::new(new_date, new_time);

        // parse tzinfo
        let final_tzinfo = if let Some(tz_val) = new_tzinfo_val {
            if tz_val.is_none() {
                None
            } else if let Some(tz) = tz_val.downcast_object_ref::<PytzTimezone>() {
                Some(tz.clone())
            } else {
                // fallback to the old tz if no recognized tz
                self.tzinfo.clone()
            }
        } else {
            // if tzinfo kwarg not provided, keep the same tzinfo
            self.tzinfo.clone()
        };

        if let Some(ref tz) = final_tzinfo {
            // produce an aware datetime
            // interpret new_naive in that tz
            let aware = tz
                .tz
                .from_local_datetime(&new_naive)
                .single()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidArgument,
                        "ambiguous or invalid local time in that timezone",
                    )
                })?;
            Ok(PyDateTime {
                state: DateTimeState::Aware(aware),
                tzinfo: final_tzinfo,
            })
        } else {
            // produce naive
            Ok(PyDateTime {
                state: DateTimeState::Naive(new_naive),
                tzinfo: None,
            })
        }
    }

    /// dt.astimezone(tz)
    /// If naive => error, or interpret as local
    /// If aware => do a real offset conversion from old tz to new tz
    pub fn astimezone(&self, tz: &PytzTimezone) -> Result<PyDateTime, Error> {
        match &self.state {
            DateTimeState::Naive(_) => {
                // Python 3.11 disallows astimezone on naive dt
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "astimezone() cannot be applied to a naive datetime",
                ))
            }
            DateTimeState::Aware(old_dt) => {
                // convert from old tz to new tz
                let dt_utc = old_dt.with_timezone(&chrono::Utc);
                let new_aware = dt_utc.with_timezone(&tz.tz);

                let py_dt = PyDateTime {
                    state: DateTimeState::Aware(new_aware),
                    tzinfo: Some(tz.clone()),
                };
                Ok(py_dt)
            }
            DateTimeState::FixedOffset(fdt) => {
                // Keep the datetime with its original fixed offset
                Ok(PyDateTime {
                    state: DateTimeState::FixedOffset(*fdt),
                    tzinfo: self.tzinfo.clone(),
                })
            }
        }
    }
}

//
// Implement the `Object` trait for PyDateTime so Jinja can call methods
//
impl Object for PyDateTime {
    fn is_true(self: &Arc<Self>) -> bool {
        true
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        match method {
            // "strftime(format)"
            "strftime" => self.strftime(args),

            // "astimezone(tz)"
            "astimezone" => {
                let tz_val = args.first().ok_or_else(|| {
                    Error::new(
                        ErrorKind::MissingArgument,
                        "astimezone() requires an argument",
                    )
                })?;
                let tz = tz_val
                    .downcast_object_ref::<PytzTimezone>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidArgument,
                            "astimezone() expects a PytzTimezone object",
                        )
                    })?;
                let new_dt = self.astimezone(tz)?;
                Ok(Value::from_object(new_dt))
            }

            // "replace(...)"
            "replace" => {
                let replaced = self.replace(args)?;
                Ok(Value::from_object(replaced))
            }

            // "date()"
            "date" => Ok(Value::from_object(self.date())),

            // "time()"
            "time" => Ok(Value::from_object(self.time())),

            // "weekday()"
            "weekday" => Ok(Value::from(self.weekday())),
            "isoweekday" => Ok(Value::from(self.isoweekday())),

            // "isoformat()"
            "isoformat" => Ok(Value::from(self.isoformat())),

            // "timestamp()"
            "timestamp" => Ok(Value::from(self.timestamp())),

            // Arithmetic
            "__add__" => self.add_op(args, true),
            "__sub__" => self.add_op(args, false),

            _ => Err(Error::new(
                ErrorKind::UnknownMethod("PyDateTime".to_string(), method.to_string()),
                format!("datetime has no method named '{}'", method),
            )),
        }
    }

    // Provide direct attribute access
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "year" => self.year(),
            "month" => self.month(),
            "day" => self.day(),
            "hour" => self.hour(),
            "minute" => self.minute(),
            "second" => self.second(),
            "tzinfo" => self.tzinfo(),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Just produce isoformat-like string
        write!(f, "{}", self.isoformat())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use minijinja::args;

    #[test]
    fn test_strptime_with_fallback() {
        let result = PyDateTimeClass::strptime(args!("2023-01-02 15:30:45", "%Y-%m-%d %H:%M:%S"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(
            dt.chrono_dt(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
                NaiveTime::from_hms_opt(15, 30, 45).unwrap()
            )
        );

        let result = PyDateTimeClass::strptime(args!("15:30:45", "%H:%M:%S"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(
            dt.chrono_dt(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(15, 30, 45).unwrap()
            )
        );

        let result = PyDateTimeClass::strptime(args!("invalid", "%Y-%m-%d"));
        assert!(result.is_err());

        let result = PyDateTimeClass::strptime(args!("2023-01-02", "%Y-%m-%d"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(
            dt.chrono_dt(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap()
            )
        );
    }

    #[test]
    fn test_parse_datetime_with_fallback() {
        // Test full datetime parsing
        let result = PyDateTimeClass::parse_datetime_with_fallback(
            "2023-01-02 15:30:45",
            "%Y-%m-%d %H:%M:%S",
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
                NaiveTime::from_hms_opt(15, 30, 45).unwrap()
            )
        );

        // Test date-only parsing
        let result = PyDateTimeClass::parse_datetime_with_fallback("2023-01-02", "%Y-%m-%d");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap()
            )
        );

        // Test time-only parsing
        let result = PyDateTimeClass::parse_datetime_with_fallback("15:30:45", "%H:%M:%S");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(15, 30, 45).unwrap()
            )
        );

        // Test invalid format
        let result = PyDateTimeClass::parse_datetime_with_fallback("invalid", "%Y-%m-%d");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Could not parse input as datetime, date, or time".to_string()
        );
    }

    #[test]
    fn test_fromisoformat() {
        let result = PyDateTimeClass::fromisoformat(args!("2023-01-02T15:30:45"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.isoformat(), "2023-01-02T15:30:45");

        let result = PyDateTimeClass::fromisoformat(args!("2023-01-02T15:30:45.000001"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.isoformat(), "2023-01-02T15:30:45.000001");

        // Test with trailing zeros in microseconds
        let result = PyDateTimeClass::fromisoformat(args!("2023-01-02T15:30:45.100000"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.isoformat(), "2023-01-02T15:30:45.100000");

        // Test with space instead of T
        let result = PyDateTimeClass::fromisoformat(args!("2023-01-02 15:30:45.100000"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.isoformat(), "2023-01-02T15:30:45.100000");

        // Test with microseconds and timezone
        let result = PyDateTimeClass::fromisoformat(args!("2023-01-02 15:30:45.100000+01:00"));
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.isoformat(), "2023-01-02T15:30:45.100000+01:00");
    }
}
