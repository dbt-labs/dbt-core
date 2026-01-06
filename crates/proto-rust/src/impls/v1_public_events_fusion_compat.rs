use crate::v1::public::events::fusion::compat::SeverityNumber;
use std::cmp;

/// Convert our proto defined severity level to OpenTelemetry severity text.
impl SeverityNumber {
    pub fn as_str(&self) -> &'static str {
        match self {
            SeverityNumber::Unspecified => "UNSPECIFIED",
            SeverityNumber::Trace => "TRACE",
            SeverityNumber::Debug => "DEBUG",
            SeverityNumber::Info => "INFO",
            SeverityNumber::Warn => "WARN",
            SeverityNumber::Error => "ERROR",
        }
    }
}

impl From<tracing::Level> for SeverityNumber {
    fn from(level: tracing::Level) -> Self {
        match level {
            tracing::Level::TRACE => SeverityNumber::Trace,
            tracing::Level::DEBUG => SeverityNumber::Debug,
            tracing::Level::INFO => SeverityNumber::Info,
            tracing::Level::WARN => SeverityNumber::Warn,
            tracing::Level::ERROR => SeverityNumber::Error,
        }
    }
}

impl From<&tracing::Level> for SeverityNumber {
    fn from(value: &tracing::Level) -> Self {
        Self::from(*value)
    }
}

impl TryInto<tracing::Level> for SeverityNumber {
    type Error = &'static str;

    fn try_into(self) -> Result<tracing::Level, &'static str> {
        Ok(match self {
            SeverityNumber::Trace => tracing::Level::TRACE,
            SeverityNumber::Debug => tracing::Level::DEBUG,
            SeverityNumber::Info => tracing::Level::INFO,
            SeverityNumber::Warn => tracing::Level::WARN,
            SeverityNumber::Error => tracing::Level::ERROR,
            SeverityNumber::Unspecified => {
                return Err("Cannot convert UNSPECIFIED severity to tracing::Level");
            }
        })
    }
}

impl PartialEq<tracing::Level> for SeverityNumber {
    fn eq(&self, other: &tracing::Level) -> bool {
        TryInto::<tracing::Level>::try_into(*self)
            .map(|level| level == *other)
            .unwrap_or(false)
    }
}

impl PartialOrd<tracing::Level> for SeverityNumber {
    fn partial_cmp(&self, other: &tracing::Level) -> Option<cmp::Ordering> {
        TryInto::<tracing::Level>::try_into(*self)
            .ok()
            .and_then(|level| level.partial_cmp(other))
    }
}

impl PartialEq<SeverityNumber> for tracing::Level {
    fn eq(&self, other: &SeverityNumber) -> bool {
        other == self
    }
}

impl PartialOrd<SeverityNumber> for tracing::Level {
    fn partial_cmp(&self, other: &SeverityNumber) -> Option<cmp::Ordering> {
        other.partial_cmp(self).map(|ord| ord.reverse())
    }
}

impl PartialEq<tracing::level_filters::LevelFilter> for SeverityNumber {
    fn eq(&self, other: &tracing::level_filters::LevelFilter) -> bool {
        TryInto::<tracing::Level>::try_into(*self)
            .map(|level| level == *other)
            .unwrap_or(false)
    }
}

impl PartialOrd<tracing::level_filters::LevelFilter> for SeverityNumber {
    fn partial_cmp(&self, other: &tracing::level_filters::LevelFilter) -> Option<cmp::Ordering> {
        TryInto::<tracing::Level>::try_into(*self)
            .ok()
            .and_then(|level| level.partial_cmp(other))
    }
}

impl PartialEq<SeverityNumber> for tracing::level_filters::LevelFilter {
    fn eq(&self, other: &SeverityNumber) -> bool {
        other == self
    }
}

impl PartialOrd<SeverityNumber> for tracing::level_filters::LevelFilter {
    fn partial_cmp(&self, other: &SeverityNumber) -> Option<cmp::Ordering> {
        other.partial_cmp(self).map(|ord| ord.reverse())
    }
}
