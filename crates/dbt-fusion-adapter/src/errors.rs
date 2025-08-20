use adbc_core::error::Status;
use arrow_schema::ArrowError;
use dbt_auth::AuthError;
use dbt_common::cancellation::Cancellable;
use dbt_common::{ErrorCode, FsError};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind};
use std::io;
use std::pin::Pin;
use std::{fmt, panic};
use tokio::task::JoinError;

pub type AdapterResult<T> = Result<T, AdapterError>;

/// A pinned Future that produces a `Result<T, Cancellable<AdapterError>>`.
pub type AsyncAdapterResult<'a, T, E = AdapterError> =
    Pin<Box<dyn Future<Output = Result<T, Cancellable<E>>> + Send + 'a>>;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AdapterErrorKind {
    /// Internal error
    Internal,
    /// SQL execution error
    SqlExecution,
    /// Configuration-related error
    Configuration,
    /// XDBC error (mostly ADBC)
    Xdbc(Status),
    /// Arrow error
    Arrow,
    /// Unexpected result
    UnexpectedResult,
    /// Unexpected Database Ref
    UnexpectedDbReference,
    /// Cancelled operation
    Cancelled,
    /// Missing information
    Incomplete,
    /// Unsupported type
    UnsupportedType,
    /// Input/Output error
    Io,
    /// JSON ser/deserialization error
    SerdeJSON,
    /// Replay of an error
    Replay,
    /// Not supported
    NotSupported,
}

impl AdapterErrorKind {
    fn description(&self) -> &'static str {
        match self {
            Self::Internal => "Internal error",
            Self::SqlExecution => "SQL execution error",
            Self::Configuration => "Configuration error",
            Self::Xdbc(_) => "ADBC error",
            Self::Arrow => "Arrow error",
            Self::UnexpectedResult => "Unexpected result",
            Self::UnexpectedDbReference => "Unexpected database reference",
            Self::Cancelled => "Operation was cancelled",
            Self::Incomplete => "Incomplete data",
            Self::UnsupportedType => "Unsupported type",
            Self::Io => "Input/output",
            Self::SerdeJSON => "JSON",
            Self::Replay => "Replay error",
            Self::NotSupported => "Not supported",
        }
    }
}

impl fmt::Display for AdapterErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

/// Adapter error.
#[derive(Debug, Clone)]
pub struct AdapterError {
    kind: AdapterErrorKind,
    message: String,
    /// SQLSTATE code from database operations.
    ///
    /// Use [AdapterError::sqlstate()] to get the string representation.
    sqlstate: [u8; 5],
    /// Vendor-specific error code, if applicable.
    vendor_code: Option<i32>,
}

impl AdapterError {
    /// Create new error.
    pub fn new(kind: AdapterErrorKind, msg: impl Into<String>) -> Self {
        Self {
            kind,
            message: msg.into(),
            sqlstate: [b'0'; 5],
            vendor_code: None,
        }
    }

    pub fn kind(&self) -> AdapterErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        let stripped_message = if matches!(self.kind, AdapterErrorKind::Xdbc(_)) {
            // Remove prefixes like "Unknown: " or "Internal: " which don't
            // add any informational value to the error message.
            self.message
                .strip_prefix("Unknown: ")
                .or_else(|| self.message.strip_prefix("Internal: "))
        } else {
            None
        };
        stripped_message.unwrap_or(&self.message)
    }

    /// Get SQLSTATE as an ASCII string.
    ///
    /// Error codes defined by the SQL standard and vendor implementations [1][2].
    ///
    /// [1] https://en.wikipedia.org/wiki/SQLSTATE
    /// [2] https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes
    pub fn sqlstate(&self) -> &str {
        // SQLSTATE is an ASCII string, so we can convert
        // it to a str without allocating a new string.
        let res = std::str::from_utf8(&self.sqlstate);
        debug_assert!(
            res.is_ok(),
            "SQLSTATE is not valid ASCII: {:?}",
            &self.sqlstate
        );
        res.unwrap_or("")
    }

    pub fn vendor_code(&self) -> Option<i32> {
        self.vendor_code
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = self.message();
        if message.is_empty() {
            write!(f, "{}", self.kind)?;
        } else {
            // Some error kinds do not have to be part of the message because the error
            // message is already descriptive enough and prefixed with context like
            // "[Snowflake] ...".
            match self.kind {
                AdapterErrorKind::Xdbc(_) => write!(f, "{message}")?,
                _ => write!(f, "{}: {message}", self.kind)?,
            }
        }
        let sqlstate: &str = self.sqlstate();
        if sqlstate != "00000" || self.vendor_code.is_some() {
            write!(f, " (SQLSTATE: {sqlstate}")?;
            if let Some(vendor_code) = self.vendor_code {
                write!(f, ", Vendor code: {vendor_code}")?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl std::error::Error for AdapterError {}

// Convert AdapterError to MinijinjaError to enable bridge to report
// errors easily
impl From<AdapterError> for MinijinjaError {
    fn from(err: AdapterError) -> Self {
        MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            format!("AdapterError: {err}"),
        )
    }
}

impl From<adbc_core::error::Error> for AdapterError {
    fn from(err: adbc_core::error::Error) -> Self {
        let sqlstate: [u8; 5] = {
            // Transmute SQLSTATE to unsigned bytes. It was mistake to make this i8
            // in ADBC core [1].
            //
            // [1] https://github.com/apache/arrow-adbc/pull/1725#discussion_r1567531539
            let unsigned: [u8; 5] = unsafe { std::mem::transmute(err.sqlstate) };
            if unsigned[0] == 0 {
                // If the string is full of '\0' bytes, we set it to "00000" (b'0' is 48).
                [b'0'; 5]
            } else {
                unsigned
            }
        };
        // This special vendor code is used to indicate that the error information
        // lives in the `private_data` field and not in the vendor_code.
        const ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA: i32 = -2147483648;
        // XXX: should 0 become Some(0) instead of None?
        let vendor_code = if [0, -1, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA].contains(&err.vendor_code)
        {
            None
        } else {
            Some(err.vendor_code)
        };

        AdapterError {
            kind: AdapterErrorKind::Xdbc(err.status),
            message: err.message,
            sqlstate,
            vendor_code,
        }
    }
}

impl From<ArrowError> for AdapterError {
    fn from(err: ArrowError) -> Self {
        AdapterError::new(AdapterErrorKind::Arrow, err.to_string())
    }
}

impl From<MinijinjaError> for AdapterError {
    fn from(err: MinijinjaError) -> Self {
        AdapterError::new(AdapterErrorKind::Configuration, err.to_string())
    }
}

impl From<io::Error> for AdapterError {
    fn from(err: io::Error) -> Self {
        AdapterError::new(AdapterErrorKind::Io, err.to_string())
    }
}

impl From<parquet::errors::ParquetError> for AdapterError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        AdapterError::new(AdapterErrorKind::Io, err.to_string())
    }
}

impl From<serde_json::Error> for AdapterError {
    fn from(err: serde_json::Error) -> Self {
        AdapterError::new(AdapterErrorKind::SerdeJSON, err.to_string())
    }
}

impl From<JoinError> for AdapterError {
    fn from(err: JoinError) -> Self {
        if err.is_cancelled() {
            AdapterError::new(AdapterErrorKind::Cancelled, "")
        } else if err.is_panic() {
            panic::resume_unwind(err.into_panic());
        } else {
            // as of today, this is unreachable, but we keep it for future-proofing
            AdapterError::new(AdapterErrorKind::Internal, err.to_string())
        }
    }
}

impl From<AuthError> for AdapterError {
    fn from(err: AuthError) -> Self {
        match err {
            AuthError::Adbc(adbc_err) => adbc_err.into(),
            AuthError::Config(msg) => AdapterError::new(AdapterErrorKind::Configuration, msg),
            AuthError::JSON(json_err) => json_err.into(),
            AuthError::Io(io_err) => io_err.into(),
        }
    }
}

impl From<AdapterError> for Box<FsError> {
    fn from(err: AdapterError) -> Self {
        // TODO: this error code is too generic
        Box::new(FsError::new(ErrorCode::Generic, format!("{err}")))
    }
}

pub fn into_fs_error(err: Cancellable<AdapterError>) -> Box<FsError> {
    match err {
        Cancellable::Cancelled => {
            let e = FsError::new(
                ErrorCode::OperationCanceled,
                "Adapter operation was cancelled",
            );
            Box::new(e)
        }
        Cancellable::Error(err) => err.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Error as AdbcError;
    use adbc_core::error::Status;

    #[test]
    fn test_adapter_error_from_adbc() {
        let adbc_err = AdbcError::with_message_and_status("Test ADBC error", Status::Internal);
        let err: AdapterError = adbc_err.into();
        assert_eq!(err.kind(), AdapterErrorKind::Xdbc(Status::Internal));
        assert_eq!(err.to_string(), "Test ADBC error");

        let adbc_err =
            AdbcError::with_message_and_status("Internal: Test ADBC error", Status::Internal);
        let err: AdapterError = adbc_err.into();
        assert_eq!(err.kind(), AdapterErrorKind::Xdbc(Status::Internal));
        assert_eq!(err.to_string(), "Test ADBC error");

        let adbc_err =
            AdbcError::with_message_and_status("Unknown: Test ADBC error", Status::Unknown);
        let err: AdapterError = adbc_err.into();
        assert_eq!(err.kind(), AdapterErrorKind::Xdbc(Status::Unknown));
        assert_eq!(err.to_string(), "Test ADBC error");
    }

    #[test]
    fn test_adapter_error_from_adbc_with_sqlstate() {
        let mut adbc_err =
            AdbcError::with_message_and_status("Internal: Test ADBC error", Status::Internal);
        adbc_err.sqlstate = [b'H' as i8, b'Y' as i8, b'1' as i8, b'0' as i8, b'7' as i8];

        let err: AdapterError = adbc_err.into();
        assert_eq!(err.kind(), AdapterErrorKind::Xdbc(Status::Internal));
        assert_eq!(err.to_string(), "Test ADBC error (SQLSTATE: HY107)");
    }

    #[test]
    fn test_adapter_error_from_adbc_with_vendor_code() {
        let mut adbc_err =
            AdbcError::with_message_and_status("Internal: Test ADBC error", Status::Internal);
        adbc_err.vendor_code = 1234;

        let err: AdapterError = adbc_err.into();
        assert_eq!(err.kind(), AdapterErrorKind::Xdbc(Status::Internal));
        assert_eq!(
            err.to_string(),
            "Test ADBC error (SQLSTATE: 00000, Vendor code: 1234)"
        );
    }

    #[test]
    fn test_adapter_error_from_adbc_with_sqlstate_and_vendor_code() {
        let mut adbc_err =
            AdbcError::with_message_and_status("Internal: Test ADBC error", Status::Internal);
        adbc_err.sqlstate = [b'H' as i8, b'Y' as i8, b'1' as i8, b'0' as i8, b'7' as i8];
        adbc_err.vendor_code = 1234;

        let err: AdapterError = adbc_err.into();
        assert_eq!(err.kind(), AdapterErrorKind::Xdbc(Status::Internal));
        assert_eq!(
            err.to_string(),
            "Test ADBC error (SQLSTATE: HY107, Vendor code: 1234)"
        );
    }
}
