/// Invalid values
#[macro_export]
macro_rules! invalid_value {
    ($msg:expr) => {
        Err(AdapterError::new(AdapterErrorKind::UnexpectedResult, $msg))
    };

    ($($arg:tt)*) => {
        Err(AdapterError::new(AdapterErrorKind::UnexpectedResult, format!($($arg)*)))
    };
}
