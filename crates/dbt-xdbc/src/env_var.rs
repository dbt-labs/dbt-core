use adbc_core::error::{Error, Result, Status};

use std::env;

const TRUE_VALUES: [&str; 4] = ["1", "true", "yes", "on"];
const FALSE_VALUES: [&str; 5] = ["0", "false", "no", "off", ""];

pub fn env_var_bool(var_name: &str) -> Result<bool> {
    match env::var_os(var_name) {
        Some(val) => {
            if TRUE_VALUES.iter().any(|s| val.eq_ignore_ascii_case(s)) {
                Ok(true)
            } else if FALSE_VALUES.iter().any(|s| val.eq_ignore_ascii_case(s)) {
                Ok(false)
            } else {
                let err = Error::with_message_and_status(
                    format!(
                        "Invalid value for environment variable {var_name:?}: {:?}. Expected one of: {} (true) or {} (false).",
                        val,
                        TRUE_VALUES.join(", "),
                        FALSE_VALUES.join(", ")
                    ),
                    Status::InvalidArguments,
                );
                Err(err)
            }
        }
        None => Ok(false),
    }
}
