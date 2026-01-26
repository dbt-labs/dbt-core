use adbc_core::error::{Error, Result, Status};

pub fn env_var_bool(var_name: &str) -> Result<bool> {
    dbt_env::env_var_bool(var_name)
        .map_err(|msg| Error::with_message_and_status(msg, Status::InvalidArguments))
}
