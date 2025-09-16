use std::env;

pub fn is_update_golden_files_mode() -> bool {
    env::var("GOLDIE_UPDATE").ok().is_some_and(|val| val == "1")
}

pub fn is_continuous_integration_environment() -> bool {
    env::var("CONTINUOUS_INTEGRATION").is_ok()
}
