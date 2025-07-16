use std::env;

pub fn is_update_golden_files_mode() -> bool {
    env::var("GOLDIE_UPDATE").unwrap_or("0".to_string()) == "1"
}

pub fn is_continuous_integration_environment() -> bool {
    env::var("CONTINUOUS_INTEGRATION").is_ok()
}
