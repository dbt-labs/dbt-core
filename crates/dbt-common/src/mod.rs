#[macro_use]
pub mod macros;

pub mod adapter;
pub mod atomic;
pub mod clap_cli;
pub mod constants;
pub mod error;
pub mod error_counter;
pub mod init;
pub mod io_utils;
pub mod node_selector;
pub mod pretty_string;
pub mod pretty_table;
pub mod profile_setup;
pub mod stats;
pub mod stdfs;
pub mod string_utils;
pub mod tokiofs;
pub use error::{CodeLocation, ErrContext, ErrorCode, FsError, FsResult, Span};
pub mod behavior_flags;
pub mod embedded_install_scripts;
pub mod io_args;
pub mod logging;
pub mod once_cell_vars;
pub mod row_limit;
pub mod serde_utils;
pub mod time;
pub mod tracing;

// ------------------------------------------------------------------------------------------------
// todo: get rid of this SDF remains

pub fn sdf_debug_level() -> i32 {
    std::env::var("SDF_DEBUG")
        .as_ref()
        .map(String::as_str)
        .map(str::parse::<i32>)
        .map(Result::unwrap_or_default)
        .unwrap_or_default()
}

pub fn is_sdf_debug() -> bool {
    sdf_debug_level() > 0
}
