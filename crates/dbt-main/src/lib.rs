// Re-export the main library functionality
pub mod compilation;
pub mod dbt_lib;
pub mod driver;
pub use driver::{DbtCompilationDriver, DbtTaskExecutionDriver};
pub mod retry;
pub mod vars;

pub mod version_check;

pub use dbt_clap_core::from_lib;

pub mod partial_parse;
mod uninstall;
pub mod update;
mod utils;

mod main_impl;
pub use main_impl::{prepare_cli_or_exit, print_trimmed_error, run_cli};
