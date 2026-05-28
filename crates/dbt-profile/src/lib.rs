//! Lightweight dbt `profiles.yml` resolver with Jinja `env_var`/`var` support.
//!
//! Standalone — no dependency on `dbt-jinja-utils` or `dbt-common`.
//! Suitable for `dbt-db-runner`, MCP tools, or any consumer that needs
//! profile credentials without pulling in the full dbt compilation stack.
//!
//! # Usage
//!
//! ```rust,no_run
//! use dbt_profile::{ResolveArgs, resolve};
//!
//! let profile = resolve(&ResolveArgs {
//!     project_dir: Some("/path/to/project".into()),
//!     ..Default::default()
//! }).unwrap();
//! println!("{}: {}", profile.adapter_type, profile.credentials_json().unwrap());
//! ```

mod error;
mod jinja;
mod resolve;

pub use error::{ProfileError, Result};
pub use resolve::{
    ProfileEnvironment, ResolveArgs, ResolvedProfile, find_profiles_path, render_target, resolve,
    resolve_target, resolve_with_env,
};
