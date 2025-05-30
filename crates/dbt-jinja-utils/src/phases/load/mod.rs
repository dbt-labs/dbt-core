//! This module contains the rendering functionality for the load phase.

mod render_project_scope;
mod render_secret_scope;

pub mod init;
pub mod secret_renderer;

pub use render_project_scope::RenderProjectScope;
pub use render_secret_scope::RenderSecretScope;
