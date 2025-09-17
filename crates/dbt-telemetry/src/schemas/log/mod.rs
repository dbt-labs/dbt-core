// Expose inner modules within the crate for relative imports
pub(crate) mod artifact;
pub(crate) mod log_message;

// Re-export all schemas from proto_rust directly for the outside world
pub use artifact::*;
pub use log_message::*;
