// Expose inner modules within the crate for relative imports
pub(crate) mod artifact;
pub(crate) mod deps;
pub(crate) mod dev;
pub(crate) mod invocation;
pub(crate) mod node;
pub(crate) mod onboarding;
pub(crate) mod phase;
pub(crate) mod process;
pub(crate) mod query;
pub(crate) mod update;

// Re-export all schemas from proto_rust directly for the outside world
pub use artifact::*;
pub use deps::*;
pub use dev::*;
pub use invocation::*;
pub use node::*;
pub use onboarding::*;
pub use phase::*;
pub use process::*;
pub use query::*;
pub use update::*;
