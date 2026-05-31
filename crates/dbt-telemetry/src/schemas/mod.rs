mod location;
mod otlp;
mod record;
// Trait implementations for span record type events
mod span;
// Trait implementations for log record type events
mod log;

// Re-export proto types for event attributes and top level envelope types directly
// for the outside world
pub use location::*;
pub use log::*;
pub use otlp::*;
pub use record::*;
pub use span::*;
