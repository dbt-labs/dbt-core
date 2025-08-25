mod constants;
mod location;
mod otlp;
mod record;
// Span record type attribute schemas
mod span;
// Event record type attribute schemas
mod event;

// Expose all schema directly for the outside world
pub use event::*;
pub use location::*;
pub use otlp::*;
pub use record::*;
pub use span::*;
