/// A module for efficiently emitting structured events.
///
/// This module provides nicer API's over using of `tracing::event!` directly.
// Re-export tracing, to allow macro to bring it into scope without the call-site crate
// requiring to declare it as a dependency explictly
pub use tracing as _tracing;

#[macro_export]
macro_rules! emit_tracing_event {
    // Attrs with message => defaults to INFO level
    ($attrs:expr, $($arg:tt)+) => {
        $crate::tracing::event_info::store_event_attributes($attrs);
        $crate::tracing::emit::_tracing::info!($($arg)+);
    };
    // Attrs without a message => defaults to INFO info level & empty message
    ($attrs:expr) => {
        $crate::tracing::event_info::store_event_attributes($attrs);
        $crate::tracing::emit::_tracing::info!("");
    };
    // Level with attrs and message
    (level: $lvl:expr, $attrs:expr, $($arg:tt)+) => {
        $crate::tracing::event_info::store_event_attributes($attrs);
        $crate::tracing::emit::_tracing::event!($lvl, $($arg)+);
    };
    // Level with attrs without a message
    (level: $lvl:expr, $attrs:expr) => {
        $crate::tracing::event_info::store_event_attributes($attrs);
        $crate::tracing::emit::_tracing::event!($lvl, "");
    };
}
