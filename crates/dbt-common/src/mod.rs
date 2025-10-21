#[macro_use]
pub mod macros;

pub mod adapter;
pub mod atomic;
pub mod cancellation;
pub mod constants;
pub mod hashing;
pub mod io_utils;
pub mod node_selector;
pub mod pretty_string;
pub mod pretty_table;
pub mod stats;
pub mod stdfs;
pub mod string_utils;
pub mod tokiofs;
#[macro_use]
pub extern crate dbt_error as error;
pub use dbt_error::{
    AdapterError, AdapterErrorKind, AdapterResult, AsyncAdapterResult, Cancellable, CodeLocation,
    ErrContext, ErrorCode, FsError, FsResult, LiftableResult, MacroSpan, Span, ectx, err, fs_err,
    into_fs_error, not_implemented_err, unexpected_err, unexpected_fs_err,
};
pub mod behavior_flags;
pub mod embedded_install_scripts;
pub mod io_args;
pub mod logging;
pub mod once_cell_vars;
pub mod row_limit;
pub mod serde_utils;
pub mod time;
pub mod tracing;

// Re-export span creation functions that were previously exported as macros
pub use tracing::{
    create_debug_span, create_debug_span_with_parent, create_info_span,
    create_info_span_with_parent, create_root_info_span,
};

mod discrete_event_emitter;
pub use discrete_event_emitter::DiscreteEventEmitter;
