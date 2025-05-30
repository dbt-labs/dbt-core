mod code_location;
mod codes;
mod name_candidate;
mod preprocessor_location;
mod types;

// reuse the following types outside of this module
pub use code_location::{AbstractLocation, AbstractSpan, CodeLocation, Span};
pub use codes::ErrorCode;
pub use codes::Warnings;
pub use preprocessor_location::MacroSpan;
pub use types::{
    ContextableResult, ErrContext, FsError, FsResult, GenericNameError, LiftableResult,
    MAX_DISPLAY_TOKENS,
};
