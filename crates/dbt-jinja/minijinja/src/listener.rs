//! This module contains the listener trait and its implementations.
//!  

use std::path::Path;

use crate::CodeLocation;

/// A listener for rendering events. This is used for LSP
pub trait RenderingEventListener: std::fmt::Debug {
    /// Returns the listener as an `Any` trait object.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns the name of the listener.
    fn name(&self) -> &str;

    /// Called when a macro start is encountered.
    #[allow(clippy::too_many_arguments)]
    fn on_macro_start(
        &self,
        _file_path: Option<&Path>,
        _line: &u32,
        _col: &u32,
        _offset: &u32,
        _expanded_line: &u32,
        _expanded_col: &u32,
        _expanded_offset: &u32,
    );

    /// Called when a macro stop is encountered.
    #[allow(clippy::too_many_arguments)]
    fn on_macro_stop(
        &self,
        _file_path: Option<&Path>,
        _line: &u32,
        _col: &u32,
        _offset: &u32,
        _expanded_line: &u32,
        _expanded_col: &u32,
        _expanded_offset: &u32,
    );

    /// Called when a malicious return is encountered.
    /// It means return is not on the top level of block
    /// e.g. {{ return(1) + 1 }}
    fn on_malicious_return(&self, _location: &CodeLocation);

    /// Called when a function is being entered.
    fn on_function_start(&self);

    /// Called when a function is being exited.
    fn on_function_end(&self);
}

/// A macro start event.
#[derive(Debug, Clone)]
pub struct MacroStart {
    /// The line number of the macro start.
    pub line: u32,
    /// The column number of the macro start.
    pub col: u32,
    /// The offset of the macro start.
    pub offset: u32,
    /// The line number of the expanded macro start.
    pub expanded_line: u32,
    /// The column number of the expanded macro start.
    pub expanded_col: u32,
    /// The offset of the expanded macro start.
    pub expanded_offset: u32,
}
