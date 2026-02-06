//! This module contains the listener trait and its implementations.
//!  

use std::fmt::Write;
use std::path::Path;

use crate::output_tracker::OutputTracker;
use crate::{machinery::Span, CodeLocation};

/// A listener for rendering events. This is used for LSP
pub trait RenderingEventListener: std::fmt::Debug {
    /// Returns the listener as an `Any` trait object.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns the name of the listener.
    fn name(&self) -> &str;

    /// Creates an OutputTracker for the given writer.
    /// If this listener tracks macro spans, it will use its internal location tracker.
    /// Otherwise, a plain OutputTracker is created.
    fn create_output_tracker<'a>(&self, _w: &'a mut (dyn Write + 'a)) -> Option<OutputTracker<'a>> {
        None
    }

    /// Called when a macro start is encountered.
    /// The expanded location can be obtained from the output_tracker_location if needed.
    fn on_macro_start(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32);

    /// Called when a macro stop is encountered.
    /// The expanded location can be obtained from the output_tracker_location if needed.
    fn on_macro_stop(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32);

    /// Called when a malicious return is encountered.
    /// It means return is not on the top level of block
    /// e.g. {{ return(1) + 1 }}
    fn on_malicious_return(&self, _location: &CodeLocation);

    /// Called when a function is being entered.
    fn on_function_start(&self);

    /// Called when a function is being exited.
    fn on_function_end(&self);

    /// Called when a ref() or source() call is rendered.
    /// This is used to detect mangled refs by checking if there are
    /// non-whitespace characters adjacent to the ref/source span.
    #[allow(clippy::too_many_arguments)]
    fn on_ref_or_source(
        &self,
        _name: &str,
        _start_line: u32,
        _start_col: u32,
        _start_offset: u32,
        _end_line: u32,
        _end_col: u32,
        _end_offset: u32,
    ) {
    }

    /// Called after rendering to check and emit mangled ref warnings.
    /// Only MangledRefWarningPrinter implements this; default is no-op.
    fn check_and_emit_mangled_ref_warnings(
        &self,
        _rendered_sql: &str,
        _macro_spans: &[(Span, Span)],
    ) {
    }
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
