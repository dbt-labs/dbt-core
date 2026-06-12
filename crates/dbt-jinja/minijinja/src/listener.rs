//! This module contains the listener trait and its implementations.
//!  

use std::fmt::Write;
use std::path::Path;

use crate::compiler::tokens::Token;
use crate::layout::JinjaLayoutEventKind;
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

    /// Called when a Jinja layout boundary is encountered during rendering.
    fn on_jinja_layout_event(&self, _kind: JinjaLayoutEventKind, _source_span: &Span) {}

    /// Called when a Jinja loop enters a repeated iteration.
    fn on_jinja_loop_iteration_start(&self, _source_span: &Span) {}

    /// Called when a Jinja loop ended without iterating.
    fn on_jinja_loop_skipped_end(&self, _source_span: &Span) {}

    /// Called when raw template text is emitted into rendered output.
    fn on_raw_emit(&self, _raw: &str, _source_span: &Span) {}

    /// Called immediately before a Jinja expression is emitted into rendered output.
    fn on_emit_start(&self, _source_span: &Span) {}

    /// Called immediately after a Jinja expression is emitted into rendered output.
    fn on_emit_end(&self, _source_span: &Span) {}

    /// Called when a malicious return is encountered.
    /// It means return is not on the top level of block
    /// e.g. {{ return(1) + 1 }}
    fn on_malicious_return(&self, _location: &CodeLocation);

    /// Called when a function is being entered.
    fn on_function_start(&self);

    /// Called when a function is being exited.
    fn on_function_end(&self);

    /// Called when a macro is invoked during rendering, to track macro dependencies.
    /// The `template_name` has the form `{package_name}.{macro_name}`.
    fn on_macro_dependency(&self, _template_name: &str) {}

    /// Whether this listener wants macro call-site locations to be tracked.
    /// Capturing the call site clones a path on every call, so the vm only does
    /// so when at least one listener opts in. Defaults to `false`.
    fn tracks_macro_call_sites(&self) -> bool {
        false
    }

    /// Called just before a macro body begins executing.
    ///
    /// - `name` is the qualified `{package}.{macro_name}`.
    /// - `call_site` is where the macro was invoked from (the calling
    ///   template's path and span), when available.
    /// - `def_path`/`def_span` is where the macro is defined.
    ///
    /// Paired with [`on_macro_execute_end`](Self::on_macro_execute_end) around the
    /// body, including when the body returns an error, so callers can maintain a
    /// balanced call stack.
    fn on_macro_execute_start(
        &self,
        _name: &str,
        _call_site: Option<(&Path, &Span)>,
        _def_path: &Path,
        _def_span: &Span,
    ) {
    }

    /// Called after a macro body finishes executing (on both success and error).
    fn on_macro_execute_end(&self, _name: &str) {}

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

    /// Called when a ref() or source() call is resolved to its unique_id
    fn on_ref_or_source_resolved(&self, _unique_id: &str) {}

    /// Called after rendering to check and emit mangled ref warnings.
    /// Only MangledRefWarningPrinter implements this; default is no-op.
    fn check_and_emit_mangled_ref_warnings(
        &self,
        _rendered_sql: &str,
        _macro_spans: &[(Span, Span)],
    ) {
    }
}

/// A listener for tokenizer events emitted during template compilation.
pub trait TokenizerEventListener: std::fmt::Debug {
    /// Called when the tokenizer emits a source token.
    fn on_source_token(&self, token: &Token<'_>, span: &Span);
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
