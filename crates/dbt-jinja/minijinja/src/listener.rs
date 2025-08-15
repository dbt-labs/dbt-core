//! This module contains the listener trait and its implementations.
//!  

use std::{cell::RefCell, path::Path};

use crate::{machinery::Span, MacroSpans};

/// A listener for rendering events. This is used for LSP
pub trait RenderingEventListener: std::fmt::Debug {
    /// Returns the listener as an `Any` trait object.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns the name of the listener.
    fn name(&self) -> &str;

    /// Called when a definition is encountered.
    fn on_definition(&self, _name: &str) {}

    /// Called when a reference is encountered.
    fn on_reference(&self, _name: &str) {}

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
    ) {
    }

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
    ) {
    }

    /// Called when a model reference is encountered.
    #[allow(clippy::too_many_arguments)]
    fn on_model_reference(
        &self,
        _name: &str,
        _start_line: &u32,
        _start_col: &u32,
        _start_offset: &u32,
        _end_line: &u32,
        _end_col: &u32,
        _end_offset: &u32,
    ) {
    }

    /// Called when a model source reference is encountered.
    #[allow(clippy::too_many_arguments)]
    fn on_model_source_reference(
        &self,
        _name: &str,
        _start_line: &u32,
        _start_col: &u32,
        _start_offset: &u32,
        _end_line: &u32,
        _end_col: &u32,
        _end_offset: &u32,
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

/// default implementation of RenderingEventListener
#[derive(Debug, Default)]
pub struct DefaultRenderingEventListener {
    /// macro spans
    pub macro_spans: RefCell<MacroSpans>,

    /// inner Vec<MacroStart> means one evaluation
    /// Vec<Vec<MacroStart>> means nested evaluations
    macro_start_stack: RefCell<Vec<MacroStart>>,

    /// Set of macro names that were called during rendering
    pub macro_calls: RefCell<std::collections::HashSet<String>>,
}

impl RenderingEventListener for DefaultRenderingEventListener {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DefaultRenderingEventListener"
    }

    fn on_macro_start(
        &self,
        _file_path: Option<&Path>,
        line: &u32,
        col: &u32,
        offset: &u32,
        expanded_line: &u32,
        expanded_col: &u32,
        expanded_offset: &u32,
    ) {
        self.macro_start_stack.borrow_mut().push(MacroStart {
            line: *line,
            col: *col,
            offset: *offset,
            expanded_line: *expanded_line,
            expanded_col: *expanded_col,
            expanded_offset: *expanded_offset,
        });
    }

    fn on_macro_stop(
        &self,
        _file_path: Option<&Path>,
        line: &u32,
        col: &u32,
        offset: &u32,
        expanded_line: &u32,
        expanded_col: &u32,
        expanded_offset: &u32,
    ) {
        let mut macro_start_stack = self.macro_start_stack.borrow_mut();
        let macro_start_stack_length = macro_start_stack.len();
        if macro_start_stack_length == 1 {
            let macro_start = macro_start_stack.pop().unwrap();
            self.macro_spans.borrow_mut().push(
                Span {
                    start_line: macro_start.line,
                    start_col: macro_start.col,
                    start_offset: macro_start.offset,
                    end_line: *line,
                    end_col: *col,
                    end_offset: *offset,
                },
                Span {
                    start_line: macro_start.expanded_line,
                    start_col: macro_start.expanded_col,
                    start_offset: macro_start.expanded_offset,
                    end_line: *expanded_line,
                    end_col: *expanded_col,
                    end_offset: *expanded_offset,
                },
            );
        } else {
            macro_start_stack.pop();
        }
    }

    fn on_reference(&self, name: &str) {
        // Track all function/macro calls
        self.macro_calls.borrow_mut().insert(name.to_string());
    }
}
