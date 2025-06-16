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
}

/// default implementation of RenderingEventListener
#[derive(Default, Debug)]
pub struct DefaultRenderingEventListener {
    /// macro spans
    pub macro_spans: RefCell<MacroSpans>,
    #[allow(clippy::type_complexity)]
    macro_start_stack: RefCell<Vec<(u32, u32, u32, u32, u32, u32)>>,
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
        self.macro_start_stack.borrow_mut().push((
            *line,
            *col,
            *offset,
            *expanded_line,
            *expanded_col,
            *expanded_offset,
        ));
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
        let (
            source_line,
            source_col,
            source_offset,
            expanded_start_line,
            expanded_start_col,
            expanded_start_offset,
        ) = self.macro_start_stack.borrow_mut().pop().unwrap();
        if self.macro_start_stack.borrow().is_empty() {
            self.macro_spans.borrow_mut().push(
                Span {
                    start_line: source_line,
                    start_col: source_col,
                    start_offset: source_offset,
                    end_line: *line,
                    end_col: *col,
                    end_offset: *offset,
                },
                Span {
                    start_line: expanded_start_line,
                    start_col: expanded_start_col,
                    start_offset: expanded_start_offset,
                    end_line: *expanded_line,
                    end_col: *expanded_col,
                    end_offset: *expanded_offset,
                },
            );
        }
    }

    fn on_reference(&self, name: &str) {
        if name == "return" {
            self.macro_start_stack.borrow_mut().pop();
        }
    }
}
