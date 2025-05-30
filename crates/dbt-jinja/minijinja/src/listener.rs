//! This module contains the listener trait and its implementations.
//!  

use std::path::Path;

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
    fn on_macro_start(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32) {}

    /// Called when a macro stop is encountered.
    fn on_macro_stop(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32) {}

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

impl RenderingEventListener for () {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DummyRenderingEventListener"
    }
}

/// default implementation of RenderingEventListener
#[derive(Default, Debug)]
pub struct DefaultRenderingEventListener;

impl RenderingEventListener for DefaultRenderingEventListener {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DefaultRenderingEventListener"
    }
}
