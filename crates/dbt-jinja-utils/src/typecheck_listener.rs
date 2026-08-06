use dbt_common::tracing::dbt_emit::emit_warn_log_from_fs_error;
use dbt_common::{CodeLocationWithFile, ErrorCode, FsError};
use minijinja::TypecheckingEventListener;
use minijinja::machinery::Span;
use std::path::PathBuf;

/// A typechecking listener that emits warnings through tracing.
pub struct YamlTypecheckingEventListener {
    current_path: PathBuf,
    current_span: Span,
}

impl YamlTypecheckingEventListener {
    /// Creates a new YamlTypecheckingEventListener
    ///
    /// # Arguments
    ///
    /// * `current_path` - The path to the current file being typechecked
    /// * `current_span` - The span context for error reporting
    pub fn new(current_path: PathBuf, current_span: Span) -> Self {
        Self {
            current_path,
            current_span,
        }
    }
}

impl TypecheckingEventListener for YamlTypecheckingEventListener {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    // Avoid normal warning getting to user when we typecheck yaml
    fn warn(&self, _message: &str) {}

    fn warn_filter(&self, message: &str) {
        // Adjust span with current_span offset
        let adjusted_span = self.current_span;

        let location = CodeLocationWithFile::new(
            adjusted_span.start_line,
            adjusted_span.start_col,
            adjusted_span.start_offset,
            self.current_path.clone(),
        );

        let fs_error =
            FsError::new(ErrorCode::JinjaError, message.to_string()).with_location(location);
        emit_warn_log_from_fs_error(fs_error);
    }

    fn set_span(&self, _span: &Span) {
        // Span is already set in the listener
    }

    fn new_block(&self, _block_id: usize) {
        // Not needed for YAML typechecking
    }

    fn on_model_reference(
        &self,
        _name: &str,
        _identifier_span: &Span,
        _start_line: &u32,
        _start_col: &u32,
        _start_offset: &u32,
        _end_line: &u32,
        _end_col: &u32,
        _end_offset: &u32,
    ) {
        // Not needed for YAML typechecking
    }

    fn flush(&self) {
        // Not needed for YAML typechecking
    }

    fn on_lookup(&self, _span: &Span, _name: &str, _kind: &str, _dependencies: Vec<Span>) {
        // Not needed for YAML typechecking
    }
}
