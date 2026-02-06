//! Rendering-based checker for mangled ref/source calls.
//!
//! A mangled ref/source is one where the `{{ ref() }}` or `{{ source() }}` call
//! is directly adjacent to other content without whitespace separation.
//!
//! Examples of mangled refs:
//! - `foo{{ ref('a') }}` - identifier characters before ref
//! - `{{ ref('a') }}bar` - identifier characters after ref

use std::cell::Cell;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dbt_common::{ErrorCode, io_args::IoArgs, tracing::emit::emit_warn_log_message};
use minijinja::{CodeLocation, listener::RenderingEventListener};

use minijinja::machinery::Span;

/// A recorded ref/source call with its position in the source code.
#[derive(Debug, Clone)]
struct RefSourceInfo {
    ref_type: String,
    /// Location in source for error reporting
    report_line: u32,
    report_col: u32,
    /// Position in source code (byte offset)
    source_start: u32,
    source_end: u32,
}

/// A printer that collects ref/source positions during rendering and checks for mangled refs.
///
/// This works by:
/// 1. Recording ref/source calls with their source positions via `on_ref_or_source`
/// 2. After rendering completes, using macro_spans to map source positions to rendered positions
/// 3. Checking the rendered output around each mapped position
///
/// Implements `RenderingEventListener` to integrate with the rendering system.
#[derive(Debug)]
pub struct MangledRefWarningPrinter {
    /// File path for error reporting
    path: PathBuf,
    /// IO args for emitting warnings
    io_args: IoArgs,
    /// Collected ref/source info during rendering
    ref_source_info: Arc<Mutex<Vec<RefSourceInfo>>>,
    /// Function call depth - only record refs at depth 0 (top level)
    /// This also covers re-rendered template strings from var() since we
    /// increment function_depth around render_str calls.
    function_depth: Cell<u32>,
}

impl MangledRefWarningPrinter {
    /// Create a new warning printer.
    pub fn new(path: PathBuf, io_args: IoArgs) -> Self {
        Self {
            path,
            io_args,
            ref_source_info: Arc::new(Mutex::new(Vec::new())),
            function_depth: Cell::new(0),
        }
    }

    fn emit_warning(&self, info: &RefSourceInfo, message: &str) {
        let location = CodeLocation {
            line: info.report_line,
            col: info.report_col,
            file: self.path.clone(),
        };
        emit_warn_log_message(
            ErrorCode::MangledRef,
            format!(
                "Mangled {}() {}. \
                This may cause unexpected behavior. Add whitespace or use a separator.\n  --> {}\n\
                To suppress this warning, add to dbt_project.yml:\n  \
                custom_checks:\n    analysis.mangled_ref: off",
                info.ref_type, message, location
            ),
            self.io_args.status_reporter.as_ref(),
        );
    }
}

impl RenderingEventListener for MangledRefWarningPrinter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "MangledRefWarningPrinter"
    }

    fn on_macro_start(
        &self,
        _file_path: Option<&std::path::Path>,
        _line: &u32,
        _col: &u32,
        _offset: &u32,
    ) {
        // Not used - we use macro_spans instead
    }

    fn on_macro_stop(
        &self,
        _file_path: Option<&std::path::Path>,
        _line: &u32,
        _col: &u32,
        _offset: &u32,
    ) {
        // Not used - we use macro_spans instead
    }

    fn on_malicious_return(&self, _location: &CodeLocation) {
        self.function_depth
            .set(self.function_depth.get().saturating_sub(1));
    }

    fn on_function_start(&self) {
        self.function_depth.set(self.function_depth.get() + 1);
    }

    fn on_function_end(&self) {
        self.function_depth
            .set(self.function_depth.get().saturating_sub(1));
    }

    fn on_ref_or_source(
        &self,
        name: &str,
        start_line: u32,
        start_col: u32,
        start_offset: u32,
        _end_line: u32,
        _end_col: u32,
        end_offset: u32,
    ) {
        // Only record refs at top level (function_depth == 0)
        // This filters out refs inside macros and re-rendered template strings from var()
        if self.function_depth.get() > 0 {
            return;
        }

        self.ref_source_info.lock().unwrap().push(RefSourceInfo {
            ref_type: name.to_string(),
            report_line: start_line,
            report_col: start_col,
            source_start: start_offset,
            source_end: end_offset,
        });
    }

    fn check_and_emit_mangled_ref_warnings(
        &self,
        rendered_sql: &str,
        macro_spans: &[(Span, Span)],
    ) {
        let infos = self.ref_source_info.lock().unwrap();
        let rendered_bytes = rendered_sql.as_bytes();

        for info in infos.iter() {
            // Find the macro_span that contains this ref/source call
            // The ref/source call is at [info.source_start, info.source_end) in the source
            // Each tuple is (source_span, expanded_span)
            let containing_span = macro_spans.iter().find(|(source_span, _)| {
                info.source_start >= source_span.start_offset
                    && info.source_end <= source_span.end_offset
            });

            let Some((_, expanded_span)) = containing_span else {
                // No containing span found - ref might be in a {% %} block that doesn't produce output
                continue;
            };

            // Check if the span actually produced output
            if expanded_span.start_offset == expanded_span.end_offset {
                // This span didn't produce any output (e.g., used as function argument)
                continue;
            }

            // The rendered position is the span's expanded position
            let rendered_start = expanded_span.start_offset as usize;
            let rendered_end = expanded_span.end_offset as usize;

            // Check character before the ref output in rendered result
            if rendered_start > 0 {
                if let Some(&c) = rendered_bytes.get(rendered_start - 1) {
                    if is_identifier_char(c as char) {
                        self.emit_warning(info, "has non-whitespace content directly before it");
                    }
                }
            }

            // Check character after the ref output in rendered result
            if let Some(&c) = rendered_bytes.get(rendered_end) {
                if is_identifier_char(c as char) {
                    self.emit_warning(info, "has non-whitespace content directly after it");
                }
            }
        }
    }
}

/// Check if a character could be part of a table/identifier name.
/// These are the characters that would cause "mangling" if adjacent to a ref/source output.
fn is_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.'
}
