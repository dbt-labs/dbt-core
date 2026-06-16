use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::Write as _,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use minijinja::{
    CodeLocation, JinjaLayoutEventKind, MacroSpans, OutputTracker, OutputTrackerLocation,
    TypecheckingEventListener,
    listener::{MacroStart, RenderingEventListener},
    machinery::Span,
};

use dbt_common::{
    ErrorCode,
    io_args::IoArgs,
    tracing::dbt_emit::{emit_error_log_message, emit_warn_log_message},
};

/// Trait for creating and destroying rendering event listeners
pub trait RenderingEventListenerFactory: Send + Sync {
    /// Creates new rendering event listeners
    fn create_listeners(
        &self,
        filename: &Path,
        offset: &dbt_frontend_common::error::CodeLocation,
    ) -> Vec<Rc<dyn RenderingEventListener>>;

    /// Destroys a rendering event listener
    fn destroy_listener(&self, _filename: &Path, _listener: Rc<dyn RenderingEventListener>);

    /// Creates rendering and tokenizer listeners for the same render.
    fn create_listener_bundle(
        &self,
        filename: &Path,
        offset: &dbt_frontend_common::error::CodeLocation,
        _source_sql: &str,
    ) -> Vec<Rc<dyn RenderingEventListener>> {
        self.create_listeners(filename, offset)
    }

    /// get macro spans
    fn drain_macro_spans(&self, filename: &Path) -> MacroSpans;
}

/// Default implementation of the `ListenerFactory` trait
#[derive(Default, Debug)]
pub struct DefaultRenderingEventListenerFactory {
    /// Suppress malicious return warning
    pub quiet: bool,
    /// macro spans
    pub macro_spans: Arc<RwLock<HashMap<PathBuf, MacroSpans>>>,
    /// Whether to check for mangled refs
    pub check_mangled_refs: bool,
    /// IO args for warning emission
    pub io_args: IoArgs,
}

impl DefaultRenderingEventListenerFactory {
    /// Creates a new rendering event listener factory
    pub fn new(quiet: bool) -> Self {
        Self {
            quiet,
            macro_spans: Arc::new(RwLock::new(HashMap::new())),
            check_mangled_refs: false,
            io_args: IoArgs::default(),
        }
    }

    /// Creates a new rendering event listener factory with mangled ref checking
    pub fn with_mangled_ref_checking(quiet: bool, io_args: IoArgs) -> Self {
        Self {
            quiet,
            macro_spans: Arc::new(RwLock::new(HashMap::new())),
            check_mangled_refs: true,
            io_args,
        }
    }
}

impl RenderingEventListenerFactory for DefaultRenderingEventListenerFactory {
    /// Creates new rendering event listeners
    fn create_listeners(
        &self,
        filename: &Path,
        _offset: &dbt_frontend_common::error::CodeLocation,
    ) -> Vec<Rc<dyn RenderingEventListener>> {
        let mut listeners: Vec<Rc<dyn RenderingEventListener>> = vec![];

        if self.check_mangled_refs {
            // Share the output tracker location so MangledRefWarningPrinter can observe
            // the current render position at the moment on_ref_or_source fires.
            let shared_tracker = Rc::new(OutputTrackerLocation::default());
            listeners.push(Rc::new(DefaultRenderingEventListener::with_tracker(
                self.quiet,
                shared_tracker.clone(),
            )));
            listeners.push(Rc::new(crate::mangled_ref::MangledRefWarningPrinter::new(
                filename.to_path_buf(),
                self.io_args.clone(),
                shared_tracker,
            )));
        } else {
            // Always add the default listener for macro spans
            listeners.push(Rc::new(DefaultRenderingEventListener::new(self.quiet)));
        }

        listeners
    }

    fn destroy_listener(&self, filename: &Path, listener: Rc<dyn RenderingEventListener>) {
        if let Some(default_listener) = listener
            .as_any()
            .downcast_ref::<DefaultRenderingEventListener>()
        {
            let new_macro_spans = default_listener.macro_spans.borrow().clone();
            if let Ok(mut macro_spans) = self.macro_spans.write() {
                macro_spans.insert(filename.to_path_buf(), new_macro_spans);
            } else {
                emit_error_log_message(
                    ErrorCode::Generic,
                    "Failed to acquire write lock on macro_spans",
                    None,
                );
            }
        }
    }

    fn drain_macro_spans(&self, filename: &Path) -> MacroSpans {
        if let Ok(mut spans) = self.macro_spans.write() {
            spans.remove(filename).unwrap_or_default()
        } else {
            emit_error_log_message(
                ErrorCode::Generic,
                "Failed to acquire write lock on macro_spans",
                None,
            );
            MacroSpans::default()
        }
    }
}

/// Trait for creating and destroying Jinja type checking event listeners
pub trait JinjaTypeCheckingEventListenerFactory: Send + Sync {
    /// Creates a new type checking event listener
    fn create_listener(
        &self,
        args: &IoArgs,
        offset: dbt_common::CodeLocationWithFile,
        noqa_comments: Option<HashSet<u32>>,
        unique_id: &str,
    ) -> Rc<dyn TypecheckingEventListener>;

    /// Destroys a type checking event listener
    fn destroy_listener(&self, filename: &Path, listener: Rc<dyn TypecheckingEventListener>);

    /// Update the unique id
    /// This is for DagExtractListener (Macro depends on) only
    /// We need to type check sql before unique id is determined
    fn update_unique_id(&self, _old_unique_id: &str, _new_unique_id: &str) {}

    /// Return the sorted list of macro unique-ids that were observed for the given
    /// node unique-id during type-checking. Returns an empty Vec when the factory
    /// has no data for the given key (e.g. in LSP mode).
    fn get_macro_depends_on(&self, _unique_id: &str) -> Vec<String> {
        vec![]
    }

    /// Determines whether or not the listener factory is able to capture
    /// information on hooks.
    fn can_listen_on_hooks(&self) -> bool {
        true
    }
}

/// Default implementation of the `ListenerFactory` trait
#[derive(Default, Debug)]
pub struct DefaultJinjaTypeCheckEventListenerFactory {
    /// all macro depends on
    /// NOTE(felipecrv): this should probably be changed to an `im` data-structure
    all_depends_on: Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>,
}

impl DefaultJinjaTypeCheckEventListenerFactory {
    /// Lock the depends_on graph for reading.
    pub fn depends_on(&self) -> RwLockReadGuard<'_, BTreeMap<String, BTreeSet<String>>> {
        self.all_depends_on.read().unwrap()
    }
}

impl JinjaTypeCheckingEventListenerFactory for DefaultJinjaTypeCheckEventListenerFactory {
    /// Creates a new type checking event listener
    fn create_listener(
        &self,
        _args: &IoArgs,
        _offset: dbt_common::CodeLocationWithFile,
        _noqa_comments: Option<HashSet<u32>>,
        unique_id: &str,
    ) -> Rc<dyn TypecheckingEventListener> {
        // create a WarningPrinter instance
        // TODO: enable warning printer
        // Rc::new(WarningPrinter::new(
        //     args.clone(),
        //     filename.to_path_buf(),
        //     noqa_comments,
        // ))
        Rc::new(DagExtractListener::new(unique_id))
    }

    fn destroy_listener(&self, _filename: &Path, listener: Rc<dyn TypecheckingEventListener>) {
        if let Some(dag_extract_listener) = listener.as_any().downcast_ref::<DagExtractListener>() {
            let depends_on = dag_extract_listener.depends_on.borrow().clone();
            if let Ok(mut all_depends_on) = self.all_depends_on.write() {
                for (reference, definition) in depends_on {
                    all_depends_on
                        .entry(reference)
                        .or_default()
                        .insert(definition);
                }
            }
        }
    }

    fn update_unique_id(&self, old_unique_id: &str, new_unique_id: &str) {
        // delete the old unique id and insert the new unique id
        if let Ok(mut all_depends_on) = self.all_depends_on.write()
            && let Some(depends_on) = all_depends_on.remove(old_unique_id)
        {
            all_depends_on.insert(new_unique_id.to_string(), depends_on);
        }
    }

    fn get_macro_depends_on(&self, unique_id: &str) -> Vec<String> {
        self.all_depends_on
            .read()
            .unwrap()
            .get(unique_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }
}

struct DagExtractListener {
    unique_id: String,
    depends_on: RefCell<Vec<(String, String)>>, // (ref, def)
}

impl DagExtractListener {
    pub fn new(unique_id: &str) -> Self {
        Self {
            unique_id: unique_id.to_string(),
            depends_on: RefCell::new(vec![]),
        }
    }
}

impl TypecheckingEventListener for DagExtractListener {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn warn(&self, _message: &str) {}

    fn set_span(&self, _span: &Span) {}

    fn new_block(&self, _block_id: usize) {}

    fn flush(&self) {}

    fn on_lookup(&self, _span: &Span, _simple_name: &str, _full_name: &str, _def_spans: Vec<Span>) {
    }

    fn on_function_call(
        &self,
        _source_span: &Span,
        _def_span: &Span,
        _def_path: &Path,
        def_unique_id: &str,
    ) {
        self.depends_on
            .borrow_mut()
            .push((self.unique_id.clone(), def_unique_id.to_string()));
    }
}

#[allow(dead_code)]
struct WarningPrinter {
    args: IoArgs,
    path: PathBuf,
    noqa_comments: Option<HashSet<u32>>,
    current_block: RefCell<usize>,
    pending_warnings: RefCell<HashMap<usize, Vec<(CodeLocation, String)>>>,
    current_span: RefCell<Option<Span>>,
}

impl WarningPrinter {
    #[allow(dead_code)]
    pub fn new(args: IoArgs, path: PathBuf, noqa_comments: Option<HashSet<u32>>) -> Self {
        Self {
            args,
            path,
            noqa_comments,
            current_block: RefCell::new(0),
            pending_warnings: RefCell::new(HashMap::new()),
            current_span: RefCell::new(None),
        }
    }
}

impl TypecheckingEventListener for WarningPrinter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn on_lookup(&self, _span: &Span, _simple_name: &str, _full_name: &str, _def_spans: Vec<Span>) {
        //
    }
    fn warn(&self, message: &str) {
        // todo: consider self.offset
        if self.noqa_comments.is_some()
            && self
                .noqa_comments
                .as_ref()
                .unwrap()
                .contains(&self.current_span.borrow().unwrap().start_line)
        {
            return;
        }
        let binding = self.current_span.borrow(); // TODO: do not use the current_span
        let current_span = binding.as_ref().unwrap();
        let location = CodeLocation {
            line: current_span.start_line,
            col: current_span.start_col,
            file: self.path.clone(),
        };

        self.pending_warnings
            .borrow_mut()
            .entry(*self.current_block.borrow())
            .or_default()
            .push((location, message.to_string()));
    }

    fn new_block(&self, block_id: usize) {
        *self.current_block.borrow_mut() = block_id;
        self.pending_warnings
            .borrow_mut()
            .insert(block_id, Vec::new());
    }

    fn set_span(&self, span: &Span) {
        *self.current_span.borrow_mut() = Some(*span);
    }

    fn flush(&self) {
        let mut warnings: Vec<_> = self
            .pending_warnings
            .borrow()
            .values()
            .flat_map(|warnings| warnings.iter().cloned())
            .collect();
        warnings.sort_by(|(loc1, msg1), (loc2, msg2)| {
            (loc1.line, loc1.col, msg1).cmp(&(loc2.line, loc2.col, msg2))
        });
        warnings.iter().for_each(|(location, message)| {
            emit_warn_log_message(
                ErrorCode::JinjaTypeCheckFailed,
                format!("{}\n  --> {}", message, location),
                self.args.status_reporter.as_ref(),
            );
        });
    }
}

/// Listener that tracks which macros are invoked during rendering.
/// Used to populate `depends_on.macros` for `state:modified` support.
#[derive(Debug, Default)]
pub struct MacroDependencyListener {
    macro_deps: RefCell<Vec<String>>,
}

impl MacroDependencyListener {
    /// Creates a new, empty dependency tracker.
    pub fn new() -> Self {
        Self {
            macro_deps: RefCell::new(Vec::new()),
        }
    }

    /// Drains collected template names, deduplicates, and returns them as dbt
    /// `macro.<package>.<name>` unique IDs.
    pub fn drain_macro_unique_ids(&self) -> Vec<String> {
        let mut deps = self.macro_deps.borrow_mut();
        deps.sort();
        deps.dedup();
        deps.drain(..)
            .map(|template_name| format!("macro.{template_name}"))
            .collect()
    }
}

impl RenderingEventListener for MacroDependencyListener {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "MacroDependencyListener"
    }

    fn on_macro_start(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32) {}
    fn on_macro_stop(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32) {}
    fn on_malicious_return(&self, _location: &CodeLocation) {}
    fn on_function_start(&self) {}
    fn on_function_end(&self) {}

    fn on_macro_dependency(&self, template_name: &str) {
        self.macro_deps.borrow_mut().push(template_name.to_string());
    }
}

/// A source location (`path:line:col`) captured for a [`JinjaTraceFrame`].
#[derive(Debug, PartialEq, Eq)]
struct JinjaTraceLocation {
    file_path: String,
    line_no: u32,
    char_no: u32,
}

impl JinjaTraceLocation {
    fn new(path: &Path, span: &Span) -> Self {
        Self {
            file_path: path.to_string_lossy().into_owned(),
            line_no: span.start_line,
            char_no: span.start_col,
        }
    }

    /// Renders the location, stripping the noisy internal-package prefix
    /// (`dbt_internal_packages/<package>/macros/`) so paths read cleanly.
    fn display(&self) -> String {
        let path = if self.file_path.is_empty() {
            "<unknown>"
        } else {
            shorten_macro_path(&self.file_path)
        };
        format!("{path}:{}:{}", self.line_no, self.char_no)
    }
}

/// Strips the `dbt_internal_packages/<package>/macros/` prefix from a macro path
/// so internal macros render as e.g. `etc/statement.sql` instead of the full
/// `dbt_internal_packages/dbt-adapters/macros/etc/statement.sql`. Paths that do
/// not match (user project files) are returned unchanged.
fn shorten_macro_path(path: &str) -> &str {
    const INTERNAL: &str = "dbt_internal_packages/";
    const MACROS: &str = "/macros/";
    if let Some(start) = path.find(INTERNAL) {
        if let Some(macros) = path[start..].find(MACROS) {
            return &path[start + macros + MACROS.len()..];
        }
    }
    path
}

/// A single macro execution captured by [`JinjaTraceListener`], annotated with
/// its nesting depth, the call site (where it was invoked from), and the
/// location where the macro is defined.
#[derive(Debug)]
struct JinjaTraceFrame {
    depth: usize,
    name: String,
    call_site: Option<JinjaTraceLocation>,
    definition: JinjaTraceLocation,
}

/// Listener that captures a nested trace of macro executions during rendering.
/// Used to produce a diagnostic dump when a materialization fails.
#[derive(Debug, Default)]
pub struct JinjaTraceListener {
    tree: RefCell<Vec<JinjaTraceFrame>>,
    depth: Cell<usize>,
}

impl JinjaTraceListener {
    /// Creates a new, empty trace listener.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if no macro executions have been recorded.
    pub fn is_empty(&self) -> bool {
        self.tree.borrow().is_empty()
    }

    /// Returns the collected macro call tree as human-readable text.
    pub fn format_trace(&self) -> String {
        let tree = self.tree.borrow();
        let mut out = String::from("Jinja macro call stack (most recent call last):\n\n");

        let frames: Vec<_> = tree.iter().collect();
        let mut i = 0;

        while i < frames.len() {
            let frame = &frames[i];

            // Collapse only genuine repeats: immediately adjacent frames that
            // are the same macro (same definition) invoked from the same call
            // site. Same name alone is not enough — the same macro called from
            // two different places is two distinct calls, not a repeat.
            let repeat_count = frames[i..]
                .iter()
                .take_while(|f| {
                    f.depth == frame.depth
                        && f.definition == frame.definition
                        && f.call_site == frame.call_site
                })
                .count();

            let indent = "  ".repeat(frame.depth);
            let definition = frame.definition.display();

            if repeat_count > 1 {
                let _ = writeln!(out, "{indent}{} ×{repeat_count} ({definition})", frame.name);
            } else {
                let _ = writeln!(out, "{indent}{} ({definition})", frame.name);
            }

            if let Some(call_site) = &frame.call_site {
                let _ = writeln!(out, "{indent}  @ {}", call_site.display());
            }

            let _ = writeln!(out);

            i += repeat_count;
        }
        out
    }
}

impl RenderingEventListener for JinjaTraceListener {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "JinjaTraceListener"
    }

    fn tracks_macro_call_sites(&self) -> bool {
        true
    }

    // The call tree is built entirely from macro execution events below; the
    // remaining rendering signals are not needed here.
    fn on_macro_start(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32) {}
    fn on_macro_stop(&self, _file_path: Option<&Path>, _line: &u32, _col: &u32, _offset: &u32) {}
    fn on_malicious_return(&self, _location: &CodeLocation) {}
    fn on_function_start(&self) {}
    fn on_function_end(&self) {}

    fn on_macro_execute_start(
        &self,
        name: &str,
        call_site: Option<(&Path, &Span)>,
        def_path: &Path,
        def_span: &Span,
    ) {
        self.tree.borrow_mut().push(JinjaTraceFrame {
            depth: self.depth.get(),
            name: name.to_string(),
            call_site: call_site.map(|(path, span)| JinjaTraceLocation::new(path, span)),
            definition: JinjaTraceLocation::new(def_path, def_span),
        });
        self.depth.set(self.depth.get() + 1);
    }

    fn on_macro_execute_end(&self, _name: &str) {
        self.depth.set(self.depth.get().saturating_sub(1));
    }
}

/// default implementation of RenderingEventListener
#[derive(Debug)]
pub struct DefaultRenderingEventListener {
    /// Suppress malicious return warning
    pub quiet: bool,

    /// io args
    pub args: IoArgs,

    /// macro spans
    pub macro_spans: RefCell<MacroSpans>,

    /// inner Vec<MacroStart> means during one function start/stop
    macro_start_stack: RefCell<Vec<Vec<MacroStart>>>,

    /// Output tracker location for tracking expanded positions
    output_tracker_location: Rc<OutputTrackerLocation>,
}

impl Default for DefaultRenderingEventListener {
    fn default() -> Self {
        Self {
            quiet: false,
            args: IoArgs::default(),
            macro_spans: RefCell::new(MacroSpans::default()),
            macro_start_stack: RefCell::new(vec![vec![]]),
            output_tracker_location: Rc::new(OutputTrackerLocation::default()),
        }
    }
}

impl DefaultRenderingEventListener {
    /// Creates a new rendering event listener
    pub fn new(quiet: bool) -> Self {
        Self {
            quiet,
            args: IoArgs::default(),
            macro_spans: RefCell::new(MacroSpans::default()),
            macro_start_stack: RefCell::new(vec![vec![]]),
            output_tracker_location: Rc::new(OutputTrackerLocation::default()),
        }
    }

    /// Creates a new rendering event listener with a shared output tracker location.
    /// Use this when the output position needs to be observable by another listener
    /// (e.g. `MangledRefWarningPrinter`) at the same time.
    pub fn with_tracker(quiet: bool, output_tracker_location: Rc<OutputTrackerLocation>) -> Self {
        Self {
            quiet,
            args: IoArgs::default(),
            macro_spans: RefCell::new(MacroSpans::default()),
            macro_start_stack: RefCell::new(vec![vec![]]),
            output_tracker_location,
        }
    }
}

#[derive(Clone, Copy)]
struct SpanPosition {
    line: u32,
    col: u32,
    offset: u32,
}

fn push_raw_source_spans(
    raw: &str,
    source_span: Span,
    expanded_start: SpanPosition,
    spans: &mut Vec<(Span, Span)>,
) {
    let mut source = SpanPosition {
        line: source_span.start_line,
        col: source_span.start_col,
        offset: source_span.start_offset,
    };
    let mut expanded = expanded_start;

    for part in raw.split_inclusive('\n') {
        if part.is_empty() {
            continue;
        }
        let source_start = source;
        let expanded_start = expanded;
        advance_span_position(&mut source, part);
        advance_span_position(&mut expanded, part);
        spans.push((
            span_from_positions(source_start, source),
            span_from_positions(expanded_start, expanded),
        ));
    }
}

fn advance_span_position(position: &mut SpanPosition, text: &str) {
    position.offset += text.len() as u32;
    if let Some(last_newline) = text.rfind('\n') {
        position.line += text.bytes().filter(|byte| *byte == b'\n').count() as u32;
        position.col = (text.len() - last_newline) as u32;
    } else {
        position.col += text.len() as u32;
    }
}

fn span_from_positions(start: SpanPosition, end: SpanPosition) -> Span {
    Span {
        start_line: start.line,
        start_col: start.col,
        start_offset: start.offset,
        end_line: end.line,
        end_col: end.col,
        end_offset: end.offset,
    }
}

impl RenderingEventListener for DefaultRenderingEventListener {
    fn on_function_start(&self) {
        self.macro_start_stack.borrow_mut().push(vec![]);
    }

    fn on_function_end(&self) {
        // assert the the top level of the stack is empty
        let mut macro_start_stack = self.macro_start_stack.borrow_mut();
        if !macro_start_stack.last().unwrap().is_empty() {
            unreachable!("MacroStart stack is not empty");
        }
        macro_start_stack.pop();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DefaultRenderingEventListener"
    }

    fn create_output_tracker<'a>(
        &self,
        w: &'a mut (dyn std::fmt::Write + 'a),
    ) -> Option<OutputTracker<'a>> {
        Some(OutputTracker::with_location(
            w,
            self.output_tracker_location.clone(),
        ))
    }

    fn on_macro_start(&self, _file_path: Option<&Path>, line: &u32, col: &u32, offset: &u32) {
        // Capture current expanded location from our own tracker
        let expanded_line = self.output_tracker_location.line();
        let expanded_col = self.output_tracker_location.col();
        let expanded_offset = self.output_tracker_location.index();

        self.macro_start_stack
            .borrow_mut()
            .last_mut()
            .unwrap()
            .push(MacroStart {
                line: *line,
                col: *col,
                offset: *offset,
                expanded_line,
                expanded_col,
                expanded_offset,
            });
    }

    fn on_macro_stop(&self, _file_path: Option<&Path>, line: &u32, col: &u32, offset: &u32) {
        // Get current expanded location from our own tracker
        let expanded_line = self.output_tracker_location.line();
        let expanded_col = self.output_tracker_location.col();
        let expanded_offset = self.output_tracker_location.index();

        let mut macro_start_stack = self.macro_start_stack.borrow_mut();
        let macro_start_stack_length = macro_start_stack.len();
        let macro_start_stack_last = macro_start_stack.last_mut().unwrap();
        let macro_start_stack_last_length = macro_start_stack_last.len();
        if macro_start_stack_length == 1 && macro_start_stack_last_length == 1 {
            let macro_start = macro_start_stack_last.pop().unwrap();
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
                    end_line: expanded_line,
                    end_col: expanded_col,
                    end_offset: expanded_offset,
                },
            );
        } else {
            macro_start_stack_last.pop();
        }
    }

    fn on_jinja_layout_event(&self, _kind: JinjaLayoutEventKind, _source_span: &Span) {}

    fn on_raw_emit(&self, raw: &str, source_span: &Span) {
        push_raw_source_spans(
            raw,
            *source_span,
            SpanPosition {
                line: self.output_tracker_location.line(),
                col: self.output_tracker_location.col(),
                offset: self.output_tracker_location.index(),
            },
            &mut self.macro_spans.borrow_mut().raw_source_spans,
        );
    }

    fn on_malicious_return(&self, location: &CodeLocation) {
        // Whenever we encounter a malicious return, it means a false MacroStart is issued
        // We should remove the false MacroStart from the stack
        let mut macro_start_stack = self.macro_start_stack.borrow_mut();
        let macro_start_stack_last = macro_start_stack.last_mut().unwrap();
        macro_start_stack_last.clear();
        if !self.quiet {
            // We should also warn it
            emit_warn_log_message(
                ErrorCode::JinjaTopLevelReturn,
                format!(
                    "return is not at the top level of the block.\nIts value is final and cannot be modified by surrounding expressions.\nExample: return(0) + 1. The + 1 is ignored and the macro returns 0.\n  --> {}",
                    location
                ),
                self.args.status_reporter.as_ref(),
            );
        }
    }
}
