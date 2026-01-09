use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, RwLock},
};

use minijinja::{
    CodeLocation, MacroSpans, TypecheckingEventListener,
    listener::{MacroStart, RenderingEventListener},
    machinery::Span,
};

use dbt_common::{
    ErrorCode,
    io_args::IoArgs,
    tracing::emit::{emit_error_log_message, emit_warn_log_message},
};

/// Trait for creating and destroying rendering event listeners
pub trait RenderingEventListenerFactory: Send + Sync {
    /// Creates a new rendering event listener
    fn create_listeners(
        &self,
        filename: &Path,
        offset: &dbt_frontend_common::error::CodeLocation,
    ) -> Vec<Rc<dyn RenderingEventListener>>;

    /// Destroys a rendering event listener
    fn destroy_listener(&self, _filename: &Path, _listener: Rc<dyn RenderingEventListener>);

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
}

impl DefaultRenderingEventListenerFactory {
    /// Creates a new rendering event listener factory
    pub fn new(quiet: bool) -> Self {
        Self {
            quiet,
            macro_spans: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl RenderingEventListenerFactory for DefaultRenderingEventListenerFactory {
    /// Creates a new rendering event listener
    fn create_listeners(
        &self,
        _filename: &Path,
        _offset: &dbt_frontend_common::error::CodeLocation,
    ) -> Vec<Rc<dyn RenderingEventListener>> {
        vec![Rc::new(DefaultRenderingEventListener::new(self.quiet))]
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
    /// Creates a new rendering event listener
    fn create_listener(
        &self,
        args: &IoArgs,
        offset: dbt_common::CodeLocationWithFile,
        noqa_comments: Option<HashSet<u32>>,
        unique_id: &str,
    ) -> Rc<dyn TypecheckingEventListener>;

    /// Destroys a rendering event listener
    fn destroy_listener(&self, filename: &Path, listener: Rc<dyn TypecheckingEventListener>);

    /// Update the unique id
    /// This is for DagExtractListener (Macro depends on) only
    /// We need to type check sql before unique id is determined
    fn update_unique_id(&self, _old_unique_id: &str, _new_unique_id: &str) {}
}

/// Default implementation of the `ListenerFactory` trait
#[derive(Default, Debug)]
pub struct DefaultJinjaTypeCheckEventListenerFactory {
    /// all macro depends on
    pub all_depends_on: Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>,
}

impl JinjaTypeCheckingEventListenerFactory for DefaultJinjaTypeCheckEventListenerFactory {
    /// Creates a new rendering event listener
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
            .iter()
            .flat_map(|(_, warnings)| warnings.iter().cloned())
            .collect();
        warnings.sort_by(|(loc1, msg1), (loc2, msg2)| {
            (loc1.line, loc1.col, msg1).cmp(&(loc2.line, loc2.col, msg2))
        });
        warnings.iter().for_each(|(location, message)| {
            emit_warn_log_message(
                ErrorCode::Generic,
                format!("{}\n  --> {}", message, location),
                self.args.status_reporter.as_ref(),
            );
        });
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
}

impl Default for DefaultRenderingEventListener {
    fn default() -> Self {
        Self {
            quiet: false,
            args: IoArgs::default(),
            macro_spans: RefCell::new(MacroSpans::default()),
            macro_start_stack: RefCell::new(vec![vec![]]),
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
        }
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
        self.macro_start_stack
            .borrow_mut()
            .last_mut()
            .unwrap()
            .push(MacroStart {
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
                    end_line: *expanded_line,
                    end_col: *expanded_col,
                    end_offset: *expanded_offset,
                },
            );
        } else {
            macro_start_stack_last.pop();
        }
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
                ErrorCode::Generic,
                format!(
                    "return is not at the top level of the block.\nIts value is final and cannot be modified by surrounding expressions.\nExample: return(0) + 1. The + 1 is ignored and the macro returns 0.\n  --> {}",
                    location
                ),
                self.args.status_reporter.as_ref(),
            );
        }
    }
}
