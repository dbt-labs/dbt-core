use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, RwLock},
};

use minijinja::{
    CodeLocation, MacroSpans, TypecheckingEventListener,
    listener::{DefaultRenderingEventListener, RenderingEventListener},
};

use dbt_common::{ErrorCode, io_args::IoArgs, tracing::emit::emit_warn_log_message};

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
    /// macro spans
    pub macro_spans: Arc<RwLock<HashMap<PathBuf, MacroSpans>>>,
}

impl RenderingEventListenerFactory for DefaultRenderingEventListenerFactory {
    /// Creates a new rendering event listener
    fn create_listeners(
        &self,
        _filename: &Path,
        _offset: &dbt_frontend_common::error::CodeLocation,
    ) -> Vec<Rc<dyn RenderingEventListener>> {
        vec![Rc::new(DefaultRenderingEventListener::default())]
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
                log::error!("Failed to acquire write lock on macro_spans");
            }
        }
    }

    fn drain_macro_spans(&self, filename: &Path) -> MacroSpans {
        if let Ok(mut spans) = self.macro_spans.write() {
            spans.remove(filename).unwrap_or_default()
        } else {
            log::error!("Failed to acquire write lock on macro_spans");
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
        offset: dbt_common::CodeLocation,
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
        _offset: dbt_common::CodeLocation,
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

    fn set_span(&self, _span: &minijinja::machinery::Span) {}

    fn new_block(&self, _block_id: usize) {}

    fn flush(&self) {}

    fn on_lookup(
        &self,
        _span: &minijinja::machinery::Span,
        _simple_name: &str,
        _full_name: &str,
        _def_spans: Vec<minijinja::machinery::Span>,
    ) {
    }

    fn on_function_call(
        &self,
        _source_span: &minijinja::machinery::Span,
        _def_span: &minijinja::machinery::Span,
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
    current_span: RefCell<Option<minijinja::machinery::Span>>,
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

    fn on_lookup(
        &self,
        _span: &minijinja::machinery::Span,
        _simple_name: &str,
        _full_name: &str,
        _def_spans: Vec<minijinja::machinery::Span>,
    ) {
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

    fn set_span(&self, span: &minijinja::machinery::Span) {
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
                &self.args,
            );
        });
    }
}
