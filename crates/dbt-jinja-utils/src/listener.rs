use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, RwLock},
};

use minijinja::{
    CodeLocation, MacroSpans, TypecheckingEventListener,
    listener::{DefaultRenderingEventListener, RenderingEventListener},
};

use dbt_common::{ErrorCode, fs_err, io_args::IoArgs, show_warning};

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

    /// get macro calls
    fn drain_macro_calls(&self, filename: &Path) -> HashSet<String>;
}

/// Default implementation of the `ListenerFactory` trait
#[derive(Default, Debug)]
pub struct DefaultRenderingEventListenerFactory {
    /// macro spans
    pub macro_spans: Arc<RwLock<HashMap<PathBuf, MacroSpans>>>,
    /// macro calls
    pub macro_calls: Arc<RwLock<HashMap<PathBuf, HashSet<String>>>>,
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

            let new_macro_calls = default_listener.macro_calls.borrow().clone();
            if let Ok(mut macro_calls) = self.macro_calls.write() {
                macro_calls.insert(filename.to_path_buf(), new_macro_calls);
            } else {
                log::error!("Failed to acquire write lock on macro_calls");
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

    fn drain_macro_calls(&self, filename: &Path) -> HashSet<String> {
        if let Ok(mut calls) = self.macro_calls.write() {
            calls.remove(filename).unwrap_or_default()
        } else {
            log::error!("Failed to acquire write lock on macro_calls");
            HashSet::new()
        }
    }
}

/// Trait for creating and destroying Jinja type checking event listeners
pub trait JinjaTypeCheckingEventListenerFactory: Send + Sync {
    /// Creates a new rendering event listener
    fn create_listener(
        &self,
        args: &IoArgs,
        filename: &Path,
        noqa_comments: Option<HashSet<u32>>,
    ) -> Rc<dyn TypecheckingEventListener>;

    /// Destroys a rendering event listener
    fn destroy_listener(&self, filename: &Path, listener: Rc<dyn TypecheckingEventListener>);
}

/// Default implementation of the `ListenerFactory` trait
#[derive(Default, Debug)]
pub struct DefaultJinjaTypeCheckEventListenerFactory {}

impl JinjaTypeCheckingEventListenerFactory for DefaultJinjaTypeCheckEventListenerFactory {
    /// Creates a new rendering event listener
    fn create_listener(
        &self,
        args: &IoArgs,
        filename: &Path,
        noqa_comments: Option<HashSet<u32>>,
    ) -> Rc<dyn TypecheckingEventListener> {
        // create a WarningPrinter instance
        Rc::new(WarningPrinter::new(
            args.clone(),
            filename.to_path_buf(),
            noqa_comments,
        ))
    }

    fn destroy_listener(&self, _filename: &Path, _listener: Rc<dyn TypecheckingEventListener>) {
        //
    }
}

struct WarningPrinter {
    args: IoArgs,
    path: PathBuf,
    noqa_comments: Option<HashSet<u32>>,
    current_block: RefCell<usize>,
    pending_warnings: RefCell<HashMap<usize, Vec<(CodeLocation, String)>>>,
    current_span: RefCell<Option<minijinja::machinery::Span>>,
}

impl WarningPrinter {
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
        if self.noqa_comments.is_some()
            && self
                .noqa_comments
                .as_ref()
                .unwrap()
                .contains(&self.current_span.borrow().unwrap().start_line)
        {
            return;
        }
        let binding = self.current_span.borrow();
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
            show_warning!(
                &self.args,
                fs_err!(ErrorCode::Generic, "{}\n  --> {}", message, location)
            );
        });
    }
}
