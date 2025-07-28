use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, RwLock},
};

use minijinja::{
    MacroSpans,
    listener::{DefaultRenderingEventListener, RenderingEventListener},
};

/// Trait for creating and destroying rendering event listeners
pub trait ListenerFactory: Send + Sync {
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
pub struct DefaultListenerFactory {
    /// macro spans
    pub macro_spans: Arc<RwLock<HashMap<PathBuf, MacroSpans>>>,
    /// macro calls
    pub macro_calls: Arc<RwLock<HashMap<PathBuf, HashSet<String>>>>,
}

impl ListenerFactory for DefaultListenerFactory {
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
