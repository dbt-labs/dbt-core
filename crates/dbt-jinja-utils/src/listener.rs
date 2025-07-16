use std::{
    collections::HashMap,
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
    fn create_listeners(&self, filename: &Path) -> Vec<Rc<dyn RenderingEventListener>>;

    /// Destroys a rendering event listener
    fn destroy_listener(&self, _filename: &Path, _listener: Rc<dyn RenderingEventListener>);

    /// get macro spans
    fn drain_macro_spans(&self, filename: &Path) -> MacroSpans;
}

/// Default implementation of the `ListenerFactory` trait
#[derive(Default, Debug)]
pub struct DefaultListenerFactory {
    /// macro spans
    pub macro_spans: Arc<RwLock<HashMap<PathBuf, MacroSpans>>>,
}

impl ListenerFactory for DefaultListenerFactory {
    /// Creates a new rendering event listener
    fn create_listeners(&self, _filename: &Path) -> Vec<Rc<dyn RenderingEventListener>> {
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
