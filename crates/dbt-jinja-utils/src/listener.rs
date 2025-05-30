use std::{path::Path, rc::Rc};

use minijinja::listener::{DefaultRenderingEventListener, RenderingEventListener};

/// Trait for creating and destroying rendering event listeners
pub trait ListenerFactory: Send + Sync {
    /// Creates a new rendering event listener
    fn create_listener(&self, filename: &Path) -> Rc<dyn RenderingEventListener>;

    /// Destroys a rendering event listener
    fn destroy_listener(&self, _filename: &Path, _listener: Rc<dyn RenderingEventListener>) {}
}

/// Default implementation of the `ListenerFactory` trait
#[derive(Default, Debug)]
pub struct DefaultListenerFactory;

impl ListenerFactory for DefaultListenerFactory {
    /// Creates a new rendering event listener
    fn create_listener(&self, _filename: &Path) -> Rc<dyn RenderingEventListener> {
        Rc::new(DefaultRenderingEventListener)
    }
}
