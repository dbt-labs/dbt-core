/// Trait for typechecking event listeners.
pub trait TypecheckingEventListener {
    /// Called when a warning is issued during typechecking.
    fn warn(&self, span: &crate::machinery::Span, message: &str);
}
