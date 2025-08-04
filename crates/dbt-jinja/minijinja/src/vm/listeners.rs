/// Trait for typechecking event listeners.
pub trait TypecheckingEventListener {
    /// Called when a warning is issued during typechecking.
    fn warn(&self, message: &str);

    /// Called when a span is set during typechecking.
    fn set_span(&self, span: &crate::machinery::Span);

    /// Called when a new block is encountered during typechecking.
    fn new_block(&self, block_id: usize);

    /// Called when typechecking is complete.
    fn flush(&self);
}

#[derive(Default, Clone)]
pub struct DefaultTypecheckingEventListener {}

impl TypecheckingEventListener for DefaultTypecheckingEventListener {
    fn warn(&self, _message: &str) {
        //
    }

    /// Called when a span is set during typechecking.
    fn set_span(&self, _span: &crate::machinery::Span) {
        //
    }

    /// Called when a new block is encountered during typechecking.
    fn new_block(&self, _block_id: usize) {
        //
    }

    /// Called when typechecking is complete.
    fn flush(&self) {
        //
    }
}
