/// Trait for typechecking event listeners.
pub trait TypecheckingEventListener {
    /// Called when a warning is issued during typechecking.
    fn warn(&self, location: &super::CodeLocation, message: &str);
}
