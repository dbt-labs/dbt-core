use std::collections::BTreeMap;

use super::shared::Recordable;
use dbt_telemetry::{DebugValue, SpanStatus, TelemetryAttributes};

use tracing::Span;
use tracing_subscriber::{
    Registry,
    registry::{Extensions, ExtensionsMut, LookupSpan, SpanRef},
};

/// Object-safe abstraction over a span sufficient for our needs.
/// This erases the concrete registry type `R` and avoids generic blowup
pub(super) trait SpanAccess {
    fn extensions(&self) -> Extensions<'_>;
    fn extensions_mut(&self) -> ExtensionsMut<'_>;
}

impl<'a, R> SpanAccess for SpanRef<'a, R>
where
    R: LookupSpan<'a>,
{
    fn extensions(&self) -> Extensions<'_> {
        SpanRef::extensions(self)
    }

    fn extensions_mut(&self) -> ExtensionsMut<'_> {
        SpanRef::extensions_mut(self)
    }
}

/// Helper that extracts arbitrary captured fields into a map.
pub(super) fn get_span_debug_extra_attrs(values: Recordable<'_>) -> BTreeMap<String, DebugValue> {
    struct SpanEventAttributesVisitor(BTreeMap<String, DebugValue>);

    impl tracing::field::Visit for SpanEventAttributesVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}").into());
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.into());
        }

        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.0.insert(field.name().to_string(), value.into());
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.insert(field.name().to_string(), value.into());
        }

        fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
            self.0.insert(field.name().to_string(), value.into());
        }

        fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
            self.0.insert(field.name().to_string(), value.into());
        }

        fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
            self.0.insert(field.name().to_string(), value.into());
        }
    }

    let mut visitor = SpanEventAttributesVisitor(BTreeMap::new());
    values.record(&mut visitor);

    visitor.0
}

/// Executes a closure with the current span reference allowing
/// direct access to the span's extensions.
///
/// # Returns
///
/// Should always return `Some(R)`. None means thread local subscriber missing,
/// which should not happen in our case.
pub(super) fn with_current_span<F, R>(mut f: F) -> Option<R>
where
    F: FnMut(SpanRef<Registry>) -> R,
{
    tracing::dispatcher::get_default(|dispatch| {
        // If the dispatcher is not a `Registry`, means tracing
        // wasn't initialized and so this is a no-op.
        let registry = dispatch.downcast_ref::<Registry>()?;

        let span_ref = registry
            // No current span? Silently ignore.
            .span(dispatch.current_span().id()?)
            .expect("Must be an existing span reference");

        Some(f(span_ref))
    })
}

/// Executes a closure with the span reference from the given span allowing
/// direct access to the span's extensions.
///
/// # Returns
///
/// Should always return `Some(R)`. None means thread local subscriber missing,
/// which should not happen in our case.
///
/// # Panics
///
/// This function will panic if it is called with a span that does not exist
/// in the current context.
pub(super) fn with_span<F, R>(span: &Span, mut f: F) -> Option<R>
where
    F: FnMut(SpanRef<Registry>) -> R,
{
    tracing::dispatcher::get_default(|dispatch| {
        // If the dispatcher is not a `Registry`, means tracing
        // wasn't initialized and so this is a no-op.
        let registry = dispatch.downcast_ref::<Registry>()?;

        let span_ref = registry
            // Disabled span? Silently ignore.
            .span(&span.id()?)
            .expect("Must be an existing span reference");

        Some(f(span_ref))
    })
}

pub fn get_root_span_ref(cur_span: SpanRef<Registry>) -> SpanRef<Registry> {
    cur_span.scope().from_root().next().unwrap_or(cur_span)
}

pub(super) fn with_root_span<F, R>(mut f: F) -> Option<R>
where
    F: FnMut(SpanRef<Registry>) -> R,
{
    with_current_span(|cur_span| f(get_root_span_ref(cur_span)))
}

fn record_span_status_on_ref(span_ext_mut: &mut ExtensionsMut<'_>, error_message: Option<&str>) {
    span_ext_mut.replace(
        error_message
            .map(SpanStatus::failed)
            .unwrap_or_else(SpanStatus::succeeded),
    );
}

/// Records the status of a span. If `error_message` is `None`, the
/// status code will be set to `Ok`, otherwise it will be set to `Error`.
pub fn record_span_status(span: &Span, error_message: Option<&str>) {
    with_span(span, |span_ref| {
        record_span_status_on_ref(&mut span_ref.extensions_mut(), error_message)
    });
}

/// Records the status and attributes of the given span.
///
/// If `error_message` is `None`, the status code will be set to `Ok`,
/// otherwise it will be set to `Error`.
///
/// The `attrs_updater` closure receives a mutable reference to the current
/// attributes and should modify them in place.
pub fn record_span_status_with_attrs<F>(
    span: &Span,
    mut attrs_updater: F,
    error_message: Option<&str>,
) where
    F: FnMut(&mut TelemetryAttributes),
{
    with_span(span, |span_ref| {
        let mut span_ext_mut = span_ref.extensions_mut();

        // Record the status of the span
        record_span_status_on_ref(&mut span_ext_mut, error_message);

        // Get the current attributes, and update or replace them
        let attrs = span_ext_mut
            .get_mut::<TelemetryAttributes>()
            .expect("Telemetry hasn't been properly initialized. Missing span event attributes");
        attrs_updater(attrs);
    });
}

/// Records the status and attributes of the given span.
///
/// Uses event `get_span_status` method to determine the status. If the event
/// doesn't support inferring status, use `record_span_status_with_attrs` instead.
///
/// The `attrs_updater` closure receives a mutable reference to the current
/// attributes and should modify them in place.
pub fn record_span_status_from_attrs<F>(span: &Span, mut attrs_updater: F)
where
    F: FnMut(&mut TelemetryAttributes),
{
    with_span(span, |span_ref| {
        let mut span_ext_mut = span_ref.extensions_mut();

        // Get the current attributes, and update or replace them
        let attrs = span_ext_mut
            .get_mut::<TelemetryAttributes>()
            .expect("Telemetry hasn't been properly initialized. Missing span event attributes");
        attrs_updater(attrs);

        // Record the status of the span from the attrs themselves
        if let Some(status) = attrs.get_span_status() {
            span_ext_mut.replace(status);
        }
    });
}

/// Records the status and attributes of the current span.
///
/// Uses event `get_span_status` method to determine the status. If the event
/// doesn't support inferring status, use `record_span_status_with_attrs` instead.
///
/// The `attrs_updater` closure receives a mutable reference to the current
/// attributes and should modify them in place.
pub fn record_current_span_status_from_attrs<F>(mut attrs_updater: F)
where
    F: FnMut(&mut TelemetryAttributes),
{
    with_current_span(|span_ref| {
        let mut span_ext_mut = span_ref.extensions_mut();

        // Get the current attributes, and update or replace them
        let attrs = span_ext_mut
            .get_mut::<TelemetryAttributes>()
            .expect("Telemetry hasn't been properly initialized. Missing span event attributes");
        attrs_updater(attrs);

        // Record the status of the span from the attrs themselves
        if let Some(status) = attrs.get_span_status() {
            span_ext_mut.replace(status);
        }
    });
}
