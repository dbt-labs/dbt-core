use dbt_telemetry::{SpanEndInfo, SpanStartInfo, TelemetryRecordRef};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use crate::tracing::event_info::with_current_thread_log_record;
use tracing::{Subscriber, span};

pub struct FilteredOutput<L, S, F> {
    layer: L,
    predicate: F,
    _subscriber: std::marker::PhantomData<S>,
}

pub trait WithOutputFilter<S, F>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    F: for<'ctx> Fn(TelemetryRecordRef, &Context<'ctx, S>) -> bool + Send + Sync + 'static,
    Self: Sized,
{
    fn with_output_filter(self, predicate: F) -> FilteredOutput<Self, S, F>;
}

impl<L, S, F> WithOutputFilter<S, F> for L
where
    L: Layer<S>,
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    F: for<'ctx> Fn(TelemetryRecordRef, &Context<'ctx, S>) -> bool + Send + Sync + 'static,
{
    fn with_output_filter(self, predicate: F) -> FilteredOutput<Self, S, F> {
        FilteredOutput::new(self, predicate)
    }
}

impl<L, S, F> FilteredOutput<L, S, F>
where
    L: Layer<S>,
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    F: for<'ctx> Fn(TelemetryRecordRef, &Context<'ctx, S>) -> bool + Send + Sync + 'static,
{
    pub fn new(layer: L, predicate: F) -> Self {
        Self {
            layer,
            predicate,
            _subscriber: std::marker::PhantomData,
        }
    }
}

impl<L, S, F> Layer<S> for FilteredOutput<L, S, F>
where
    L: Layer<S>,
    S: Subscriber + for<'lookup> LookupSpan<'lookup> + 'static,
    F: for<'ctx> Fn(TelemetryRecordRef, &Context<'ctx, S>) -> bool + Send + Sync + 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("Span must exist for id in the current context");

        // Access extension in a separate block to avoid locking it when calling wrapped layer.
        let enabled = if let Some(span_start) = span.extensions().get::<SpanStartInfo>() {
            (self.predicate)(TelemetryRecordRef::SpanStart(span_start), &ctx)
        } else {
            unreachable!("Unexpectedly missing span start data!");
        };

        if enabled {
            self.layer.on_new_span(attrs, id, ctx.clone());
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");

        // Access extension in a separate block to avoid locking it when calling wrapped layer.
        let enabled = if let Some(span_end) = span.extensions().get::<SpanEndInfo>() {
            (self.predicate)(TelemetryRecordRef::SpanEnd(span_end), &ctx)
        } else {
            unreachable!("Unexpectedly missing span start data!");
        };

        if enabled {
            self.layer.on_close(id, ctx.clone());
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut enabled = false;
        with_current_thread_log_record(|log_record| {
            enabled = (self.predicate)(TelemetryRecordRef::LogRecord(log_record), &ctx);
        });

        if enabled {
            self.layer.on_event(event, ctx);
        }
    }
}
