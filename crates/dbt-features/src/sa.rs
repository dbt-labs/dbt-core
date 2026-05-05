use dbt_common::cancellation::CancellationTokenSource;
use dbt_common::fail_fast::FailFast;

use crate::feature_stack::*;
use crate::tracing::TracingFeature;

struct NoOpExtensionHooks;
impl CliExtensionHooks for NoOpExtensionHooks {}

pub struct SourceAvailableFeatureStackBuilder {
    send_anonymous_usage_stats: bool,
    tracing: TracingFeature,
}

impl SourceAvailableFeatureStackBuilder {
    pub fn new(tracing: TracingFeature) -> Self {
        Self {
            send_anonymous_usage_stats: false,
            tracing,
        }
    }

    pub fn send_anonymous_usage_stats(mut self, enabled: bool) -> Self {
        self.send_anonymous_usage_stats = enabled;
        self
    }

    pub fn build(self) -> Box<FeatureStack> {
        let instrumentation = InstrumentationFeature {
            event_emitter: vortex_events::fusion_sa_event_emitter(self.send_anonymous_usage_stats),
        };
        let cli_extension = CliExtensionFeature {
            hooks: Box::new(NoOpExtensionHooks),
        };
        let stack = FeatureStack {
            instrumentation,
            cli_extension,
            tracing: self.tracing,
            cancellation_token_source: CancellationTokenSource::new(),
            fail_fast: FailFast::new(),
        };
        Box::new(stack)
    }
}
