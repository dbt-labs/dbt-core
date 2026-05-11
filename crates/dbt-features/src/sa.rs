use dbt_common::cancellation::CancellationTokenSource;
use dbt_common::fail_fast::FailFast;

use crate::adapter::AdapterFeature;
use crate::antlr_parser::AntlrParserFeature;
use crate::feature_stack::*;
use crate::tracing::TracingFeature;

struct NoOpExtensionHooks;
impl CliExtensionHooks for NoOpExtensionHooks {}

pub struct SourceAvailableFeatureStackBuilder {
    send_anonymous_usage_stats: bool,
    tracing: TracingFeature,
    adapter: AdapterFeature,
    antlr_parser: AntlrParserFeature,
}

impl SourceAvailableFeatureStackBuilder {
    pub fn new(tracing: TracingFeature, adapter: AdapterFeature) -> Self {
        Self {
            send_anonymous_usage_stats: false,
            tracing,
            adapter,
            antlr_parser: Default::default(),
        }
    }

    pub fn send_anonymous_usage_stats(mut self, enabled: bool) -> Self {
        self.send_anonymous_usage_stats = enabled;
        self
    }

    pub fn antlr_parser(mut self, feature: AntlrParserFeature) -> Self {
        self.antlr_parser = feature;
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
            adapter: self.adapter,
            antlr_parser: self.antlr_parser,
            cancellation_token_source: CancellationTokenSource::new(),
            fail_fast: FailFast::new(),
        };
        Box::new(stack)
    }
}
