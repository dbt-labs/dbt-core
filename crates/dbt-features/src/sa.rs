use dbt_common::cancellation::CancellationTokenSource;
use dbt_common::fail_fast::FailFast;

use crate::feature_stack::*;
use crate::formatter::FormatterCommandHandler;
use crate::linter::LinterCommandHandler;

struct NoOpExtensionHooks;
impl CliExtensionHooks for NoOpExtensionHooks {}

#[derive(Default)]
pub struct SourceAvailableFeatureStackBuilder {
    send_anonymous_usage_stats: bool,
}

impl SourceAvailableFeatureStackBuilder {
    pub fn send_anonymous_usage_stats(mut self, enabled: bool) -> Self {
        self.send_anonymous_usage_stats = enabled;
        self
    }

    pub fn build(self) -> Box<FeatureStack> {
        let instrumentation = InstrumentationFeature {
            event_emitter: vortex_events::fusion_sa_event_emitter(self.send_anonymous_usage_stats),
        };
        let formatter = FormatterFeature {
            command_handler: Box::new(FormatterCommandHandler {}),
        };
        let linter = LinterFeature {
            command_handler: Box::new(LinterCommandHandler {}),
        };
        let cli_extension = CliExtensionFeature {
            hooks: Box::new(NoOpExtensionHooks),
        };
        let stack = FeatureStack {
            instrumentation,
            formatter,
            linter,
            cli_extension,
            cancellation_token_source: CancellationTokenSource::new(),
            fail_fast: FailFast::new(),
        };
        Box::new(stack)
    }
}
