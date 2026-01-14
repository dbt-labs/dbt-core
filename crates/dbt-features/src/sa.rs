use crate::feature_stack::*;
use crate::formatter::FormatterCommandHandler;
use crate::linter::LinterCommandHandler;

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
        let stack = FeatureStack {
            instrumentation,
            formatter,
            linter,
        };
        Box::new(stack)
    }
}
