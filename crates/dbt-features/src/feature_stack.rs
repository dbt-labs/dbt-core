use dbt_common::DiscreteEventEmitter;
use dbt_common::FsResult;
use dbt_common::io_args::EvalArgs;
use dbt_schemas::state::ResolverState;
use std::fmt;

pub trait CommandHandler: Sync + Send {
    fn process_eval_args(
        &self,
        eval_args: &EvalArgs,
        resolver_state: &ResolverState,
    ) -> FsResult<()>;
}

/// The instrumentation feature. Exposed as a set of instrumentation services.
pub struct InstrumentationFeature {
    pub event_emitter: Box<dyn DiscreteEventEmitter>,
    // TODO: add more instrumentation services here
}

/// The formatter feature. Exposed as a [CommandHandler] implementation.
pub struct FormatterFeature {
    pub command_handler: Box<dyn CommandHandler>,
}

pub struct LinterFeature {
    pub command_handler: Box<dyn CommandHandler>,
}

/// A feature stack is an object that can be initialized with type-erased
/// objects that implement feature-specific services.
pub struct FeatureStack {
    pub instrumentation: InstrumentationFeature,
    pub formatter: FormatterFeature,
    pub linter: LinterFeature,
    // TODO: add more features here
}

impl fmt::Debug for FeatureStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureStack").finish()
    }
}
