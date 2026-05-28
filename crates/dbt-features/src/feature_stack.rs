use std::fmt;

use dbt_common::DiscreteEventEmitter;
use dbt_common::cancellation::CancellationTokenSource;
use dbt_common::fail_fast::FailFast;

use crate::adapter::AdapterFeature;
use crate::antlr_parser::AntlrParserFeature;
use crate::cli_extension::CliExtensionFeature;
use crate::index::IndexFeature;
use crate::metricflow::MetricflowFeature;
use crate::sidecar::SidecarFeature;
use crate::task_runner::TaskRunnerFeature;
use crate::tracing::TracingFeature;

/// The instrumentation feature. Exposed as a set of instrumentation services.
pub struct InstrumentationFeature {
    pub event_emitter: Box<dyn DiscreteEventEmitter>,
    // TODO: add more instrumentation services here
}

/// A feature stack is an object that can be initialized with type-erased
/// objects that implement feature-specific services.
pub struct FeatureStack {
    pub instrumentation: InstrumentationFeature,
    pub cli_extension: CliExtensionFeature,
    pub index: IndexFeature,
    pub tracing: TracingFeature,
    pub adapter: AdapterFeature,
    pub antlr_parser: AntlrParserFeature,
    pub sidecar: SidecarFeature,
    pub metricflow: MetricflowFeature,
    pub task_runner: TaskRunnerFeature,
    // TODO: add more features here
    /// Global [CancelltionTokenSource] that can be used to signal cancellation to
    /// tasks running in other threads from a signal handler (e.g. Ctrl+C).
    pub cancellation_token_source: CancellationTokenSource,
    /// Per CLI invocation fail-fast signal.
    ///
    /// Each invocation of the CLI (or test) gets its own isolated signal
    /// so concurrent runs don't interfere with each other.
    pub fail_fast: FailFast,
}

impl fmt::Debug for FeatureStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureStack").finish()
    }
}
