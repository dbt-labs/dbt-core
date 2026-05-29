use std::path::Path;

use uuid::Uuid;

/// Emit discrete events during dbt execution.
///
/// There are multiple implementations of this trait, depending on the context.
/// The main one is the `FusionSaEventEmitter`, which is used in the
/// source-available version of dbt Fusion.
pub trait DiscreteEventEmitter: Send + Sync {
    fn invocation_start_event(
        &self,
        invocation_id: &Uuid,
        root_project_name: &str,
        profile_path: Option<&Path>,
        command: String,
    );

    fn dbt_distribution(&self) -> &'static str;

    // TODO(felipecrv): move more events to this trait
    // so we can use different implementations in different contexts
}
