use std::sync::Arc;

use error::CodeLocation;

use crate::{io_utils::StatusReporter, logging::LogEvent, stats::NodeStatus};

pub fn report_completed(
    node_status: &NodeStatus,
    defined_at: Option<CodeLocation>,
    display_path: &str,
    with_cache: bool,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    let Some(status_reporter) = status_reporter else {
        return;
    };

    if !matches!(node_status, &NodeStatus::NoOp) {
        let log_event: LogEvent = node_status.clone().into();

        let desc = if matches!(node_status, NodeStatus::Succeeded) {
            with_cache.then_some("New changes detected".to_string())
        } else if matches!(node_status, NodeStatus::TestWarned | NodeStatus::Errored)
            && let Some(location) = defined_at
        {
            Some(location.to_string())
        } else {
            node_status.get_message()
        };

        status_reporter.show_progress(log_event.action().as_str(), display_path, desc.as_deref());
    }
}
