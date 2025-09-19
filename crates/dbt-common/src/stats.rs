use chrono::{DateTime, Local};
use dbt_telemetry::{
    NodeCacheReason, NodeErrorType, NodeEvaluated, NodeOutcome, NodeSkipReason, NodeType,
    TestEvaluationDetail, TestOutcome,
};
use proto_rust::impls::node::update_dbt_core_event_code_for_node_evaluation_end;
use proto_rust::v1::public::events::fusion::node::NodeCacheDetail;
use proto_rust::v1::public::events::fusion::node::node_evaluated::NodeOutcomeDetail;
use serde_json::Value as JsonValue;
use strum_macros::EnumString;

use std::fmt;
use std::time::{Duration, SystemTime};

// ------------------------------------------------------------------------------------------------
// Trivial Stats, foundation for run-results

#[derive(EnumString, PartialEq, Debug, Clone)]
pub enum NodeStatus {
    // the following states can be reported on the makefile
    Succeeded,
    Errored,
    TestWarned,
    TestPassed,
    SkippedUpstreamFailed,
    ReusedNoChanges(String),
    ReusedStillFresh(String),
    ReusedStillFreshNoChanges(String),
    NoOp,
}

impl NodeStatus {
    pub fn get_message(&self) -> Option<String> {
        match self {
            NodeStatus::ReusedNoChanges(message) => Some(message.clone()),
            NodeStatus::ReusedStillFresh(message) => Some(message.clone()),
            NodeStatus::ReusedStillFreshNoChanges(message) => Some(message.clone()),
            _ => None,
        }
    }
}
impl From<NodeStatus> for NodeOutcome {
    fn from(status: NodeStatus) -> Self {
        match status {
            NodeStatus::Succeeded | NodeStatus::TestWarned | NodeStatus::TestPassed => {
                NodeOutcome::Success
            }
            NodeStatus::Errored => NodeOutcome::Error,
            NodeStatus::SkippedUpstreamFailed => NodeOutcome::Skipped,
            NodeStatus::ReusedNoChanges(_) => NodeOutcome::Skipped,
            NodeStatus::ReusedStillFresh(_) => NodeOutcome::Skipped,
            NodeStatus::ReusedStillFreshNoChanges(_) => NodeOutcome::Skipped,
            NodeStatus::NoOp => NodeOutcome::Skipped,
        }
    }
}

/// TODO: this is a temporary reverse mapping from legacy status to new outcome model.
/// We should revert to using outcome directly in the task result and calculate status
/// from that - since outcome is more expressive, and current implementation is lossy.
pub fn update_node_outcome_from_legacy_status(event: &mut NodeEvaluated, status: NodeStatus) {
    match status {
        NodeStatus::Succeeded => {
            event.set_node_outcome(NodeOutcome::Success);
        }
        NodeStatus::TestPassed => {
            event.set_node_outcome(NodeOutcome::Success);

            event.node_outcome_detail = Some(NodeOutcomeDetail::NodeTestDetail(
                TestEvaluationDetail::new(TestOutcome::Passed, 0), // Todo - we need rows
            ));
        }
        NodeStatus::TestWarned => {
            event.set_node_outcome(NodeOutcome::Success);

            event.node_outcome_detail = Some(NodeOutcomeDetail::NodeTestDetail(
                TestEvaluationDetail::new(TestOutcome::Warned, 0), // Todo - we need rows
            ));
        }
        NodeStatus::Errored => {
            if event.node_type() == NodeType::Test || event.node_type() == NodeType::UnitTest {
                event.set_node_outcome(NodeOutcome::Success);

                event.node_outcome_detail = Some(NodeOutcomeDetail::NodeTestDetail(
                    TestEvaluationDetail::new(TestOutcome::Failed, 0), // Todo - we need rows
                ));
            } else {
                event.set_node_outcome(NodeOutcome::Error);
                event.set_node_error_type(NodeErrorType::User); // TODO: probably not necessary
            };
        }
        NodeStatus::SkippedUpstreamFailed => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Upstream);
        }
        NodeStatus::ReusedNoChanges(_) => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Cached);

            event.node_outcome_detail = Some(NodeOutcomeDetail::NodeCacheDetail(
                NodeCacheDetail::new(NodeCacheReason::NoChanges),
            ));
        }
        NodeStatus::ReusedStillFresh(_) => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Cached);

            event.node_outcome_detail = Some(NodeOutcomeDetail::NodeCacheDetail(
                NodeCacheDetail::new(NodeCacheReason::StillFresh),
            ));
        }
        NodeStatus::ReusedStillFreshNoChanges(_) => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Cached);

            event.node_outcome_detail = Some(NodeOutcomeDetail::NodeCacheDetail(
                NodeCacheDetail::new(NodeCacheReason::UpdateCriteriaNotMet),
            ));
        }
        NodeStatus::NoOp => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::NoOp);
        }
    };

    update_dbt_core_event_code_for_node_evaluation_end(event);
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status_str = match self {
            NodeStatus::Succeeded | NodeStatus::TestWarned | NodeStatus::TestPassed => "success",
            NodeStatus::Errored => "error",
            NodeStatus::SkippedUpstreamFailed => "skipped",
            NodeStatus::ReusedNoChanges(_)
            | NodeStatus::ReusedStillFresh(_)
            | NodeStatus::ReusedStillFreshNoChanges(_) => "reused",
            NodeStatus::NoOp => "noop",
        };
        write!(f, "{status_str}")
    }
}

#[derive(Debug, Clone)]
pub struct Stat {
    pub unique_id: String,
    pub num_rows: Option<usize>,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub status: NodeStatus,
    pub thread_id: String,
    pub adapter_response: Option<JsonValue>,
}

impl Stat {
    pub fn new(
        unique_id: String,
        start_time: SystemTime,
        num_rows: Option<usize>,
        status: NodeStatus,
        adapter_response: Option<JsonValue>,
    ) -> Self {
        let end_time = SystemTime::now();
        Stat {
            unique_id,
            num_rows,
            start_time,
            end_time,
            status,
            thread_id: format!(
                "Thread-{}",
                format!("{:?}", std::thread::current().id())
                    .trim_start_matches("ThreadId(")
                    .trim_end_matches(")")
            ),
            adapter_response,
        }
    }

    pub fn get_duration(&self) -> Duration {
        self.end_time
            .duration_since(self.start_time)
            .unwrap_or_default()
    }

    pub fn format_time(system_time: SystemTime) -> String {
        let datetime: DateTime<Local> = DateTime::from(system_time);
        datetime.format("%H:%M:%S").to_string()
    }
    pub fn status_string(&self) -> String {
        if self.status == NodeStatus::Succeeded
            && (self.unique_id.starts_with("test.") || self.unique_id.starts_with("unit_test."))
        {
            match self.num_rows {
                Some(0) => "Passed".to_string(),
                Some(_) => "Failed".to_string(),
                None => "Succeeded".to_string(),
            }
        } else {
            format!("{:?}", self.status)
        }
    }
    pub fn result_status_string(&self) -> String {
        match self.status {
            NodeStatus::Succeeded | NodeStatus::TestWarned | NodeStatus::TestPassed => {
                if self.unique_id.starts_with("test.") || self.unique_id.starts_with("unit_test.") {
                    match self.num_rows {
                        Some(0) => "pass".to_string(),
                        Some(_) => "fail".to_string(),
                        // Using "success" as fallback, though tests should have pass/fail
                        None => "success".to_string(),
                    }
                } else {
                    "success".to_string()
                }
            }
            NodeStatus::Errored => "error".to_string(),
            NodeStatus::SkippedUpstreamFailed => "skipped".to_string(),
            NodeStatus::ReusedNoChanges(_) => "reused".to_string(),
            NodeStatus::ReusedStillFresh(_) => "reused".to_string(),
            NodeStatus::ReusedStillFreshNoChanges(_) => "reused".to_string(),
            NodeStatus::NoOp => "skipped".to_string(),
        }
    }
}
