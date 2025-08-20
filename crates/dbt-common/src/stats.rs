use chrono::{DateTime, Local};
use dbt_telemetry::NodeExecutionStatus;
use std::fmt;
use std::time::{Duration, SystemTime};
use strum_macros::EnumString;

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

impl From<&NodeStatus> for NodeExecutionStatus {
    fn from(val: &NodeStatus) -> Self {
        match val {
            NodeStatus::Succeeded | NodeStatus::TestPassed | NodeStatus::TestWarned => {
                NodeExecutionStatus::Success
            }
            NodeStatus::Errored => NodeExecutionStatus::Error,
            NodeStatus::SkippedUpstreamFailed => NodeExecutionStatus::Skipped,
            NodeStatus::ReusedNoChanges(_) => NodeExecutionStatus::Reused,
            NodeStatus::ReusedStillFresh(_) => NodeExecutionStatus::Reused,
            NodeStatus::ReusedStillFreshNoChanges(_) => NodeExecutionStatus::Reused,
            NodeStatus::NoOp => NodeExecutionStatus::Skipped,
        }
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
}

impl Stat {
    pub fn new(
        unique_id: String,
        start_time: SystemTime,
        num_rows: Option<usize>,
        status: NodeStatus,
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
        if self.status == NodeStatus::Succeeded && self.unique_id.starts_with("test.")
            || self.unique_id.starts_with("unit_test.")
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
