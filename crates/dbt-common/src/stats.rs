use chrono::{DateTime, Local};
use dbt_telemetry::NodeOutcome;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
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
    ReusedStillFresh(String, u64, u64),
    ReusedStillFreshNoChanges(String),
    NoOp,
}

impl NodeStatus {
    pub fn get_message(&self) -> Option<String> {
        match self {
            NodeStatus::ReusedNoChanges(message) => Some(message.clone()),
            NodeStatus::ReusedStillFresh(message, _, _) => Some(message.clone()),
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
            NodeStatus::ReusedStillFresh(_, _, _) => NodeOutcome::Skipped,
            NodeStatus::ReusedStillFreshNoChanges(_) => NodeOutcome::Skipped,
            NodeStatus::NoOp => NodeOutcome::Skipped,
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
            | NodeStatus::ReusedStillFresh(_, _, _)
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
    pub message: Option<String>,
}

impl Stat {
    pub fn new(
        unique_id: String,
        start_time: SystemTime,
        num_rows: Option<usize>,
        status: NodeStatus,
        message: Option<String>,
    ) -> Self {
        let end_time = SystemTime::now();

        // Hash the thread ID to get a stable u64 representation
        let thread_id_hash = {
            let id = std::thread::current().id();
            let mut hasher = DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish()
        };

        Stat {
            unique_id,
            num_rows,
            start_time,
            end_time,
            status,
            thread_id: format!("Thread-{}", thread_id_hash),
            message,
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
            NodeStatus::Succeeded
                if self.unique_id.starts_with("test.")
                    || self.unique_id.starts_with("unit_test.") =>
            {
                match self.num_rows {
                    Some(0) => "pass".to_string(),
                    Some(_) => "fail".to_string(),
                    // Using "pass" as fallback, though tests should have pass/fail
                    None => "pass".to_string(),
                }
            }
            NodeStatus::Errored
                if self.unique_id.starts_with("test.")
                    || self.unique_id.starts_with("unit_test.") =>
            {
                match self.num_rows {
                    Some(0) => "error".to_string(),
                    Some(_) => "fail".to_string(),
                    None => "error".to_string(),
                }
            }
            NodeStatus::Succeeded => "success".to_string(),
            NodeStatus::TestWarned => "warn".to_string(),
            NodeStatus::TestPassed => "pass".to_string(),
            NodeStatus::Errored => "error".to_string(),
            NodeStatus::SkippedUpstreamFailed => "skipped".to_string(),
            NodeStatus::ReusedNoChanges(_) => "reused".to_string(),
            NodeStatus::ReusedStillFresh(_, _, _) => "reused".to_string(),
            NodeStatus::ReusedStillFreshNoChanges(_) => "reused".to_string(),
            NodeStatus::NoOp => "skipped".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_id_format() {
        let stat = Stat::new(
            "test.model".to_string(),
            SystemTime::now(),
            None,
            NodeStatus::Succeeded,
            None,
        );

        // Thread ID should be in format "Thread-<number>"
        assert!(
            stat.thread_id.starts_with("Thread-"),
            "thread_id should start with 'Thread-', got: {}",
            stat.thread_id
        );

        // Extract the number part and verify it's a valid number
        let number_part = stat.thread_id.trim_start_matches("Thread-");
        assert!(
            number_part.parse::<u64>().is_ok(),
            "thread_id should end with a number, got: {}",
            stat.thread_id
        );
    }
}
