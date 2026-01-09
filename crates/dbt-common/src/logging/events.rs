//! This module defines the events used for structured logging
//!
//! Event objects are meant to be passed to the [log] crate's `log!` macro
//! facade, under a correspondingly named key using the `serde` value capture.
//! Loggers interested in these events can then extract them from the log record
//! using the `from_record` method on the event type.

use crate::{
    constants::{
        ANALYZING, COMPILING, DEBUGGED, FAILED, HYDRATING, PARSING, PASSED, PREVIEWING, RENDERED,
        RENDERING, REUSED, RUNNING, SKIPPED, SUCCEEDED, WARNED,
    },
    pretty_string::{GREEN, RED, YELLOW},
    stats::NodeStatus,
};
use console::{Style, StyledObject};
use log::Level;
use serde::{Deserialize, Serialize};

type YmlValue = dbt_serde_yaml::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Severity {
    Error,
    Warning,
    Info,
    Debug,
}

// TODO: below are legacy logging structs, adapt them to the new logging system

// Mapping of action constants to event names for logging
#[derive(Debug, Clone)]
pub enum LogEvent {
    // Parse phase
    Parsing,
    // Render phase
    CompiledNode,
    Rendering,
    Hydrating,
    // Analyze phase
    Analyzing,
    // Debug phase
    DebugResult,
    // Run phase
    NodeStart,
    NodeSuccess,
    TestPass,
    TestWarn,
    ShowNode,
    Skipping,
    Failed,
    Reused,
    // Unknown phase
    Unknown(String),
}

impl LogEvent {
    pub fn name(&self) -> &str {
        match self {
            LogEvent::NodeStart => "NodeExecuting",
            LogEvent::NodeSuccess | LogEvent::Failed | LogEvent::TestPass | LogEvent::TestWarn => {
                "NodeFinished"
            }
            LogEvent::CompiledNode => "CompiledNode",
            LogEvent::ShowNode => "ShowNode",
            LogEvent::Skipping => "MarkSkippedChildren",
            LogEvent::DebugResult => "DebugCmdResult",
            LogEvent::Parsing => "ParseResource",
            LogEvent::Hydrating => "HydrateResource",
            LogEvent::Rendering => "CompileResource",
            LogEvent::Analyzing => "AnalyzeResource",
            LogEvent::Reused => "Reused",
            LogEvent::Unknown(_action) => "Unknown",
        }
    }

    pub fn code(&self) -> &str {
        // These are code from dbt-core
        match self {
            LogEvent::NodeStart => "Q024",
            LogEvent::NodeSuccess | LogEvent::Failed | LogEvent::TestPass | LogEvent::TestWarn => {
                "Q025"
            }
            LogEvent::CompiledNode => "Q042",
            LogEvent::ShowNode => "Q041",
            LogEvent::Skipping | LogEvent::Reused => "Z033",
            LogEvent::DebugResult => "Z048",
            LogEvent::Parsing
            | LogEvent::Analyzing
            | LogEvent::Rendering
            | LogEvent::Hydrating
            | LogEvent::Unknown(_) => "",
        }
    }
    pub fn level(&self) -> Level {
        match self {
            // Error level events
            // Info level events
            // (Everything related to run phase(execute sql remotely) should be at info level.)
            LogEvent::NodeStart
            | LogEvent::ShowNode
            | LogEvent::DebugResult
            | LogEvent::NodeSuccess
            | LogEvent::Skipping
            | LogEvent::TestPass
            | LogEvent::TestWarn
            | LogEvent::Failed
            | LogEvent::Parsing
            | LogEvent::Analyzing
            | LogEvent::Hydrating
            | LogEvent::Rendering
            | LogEvent::Reused
            | LogEvent::Unknown(_) => Level::Info,
            // Debug level events
            // (All events related to local phases: parse, compile should be at debug level.)
            LogEvent::CompiledNode => Level::Debug,
        }
    }
    pub fn phase(&self) -> &str {
        match self {
            LogEvent::Parsing => "parse",
            LogEvent::Analyzing => "analyze",
            LogEvent::Hydrating => "hydrate",
            LogEvent::Rendering | LogEvent::CompiledNode => "render",
            LogEvent::NodeStart | LogEvent::TestPass | LogEvent::ShowNode | LogEvent::TestWarn => {
                "run"
            }
            LogEvent::NodeSuccess | LogEvent::Failed | LogEvent::Skipping | LogEvent::Reused => {
                "completed"
            }
            _ => "",
        }
    }

    pub fn action(&self) -> String {
        match self {
            // Node execution events
            LogEvent::NodeStart => RUNNING.to_string(),
            LogEvent::NodeSuccess => SUCCEEDED.to_string(),
            LogEvent::Failed => FAILED.to_string(),
            // Node status events
            LogEvent::CompiledNode => RENDERED.to_string(),
            LogEvent::ShowNode => PREVIEWING.to_string(),
            LogEvent::TestPass => PASSED.to_string(),
            LogEvent::TestWarn => WARNED.to_string(),
            // Special events
            LogEvent::Skipping => SKIPPED.to_string(),
            LogEvent::DebugResult => DEBUGGED.to_string(),
            LogEvent::Parsing => PARSING.to_string(),
            LogEvent::Rendering => RENDERING.to_string(),
            LogEvent::Hydrating => HYDRATING.to_string(),
            LogEvent::Analyzing => ANALYZING.to_string(),
            LogEvent::Reused => REUSED.to_string(),
            LogEvent::Unknown(action) => action.to_string(),
        }
    }

    pub fn formatted_action(&self) -> StyledObject<String> {
        match self {
            // Node execution events
            LogEvent::NodeSuccess => GREEN.apply_to(SUCCEEDED.to_string()),
            LogEvent::Failed => RED.apply_to(FAILED.to_string()),
            LogEvent::Skipping => YELLOW.apply_to(SKIPPED.to_string()),
            LogEvent::TestPass => GREEN.apply_to(PASSED.to_string()),
            LogEvent::TestWarn => YELLOW.apply_to(WARNED.to_string()),
            LogEvent::Reused => GREEN.apply_to(REUSED.to_string()),
            // Node status events
            _ => Style::new().apply_to(self.action()),
        }
    }
}

impl From<NodeStatus> for LogEvent {
    fn from(value: NodeStatus) -> Self {
        match value {
            NodeStatus::Succeeded => LogEvent::NodeSuccess,
            NodeStatus::TestPassed => LogEvent::TestPass,
            NodeStatus::Errored => LogEvent::Failed,
            NodeStatus::TestWarned => LogEvent::TestWarn,
            NodeStatus::SkippedUpstreamFailed => LogEvent::Skipping,
            NodeStatus::ReusedNoChanges(_) => LogEvent::Reused,
            NodeStatus::ReusedStillFresh(_, _, _) => LogEvent::Reused,
            NodeStatus::ReusedStillFreshNoChanges(_) => LogEvent::Reused,
            NodeStatus::NoOp => LogEvent::Unknown("NoOp".to_string()),
        }
    }
}

impl From<&str> for LogEvent {
    fn from(value: &str) -> Self {
        match value {
            RUNNING => LogEvent::NodeStart,
            SUCCEEDED => LogEvent::NodeSuccess,
            PASSED => LogEvent::TestPass,
            RENDERED => LogEvent::CompiledNode,
            PREVIEWING => LogEvent::ShowNode,
            SKIPPED => LogEvent::Skipping,
            FAILED => LogEvent::Failed,
            DEBUGGED => LogEvent::DebugResult,
            PARSING => LogEvent::Parsing,
            ANALYZING => LogEvent::Analyzing,
            COMPILING => LogEvent::Rendering,
            RENDERING => LogEvent::Rendering,
            HYDRATING => LogEvent::Hydrating,
            REUSED => LogEvent::Skipping,
            _ => LogEvent::Unknown(value.to_string()),
        }
    }
}

pub struct FsInfo {
    pub event: LogEvent,
    pub target: String,
    pub data: Option<YmlValue>,
    pub desc: Option<String>,
}
impl FsInfo {
    pub fn is_phase_hydrate(&self) -> bool {
        self.event.phase() == "hydrate"
    }
    pub fn is_phase_parse(&self) -> bool {
        self.event.phase() == "parse"
    }
    pub fn is_phase_render(&self) -> bool {
        self.event.phase() == "render"
    }
    pub fn is_phase_analyze(&self) -> bool {
        self.event.phase() == "analyze"
    }
    pub fn is_phase_run(&self) -> bool {
        self.event.phase() == "run"
    }
    pub fn is_phase_completed(&self) -> bool {
        self.event.phase() == "completed"
    }
    pub fn is_phase_unknown(&self) -> bool {
        self.event.phase() == ""
    }
}
