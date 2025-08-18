//! This module defines the events used for structured logging
//!
//! Event objects are meant to be passed to the [log] crate's `log!` macro
//! facade, under a correspondingly named key using the `serde` value capture.
//! For example, to log a `StatEvent::Counter` event, you would use:
//! ```
//! log!(..., _STAT_EVENT_:serde = StatEvent::counter("my_counter", 1));
//! ```
//! Loggers interested in these events can then extract them from the log record
//! using the `from_record` method on the event type.

use std::time::Duration;

use crate::{
    CodeLocation,
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

/// Events related to statistics gathering
#[derive(Clone, Serialize, Deserialize)]
pub enum StatEvent {
    /// Increments a counter by the specified step.
    Counter { name: String, step: i64 },
    /// Records a timer event with the specified name and duration.
    Timer { name: String, duration: Duration },
    /// An error with a specified error code and severity.
    FsError { code: u16, severity: Severity },
}

impl StatEvent {
    /// Creates a counter event with the specified name and step.
    pub fn counter(name: impl Into<String>, step: i64) -> Self {
        StatEvent::Counter {
            name: name.into(),
            step,
        }
    }

    /// Creates a timer event with the specified name and duration.
    pub fn timer(name: impl Into<String>, duration: Duration) -> Self {
        StatEvent::Timer {
            name: name.into(),
            duration,
        }
    }

    /// Creates an error event with the specified error code.
    ///
    /// Note the designation of `FsError` or `FsWarning` should be interpreted
    /// as a "suggestion" from the caller -- the logger implementations
    /// responding to these events are free to redesignate their severity based
    /// on the specified error code.
    pub fn fs_error(code: u16) -> Self {
        StatEvent::FsError {
            code,
            severity: Severity::Error,
        }
    }

    /// Creates a warning event with the specified error code.
    ///
    /// Note the designation of `FsError` or `FsWarning` should be interpreted
    /// as a "suggestion" from the caller -- the logger implementations
    /// responding to these events are free to redesignate their severity based
    /// on the specified error code.
    pub fn fs_warning(code: u16) -> Self {
        StatEvent::FsError {
            code,
            severity: Severity::Warning,
        }
    }

    /// Extracts a `StatEvent` from a log record.
    pub fn from_record(record: &log::Record) -> Option<Self> {
        let event = record
            .key_values()
            .get(log::kv::Key::from("_STAT_EVENT_"))?
            .serialize(serde_json::value::Serializer)
            .ok()?;
        serde_json::from_value(event).ok()
    }
}

/// An event representing an error, with a severity level, code, optional location,
/// and a message.
#[derive(Clone, Serialize, Deserialize)]
pub struct ErrorEvent {
    pub severity: Severity,
    pub code: u16,
    pub location: Option<CodeLocation>,
    pub message: String,
}

impl ErrorEvent {
    /// Creates a new `ErrorEvent` with the specified severity, code, location, and message.
    pub fn new(
        severity: Severity,
        code: u16,
        location: Option<CodeLocation>,
        message: impl Into<String>,
    ) -> Self {
        ErrorEvent {
            severity,
            code,
            location,
            message: message.into(),
        }
    }

    /// Extracts an `ErrorEvent` from a log record.
    pub fn from_record(record: &log::Record) -> Option<Self> {
        let event = record
            .key_values()
            .get(log::kv::Key::from("_ERROR_EVENT_"))?
            .serialize(serde_json::value::Serializer)
            .ok()?;
        serde_json::from_value(event).ok()
    }
}

/// Events related to terminal progress indicators
#[derive(Serialize, Deserialize, Default)]
pub enum TermEvent {
    StartSpinner {
        uid: String,
        prefix: Option<String>,
    },
    StartBar {
        uid: String,
        prefix: Option<String>,
        total: u64,
    },
    StartPlainBar {
        uid: String,
        prefix: Option<String>,
        total: u64,
    },
    AddBarContextItem {
        uid: String,
        item: String,
    },
    FinishBarContextItem {
        uid: String,
        item: String,
    },
    AddSpinnerContextItem {
        uid: String,
        item: String,
    },
    FinishSpinnerContextItem {
        uid: String,
        item: String,
    },
    IncBar {
        uid: String,
        inc: u64,
    },
    RemoveSpinner {
        uid: String,
    },
    RemoveBar {
        uid: String,
    },
    #[default]
    Noop,
}

impl TermEvent {
    pub fn start_spinner(uid: String) -> Self {
        TermEvent::StartSpinner { uid, prefix: None }
    }

    pub fn start_bar(uid: String, total: u64) -> Self {
        TermEvent::StartBar {
            uid,
            prefix: None,
            total,
        }
    }

    pub fn start_plain_bar(uid: String, total: u64) -> Self {
        TermEvent::StartPlainBar {
            uid,
            prefix: None,
            total,
        }
    }

    pub fn add_bar_context_item(uid: String, item: String) -> Self {
        TermEvent::AddBarContextItem { uid, item }
    }

    pub fn finish_bar_context_item(uid: String, item: String) -> Self {
        TermEvent::FinishBarContextItem { uid, item }
    }

    pub fn add_spinner_context_item(uid: String, item: String) -> Self {
        TermEvent::AddSpinnerContextItem { uid, item }
    }

    pub fn finish_spinner_context_item(uid: String, item: String) -> Self {
        TermEvent::FinishSpinnerContextItem { uid, item }
    }

    pub fn inc_bar(uid: String, inc: u64) -> Self {
        TermEvent::IncBar { uid, inc }
    }

    pub fn remove_spinner(uid: String) -> Self {
        TermEvent::RemoveSpinner { uid }
    }

    pub fn remove_bar(uid: String) -> Self {
        TermEvent::RemoveBar { uid }
    }

    pub fn from_record(record: &log::Record) -> Option<Self> {
        Self::deserialize(
            record
                .key_values()
                .get(log::kv::Key::from("_TERM_EVENT_"))?
                .serialize(serde_json::value::Serializer)
                .ok()?,
        )
        .ok()
    }
}

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
            NodeStatus::ReusedStillFresh(_) => LogEvent::Reused,
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
