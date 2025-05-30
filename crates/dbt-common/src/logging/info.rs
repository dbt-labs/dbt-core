use crate::constants::{
    COMPILED, COMPILING, DEBUGGED, FAILED, PARSING, PASS, PREVIEWING, RUNNING, SKIPPING, SUCCESS,
};
use log::Level;
use serde_json::Value;

// Mapping of action constants to event names for logging
#[derive(Debug, Clone)]
pub enum LogEvent {
    NodeStart,
    NodeSuccess,
    TestPass,
    CompiledNode,
    ShowNode,
    Skipping,
    Failed,
    DebugResult,
    Parsing,
    Compiling,
    Unknown(String),
}

impl LogEvent {
    pub fn name(&self) -> &str {
        match self {
            LogEvent::NodeStart => "NodeExecuting",
            LogEvent::NodeSuccess | LogEvent::Failed | LogEvent::TestPass => "NodeFinished",
            LogEvent::CompiledNode => "CompiledNode",
            LogEvent::ShowNode => "ShowNode",
            LogEvent::Skipping => "MarkSkippedChildren",
            LogEvent::DebugResult => "DebugCmdResult",
            LogEvent::Parsing => "ParseResource",
            LogEvent::Compiling => "CompileResource",
            LogEvent::Unknown(_action) => "Unknown",
        }
    }

    pub fn code(&self) -> &str {
        // These are code from dbt-core
        match self {
            LogEvent::NodeStart => "Q024",
            LogEvent::NodeSuccess | LogEvent::Failed | LogEvent::TestPass => "Q025",
            LogEvent::CompiledNode => "Q042",
            LogEvent::ShowNode => "Q041",
            LogEvent::Skipping => "Z033",
            LogEvent::DebugResult => "Z048",
            LogEvent::Parsing | LogEvent::Compiling | LogEvent::Unknown(_) => "",
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
            | LogEvent::Failed
            | LogEvent::Parsing
            | LogEvent::Compiling
            | LogEvent::Unknown(_) => Level::Info,
            // Debug level events
            // (All events related to local phases: parse, compile should be at debug level.)
            LogEvent::CompiledNode => Level::Debug,
        }
    }
    pub fn phase(&self) -> &str {
        match self {
            LogEvent::Parsing => "parse",
            LogEvent::Compiling | LogEvent::CompiledNode => "compile",
            LogEvent::NodeStart
            | LogEvent::NodeSuccess
            | LogEvent::Failed
            | LogEvent::TestPass
            | LogEvent::ShowNode
            | LogEvent::Skipping => "run",
            _ => "",
        }
    }

    pub fn action(&self) -> String {
        match self {
            // Node execution events
            LogEvent::NodeStart => RUNNING.to_string(),
            LogEvent::NodeSuccess => SUCCESS.to_string(),
            LogEvent::Failed => FAILED.to_string(),
            // Node status events
            LogEvent::CompiledNode => COMPILED.to_string(),
            LogEvent::ShowNode => PREVIEWING.to_string(),
            LogEvent::TestPass => PASS.to_string(),
            // Special events
            LogEvent::Skipping => SKIPPING.to_string(),
            LogEvent::DebugResult => DEBUGGED.to_string(),
            LogEvent::Parsing => PARSING.to_string(),
            LogEvent::Compiling => COMPILING.to_string(),
            LogEvent::Unknown(action) => action.to_string(),
        }
    }
}

impl From<&str> for LogEvent {
    fn from(value: &str) -> Self {
        match value {
            RUNNING => LogEvent::NodeStart,
            SUCCESS => LogEvent::NodeSuccess,
            PASS => LogEvent::TestPass,
            COMPILED => LogEvent::CompiledNode,
            PREVIEWING => LogEvent::ShowNode,
            SKIPPING => LogEvent::Skipping,
            FAILED => LogEvent::Failed,
            DEBUGGED => LogEvent::DebugResult,
            PARSING => LogEvent::Parsing,
            COMPILING => LogEvent::Compiling,
            _ => LogEvent::Unknown(value.to_string()),
        }
    }
}

pub struct FsInfo {
    pub event: LogEvent,
    pub target: String,
    pub data: Option<Value>,
    pub desc: Option<String>,
}
impl FsInfo {
    pub fn is_phase_compile(&self) -> bool {
        self.event.phase() == "compile"
    }
    pub fn is_phase_parse(&self) -> bool {
        self.event.phase() == "parse"
    }
    pub fn is_phase_run(&self) -> bool {
        self.event.phase() == "run"
    }
    pub fn is_phase_unknown(&self) -> bool {
        self.event.phase() == ""
    }
}
