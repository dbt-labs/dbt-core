use std::fmt;

use dbt_schemas::schemas::telemetry::ExecutionPhase;

/// Stands for Task Phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum TP {
    Render,
    Analyze,
    Run,
    Compare,
    Show,
}

impl fmt::Display for TP {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TP::Render => "render",
                TP::Analyze => "analyze",
                TP::Run => "run",
                TP::Compare => "compare",
                TP::Show => "show",
            }
        )
    }
}

impl From<TP> for ExecutionPhase {
    fn from(tp: TP) -> Self {
        match tp {
            TP::Render => ExecutionPhase::Render,
            TP::Analyze => ExecutionPhase::Analyze,
            TP::Run => ExecutionPhase::Run,
            TP::Show => ExecutionPhase::Run,
            TP::Compare => ExecutionPhase::Compare,
        }
    }
}
