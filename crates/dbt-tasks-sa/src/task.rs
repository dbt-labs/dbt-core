use std::sync::Arc;

use dbt_common::collections::DashMap;
use dbt_scheduler::instructions::{LpInstruction, SqlInstruction};
use minijinja::Value as MinijinjaValue;

// Unified result type for all task outputs (render and/or analyze phase)
#[derive(Debug, Clone)]
pub struct TaskResult {
    pub sql_instruction: SqlInstruction,
    pub config_map: Arc<DashMap<String, MinijinjaValue>>,
    /// Present only when the analyze phase ran (Local/Sidecar/Service execution).
    pub lp_instruction: Option<LpInstruction>,
}
