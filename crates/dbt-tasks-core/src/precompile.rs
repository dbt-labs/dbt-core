use dbt_common::io_args::StaticAnalysisKind;
use dbt_schemas::schemas::{IntrospectionKind, Nodes};

use crate::RunTasksArgs;

pub trait StaticAnalysisBuckets {
    fn global_static_analysis(&self) -> Option<StaticAnalysisKind>;

    fn in_off_closure(&self, node_id: &str) -> bool;
    fn in_baseline_closure(&self, node_id: &str) -> bool;
    fn in_dynamic_closure(&self, node_id: &str) -> bool;
    fn in_baseline_or_off_closure(&self, node_id: &str) -> bool;

    fn dynamic_node(&self, node_id: &str) -> Option<IntrospectionKind>;
    fn has_dynamic_closure(&self) -> bool;

    fn will_build_phased_task_graph(&self, arg: &RunTasksArgs, task_nodes: &Nodes);
}
