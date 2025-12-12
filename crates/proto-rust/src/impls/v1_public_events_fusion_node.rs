use crate::v1::public::events::fusion::{
    node::{
        NodeCacheDetail, NodeEvaluated, NodeMaterialization, NodeOutcome, NodeProcessed,
        NodeSkipReason, NodeSkipUpstreamDetail, NodeType, SourceFreshnessDetail,
        TestEvaluationDetail, TestOutcome, node_evaluated, node_processed,
    },
    phase::ExecutionPhase,
};

// Display trait is intentionally not implemented to avoid inefficient usage.
// Prefer `node_type.as_ref()` or if you need String use `node_type.as_ref().to_string()`.

impl NodeType {
    pub const fn as_static_ref(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Model => "model",
            Self::Seed => "seed",
            Self::Snapshot => "snapshot",
            Self::Source => "source",
            Self::Test => "test",
            Self::UnitTest => "unit_test",
            Self::Macro => "macro",
            Self::DocsMacro => "docs_macro",
            Self::Analysis => "analysis",
            Self::Operation => "operation",
            Self::Exposure => "exposure",
            Self::Metric => "metric",
            Self::SavedQuery => "saved_query",
            Self::SemanticModel => "semantic_model",
            Self::Function => "function",
        }
    }
}

impl NodeType {
    pub const fn pretty(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Model => "model",
            Self::Seed => "seed",
            Self::Snapshot => "snapshot",
            Self::Source => "source",
            Self::Test => "test",
            Self::UnitTest => "unit test",
            Self::Macro => "macro",
            Self::DocsMacro => "docs macro",
            Self::Analysis => "analysis",
            Self::Operation => "operation",
            Self::Exposure => "exposure",
            Self::Metric => "metric",
            Self::SavedQuery => "saved query",
            Self::SemanticModel => "semantic model",
            Self::Function => "function",
        }
    }
}

impl NodeMaterialization {
    pub const fn as_static_ref(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Snapshot => "snapshot",
            Self::Seed => "seed",
            Self::View => "view",
            Self::Table => "table",
            Self::Incremental => "incremental",
            Self::MaterializedView => "materialized_view",
            Self::External => "external",
            Self::Test => "test",
            Self::Ephemeral => "ephemeral",
            Self::Unit => "unit_test",
            Self::Analysis => "analysis",
            Self::StreamingTable => "streaming_table",
            Self::DynamicTable => "dynamic_table",
            Self::Function => "function",
            Self::Custom => "custom",
        }
    }
}

fn dbt_core_event_code_for_node_evaluation(phase: ExecutionPhase) -> Option<&'static str> {
    // Matches json_compat_layer behavior: emit Q030/Q031 for all nodes in Render/Run phases
    match phase {
        // Q030: NodeCompiling — emitted for all nodes in Render phase
        ExecutionPhase::Render => Some("Q030"),
        // Q031: NodeExecuting — emitted for all nodes in Run phase
        ExecutionPhase::Run => Some("Q031"),
        // No codes for other phases
        _ => None,
    }
}

fn dbt_core_event_code_for_node_processed_end(
    node_type: NodeType,
    node_outcome: NodeOutcome,
    node_skip_reason: Option<NodeSkipReason>,
) -> Option<&'static str> {
    // Handle explicit skip outcomes first where Core has distinct codes
    if node_outcome == NodeOutcome::Skipped {
        if matches!(node_skip_reason, Some(NodeSkipReason::NoOp)) {
            // Q019: LogNodeNoOpResult — clear match for NO-OP skips (e.g., ephemeral)
            return Some("Q019");
        }
        // Q034: SkippingDetails
        return Some("Q034");
    }

    // NodeProcessed spans entire node execution, so we use node type to determine the code
    match node_type {
        // Q007: LogTestResult — info level, specific to tests
        NodeType::Test | NodeType::UnitTest => Some("Q007"),

        // Q012: LogModelResult — info level, preferred over generic Q025
        NodeType::Model => Some("Q012"),

        // Q016: LogSeedResult — info level, preferred over generic Q025
        NodeType::Seed => Some("Q016"),

        // Q015: LogSnapshotResult — info level, preferred over generic Q025
        NodeType::Snapshot => Some("Q015"),

        // Q047: LogFunctionResult — info level, specific to functions
        NodeType::Function => Some("Q047"),

        // Q018: LogFreshnessResult — info level, for source freshness
        NodeType::Source => Some("Q018"),

        // Q008: LogNodeResult — info level, generic fallback for other node types
        _ => Some("Q008"),
    }
}

pub fn update_dbt_core_event_code_for_node_processed_end(event: &mut NodeProcessed) {
    if let Some(code) = dbt_core_event_code_for_node_processed_end(
        event.node_type(),
        event.node_outcome(),
        // Only pass `Some()` if it is actually set
        event.node_skip_reason.map(|_| event.node_skip_reason()),
    ) {
        event.dbt_core_event_code = code.to_string();
    }
}

pub enum NodeEvent<'a> {
    Evaluated(&'a NodeEvaluated),
    Processed(&'a NodeProcessed),
}

impl<'a> From<&'a NodeEvaluated> for NodeEvent<'a> {
    fn from(ne: &'a NodeEvaluated) -> Self {
        NodeEvent::Evaluated(ne)
    }
}

impl<'a> From<&'a NodeProcessed> for NodeEvent<'a> {
    fn from(np: &'a NodeProcessed) -> Self {
        NodeEvent::Processed(np)
    }
}

pub enum AnyNodeOutcomeDetail<'a> {
    NodeCacheDetail(&'a NodeCacheDetail),
    NodeTestDetail(&'a TestEvaluationDetail),
    NodeFreshnessOutcome(&'a SourceFreshnessDetail),
    NodeSkipUpstreamDetail(&'a NodeSkipUpstreamDetail),
}

impl<'a> From<&'a node_processed::NodeOutcomeDetail> for AnyNodeOutcomeDetail<'a> {
    fn from(detail: &'a node_processed::NodeOutcomeDetail) -> Self {
        match detail {
            node_processed::NodeOutcomeDetail::NodeCacheDetail(cache_detail) => {
                AnyNodeOutcomeDetail::NodeCacheDetail(cache_detail)
            }
            node_processed::NodeOutcomeDetail::NodeTestDetail(test_detail) => {
                AnyNodeOutcomeDetail::NodeTestDetail(test_detail)
            }
            node_processed::NodeOutcomeDetail::NodeFreshnessOutcome(freshness_detail) => {
                AnyNodeOutcomeDetail::NodeFreshnessOutcome(freshness_detail)
            }
            node_processed::NodeOutcomeDetail::NodeSkipUpstreamDetail(skip_detail) => {
                AnyNodeOutcomeDetail::NodeSkipUpstreamDetail(skip_detail)
            }
        }
    }
}

impl<'a> From<&'a node_evaluated::NodeOutcomeDetail> for AnyNodeOutcomeDetail<'a> {
    fn from(detail: &'a node_evaluated::NodeOutcomeDetail) -> Self {
        match detail {
            node_evaluated::NodeOutcomeDetail::NodeCacheDetail(cache_detail) => {
                AnyNodeOutcomeDetail::NodeCacheDetail(cache_detail)
            }
            node_evaluated::NodeOutcomeDetail::NodeTestDetail(test_detail) => {
                AnyNodeOutcomeDetail::NodeTestDetail(test_detail)
            }
            node_evaluated::NodeOutcomeDetail::NodeFreshnessOutcome(freshness_detail) => {
                AnyNodeOutcomeDetail::NodeFreshnessOutcome(freshness_detail)
            }
            node_evaluated::NodeOutcomeDetail::NodeSkipUpstreamDetail(skip_detail) => {
                AnyNodeOutcomeDetail::NodeSkipUpstreamDetail(skip_detail)
            }
        }
    }
}

pub fn get_node_outcome_detail<'a>(node: NodeEvent<'a>) -> Option<AnyNodeOutcomeDetail<'a>> {
    match node {
        NodeEvent::Evaluated(ne) => ne.node_outcome_detail.as_ref().map(|dtl| dtl.into()),
        NodeEvent::Processed(np) => np.node_outcome_detail.as_ref().map(|dtl| dtl.into()),
    }
}

/// Extract test outcome from test details if available
pub fn get_test_outcome(node: NodeEvent) -> Option<TestOutcome> {
    get_node_outcome_detail(node).and_then(|detail| {
        if let AnyNodeOutcomeDetail::NodeTestDetail(test_detail) = detail {
            Some(test_detail.test_outcome())
        } else {
            None
        }
    })
}

/// Extract test outcome from test details if available
pub fn get_cache_detail(node: NodeEvent<'_>) -> Option<&'_ NodeCacheDetail> {
    get_node_outcome_detail(node).and_then(|detail| {
        if let AnyNodeOutcomeDetail::NodeCacheDetail(cache_detail) = detail {
            Some(cache_detail)
        } else {
            None
        }
    })
}

impl From<node_evaluated::NodeOutcomeDetail> for node_processed::NodeOutcomeDetail {
    fn from(detail: node_evaluated::NodeOutcomeDetail) -> Self {
        match detail {
            node_evaluated::NodeOutcomeDetail::NodeCacheDetail(d) => {
                node_processed::NodeOutcomeDetail::NodeCacheDetail(d)
            }
            node_evaluated::NodeOutcomeDetail::NodeTestDetail(d) => {
                node_processed::NodeOutcomeDetail::NodeTestDetail(d)
            }
            node_evaluated::NodeOutcomeDetail::NodeFreshnessOutcome(d) => {
                node_processed::NodeOutcomeDetail::NodeFreshnessOutcome(d)
            }

            node_evaluated::NodeOutcomeDetail::NodeSkipUpstreamDetail(d) => {
                node_processed::NodeOutcomeDetail::NodeSkipUpstreamDetail(d)
            }
        }
    }
}

impl NodeEvaluated {
    /// Creates a new `NodeEvaluated` event indicating start of a node processing.
    ///
    /// This is thin wrapper around `new` that avoids the need to pass
    /// `None` for fields that are only known at the end of processing.
    ///
    /// # Arguments
    /// * `unique_id` - unique_id is the globally unique identifier for this node.
    /// * `name` - Node name.
    /// * `database` - Database where this node will be created if applicable.
    /// * `schema` - Schema where this node will be created if applicable.
    /// * `identifier` - Name of the relation (table, view, etc.) that will be created for this node if applicable.
    /// * `materialization` - How this node is materialized in the data warehouse.
    /// * `custom_materialization` - If materialization == NODE_MATERIALIZATION_CUSTOM, this field contains the custom materialization name.
    /// * `node_type` - Type of node being evaluated. Known as `resource_type` in dbt core.
    /// * `phase` - Execution phase during which this node was evaluated.
    /// * `relative_path` - The file path to the node, relative to the project root.
    /// * `defined_at_line` - The line number in the file where the test is defined, if applicable.
    /// * `defined_at_column` - The column number in the file where the test is defined, if applicable.
    /// * `node_checksum` - The checksum of the node's contents
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        unique_id: String,
        name: String,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        materialization: Option<NodeMaterialization>,
        custom_materialization: Option<String>,
        node_type: NodeType,
        phase: ExecutionPhase,
        relative_path: String,
        defined_at_line: Option<u32>,
        defined_at_column: Option<u32>,
        node_checksum: String,
    ) -> Self {
        Self::new(
            unique_id,
            name,
            database,
            schema,
            identifier,
            materialization,
            custom_materialization,
            node_type,
            NodeOutcome::Unspecified,
            phase,
            relative_path,
            defined_at_line,
            defined_at_column,
            node_checksum,
            Some(false), // whether sao_enabled or not is only known at task runtime
            None,        // node_error_type
            None,        // node_cancel_reason
            None,        // node_skip_reason
            dbt_core_event_code_for_node_evaluation(phase).map(str::to_string),
            None, // rows_affected
            None, // node_outcome_detail
        )
    }
}

impl NodeProcessed {
    /// Creates a new `NodeProcessed` event indicating start of node processing across all phases.
    ///
    /// This is thin wrapper around `new` that avoids the need to pass
    /// `None` for fields that are only known at the end of processing,
    /// and auto assigns the appropriate dbt core event code (Q024) for start of node processing.
    ///
    /// # Arguments
    /// * `unique_id` - unique_id is the globally unique identifier for this node.
    /// * `name` - Node name.
    /// * `database` - Database where this node will be created if applicable.
    /// * `schema` - Schema where this node will be created if applicable.
    /// * `identifier` - Name of the relation (table, view, etc.) that will be created for this node if applicable.
    /// * `materialization` - How this node is materialized in the data warehouse.
    /// * `custom_materialization` - If materialization == NODE_MATERIALIZATION_CUSTOM, this field contains the custom materialization name.
    /// * `node_type` - Type of node being evaluated. Known as `resource_type` in dbt core.
    /// * `last_phase` - Last execution phase that is expected to be run for this node.
    /// * `relative_path` - The file path to the node, relative to the project root.
    /// * `defined_at_line` - The line number in the file where the test is defined, if applicable.
    /// * `defined_at_column` - The column number in the file where the test is defined, if applicable.
    /// * `node_checksum` - The checksum of the node's contents
    /// * `in_selection` - Whether the node is in the selection set for execution
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        unique_id: String,
        name: String,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        materialization: Option<NodeMaterialization>,
        custom_materialization: Option<String>,
        node_type: NodeType,
        last_phase: Option<ExecutionPhase>,
        relative_path: String,
        defined_at_line: Option<u32>,
        defined_at_column: Option<u32>,
        node_checksum: String,
        in_selection: bool,
    ) -> Self {
        Self::new(
            unique_id,
            name,
            database,
            schema,
            identifier,
            materialization,
            custom_materialization,
            node_type,
            NodeOutcome::Unspecified,
            last_phase.unwrap_or_default(),
            relative_path,
            defined_at_line,
            defined_at_column,
            node_checksum,
            Some(false), // whether sao_enabled or not is only known at task runtime
            None,        // node_error_type
            None,        // node_cancel_reason
            None,        // node_skip_reason
            "Q024".to_string(), // dbt_core_event_code
            None,        // duration_ms will be calculated during task execution
            in_selection,
            None, // rows_affected
            None, // node_outcome_detail
        )
    }
}
