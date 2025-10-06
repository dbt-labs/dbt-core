use crate::v1::public::events::fusion::{
    node::{NodeEvaluated, NodeMaterialization, NodeOutcome, NodeSkipReason, NodeType},
    phase::ExecutionPhase,
};

// Display trait is intentionally not implemented to avoid inefficient usage.
// Prefer `node_type.as_ref()` or if you need String use `node_type.as_ref().to_string()`.

impl AsRef<str> for NodeType {
    fn as_ref(&self) -> &str {
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

pub fn dbt_core_event_code_for_node_evaluation_start(
    node_type: NodeType,
    phase: ExecutionPhase,
) -> Option<&'static str> {
    match phase {
        ExecutionPhase::Render => match node_type {
            // Q030: NodeCompiling — clear match for compiling models (render phase)
            NodeType::Model => Some("Q030"),
            NodeType::Function => Some("Q030"),

            // TODO: Snapshots/tests are compiled too in Core; map to Q030?
            // Leaving as None until confirmed to avoid over-reporting.
            NodeType::Snapshot => None,
            NodeType::Test => None,
            NodeType::UnitTest => None,

            // TODO: Seeds typically load CSVs; not a clear "compile" step.
            NodeType::Seed => None,

            // Likely not compiled/evaluated as standalone nodes in render
            NodeType::Source => None,
            NodeType::Macro => None,
            NodeType::DocsMacro => None,
            // Analysis nodes have SQL, but start mapping is ambiguous across commands
            NodeType::Analysis => None, // TODO: consider Q030 if/when analysis is compiled
            NodeType::Operation => None,
            NodeType::Exposure => None,
            NodeType::Metric => None,
            NodeType::SavedQuery => None,
            NodeType::SemanticModel => None,
            NodeType::Unspecified => None,
        },
        ExecutionPhase::Run => match node_type {
            // Q031: NodeExecuting — clear match for execution of runnable nodes
            NodeType::Model | NodeType::Seed | NodeType::Snapshot | NodeType::Operation => {
                Some("Q031")
            }

            // Tests have separate start lines in Core; not a clear Q031 mapping
            NodeType::Test | NodeType::UnitTest => None, // TODO: consider Q011 if we add generic start

            // Functions have an executable phase in the build command, but not run
            NodeType::Function => None,

            // Not executed directly in Run phase
            NodeType::Source
            | NodeType::Macro
            | NodeType::DocsMacro
            | NodeType::Analysis
            | NodeType::Exposure
            | NodeType::Metric
            | NodeType::SavedQuery
            | NodeType::SemanticModel
            | NodeType::Unspecified => None,
        },
        // Freshness analysis doesn't have a clear per-node "start" Q-code in Core
        ExecutionPhase::FreshnessAnalysis => None,
        // Other phases don't have clear node start legacy codes to map
        ExecutionPhase::Unspecified
        | ExecutionPhase::Clean
        | ExecutionPhase::LoadProject
        | ExecutionPhase::Parse
        | ExecutionPhase::Schedule
        | ExecutionPhase::InitAdapter
        | ExecutionPhase::DeferHydration
        | ExecutionPhase::TaskGraphBuild
        | ExecutionPhase::NodeCacheHydration
        | ExecutionPhase::Analyze
        | ExecutionPhase::Lineage
        | ExecutionPhase::Debug => None,
    }
}

fn dbt_core_event_code_for_node_evaluation_end(
    node_type: NodeType,
    phase: ExecutionPhase,
    node_outcome: NodeOutcome,
    node_skip_reason: Option<NodeSkipReason>,
) -> Option<&'static str> {
    // Handle explicit skip outcomes first where Core has distinct codes
    if node_outcome == NodeOutcome::Skipped {
        if matches!(node_skip_reason, Some(NodeSkipReason::NoOp)) {
            // Q019: LogNodeNoOpResult — clear match for NO-OP skips (e.g., ephemeral)
            return Some("Q019");
        }
        // Q034: SkippingDetails — general skip details in Core
        // TODO: confirm this is a correct mapping
        return Some("Q034");
    }

    match (phase, node_type) {
        // Q007: LogTestResult — clear match for test/unit test completion
        (ExecutionPhase::Run, NodeType::Test) | (ExecutionPhase::Run, NodeType::UnitTest) => {
            Some("Q007")
        }

        // Q018: LogFreshnessResult — clear match for source freshness completion
        (ExecutionPhase::FreshnessAnalysis, NodeType::Source) => Some("Q018"),

        // Q025: NodeFinished — generic finish for runnable nodes in Run phase
        (ExecutionPhase::Run, NodeType::Model)
        | (ExecutionPhase::Run, NodeType::Seed)
        | (ExecutionPhase::Run, NodeType::Snapshot)
        | (ExecutionPhase::Run, NodeType::Operation) => Some("Q025"),

        // Ambiguous/unsupported combinations — leave unmapped
        _ => None,
    }
}

pub fn update_dbt_core_event_code_for_node_evaluation_end(event: &mut NodeEvaluated) {
    if let Some(code) = dbt_core_event_code_for_node_evaluation_end(
        event.node_type(),
        event.phase(),
        event.node_outcome(),
        // Only pass `Some()` if it is actually set
        event.node_skip_reason.map(|_| event.node_skip_reason()),
    ) {
        event.dbt_core_event_code = Some(code.to_string());
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
            None,
            None,
            None,
            dbt_core_event_code_for_node_evaluation_start(node_type, phase).map(str::to_string),
            None,
        )
    }
}
