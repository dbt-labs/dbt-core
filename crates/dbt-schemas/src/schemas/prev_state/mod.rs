use super::{RunResultsArtifact, manifest::DbtManifest, sources::FreshnessResultsArtifact};
use crate::schemas::DbtSource;
use crate::schemas::common::{DbtQuoting, ResolvedQuoting};
use crate::schemas::manifest::nodes_from_dbt_manifest;
use crate::schemas::project::configs::common::{log_state_mod_diff, unrendered_value_eq};
use crate::schemas::serde::typed_struct_from_json_file;
use crate::schemas::{
    InternalDbtNode, Nodes, nodes::DBTTEST_CONFIG_MODIFIERS, nodes::DbtModel, nodes::DbtTest,
    nodes::is_invalid_for_relation_comparison, nodes::same_persisted_description,
};
use dbt_adapter_core::AdapterType;
use dbt_common::string_utils::test_name_from_uid;
use dbt_common::tracing::dbt_emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, constants::DBT_MANIFEST_JSON, fs_err};
use dbt_telemetry::NodeType;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
pub static TEST_SIG_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Controls how a manifest load failure is handled in [`StateArtifacts::try_new_with_target_path`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnManifestLoadFailure {
    /// Propagate as a hard error. Use when `--state` is explicitly provided by the user.
    Error,
    /// Emit a warning and continue with no manifest nodes. Use when state is auto-loaded
    /// and the selector requires `state:modified` / `state:new`.
    Warn,
    /// Silently ignore and continue with no manifest nodes. Use when state is auto-loaded
    /// and the selector does not require the manifest.
    Ignore,
}

#[derive(Debug, Clone)]
pub struct StateArtifacts {
    pub nodes: Option<Nodes>,
    pub run_results: Option<RunResultsArtifact>,
    pub source_freshness_results: Option<FreshnessResultsArtifact>,
    pub state_path: PathBuf,
    pub target_path: Option<PathBuf>,
    /// Pre-built index: test signature → unique_id of the matching previous test.
    /// `None` value means the signature is ambiguous (two or more tests share it).
    test_sig_index: std::collections::HashMap<TestSignature, Option<String>>,
    /// Index of state-manifest test names (3rd unique_id component) → unique_id.
    /// Used to match Mantle-produced manifests where unique_ids use the full untruncated
    /// test name, against Fusion's truncated names after translating via the truncation map.
    test_full_name_index: std::collections::HashMap<String, String>,
    /// Lazily populated map of truncated_test_name → state unique_id.
    /// Set once via `set_test_name_truncations` after the current project is parsed.
    truncated_name_to_state_uid: std::sync::OnceLock<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModificationType {
    Body,
    Configs,
    Relation,
    PersistedDescriptions,
    Macros,
    Contract,
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestSignature {
    name: String,
    namespace: Option<String>,
    attached_node: String,
    column_name: Option<String>,
    /// Sorted, normalized kwargs excluding volatile keys.
    kwargs: Vec<(String, String)>,
}

impl fmt::Display for StateArtifacts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StateArtifacts from {}", self.state_path.display())
    }
}

impl StateArtifacts {
    fn test_signature(test: &DbtTest) -> Option<TestSignature> {
        #[cfg(test)]
        TEST_SIG_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let attached_node = test.__test_attr__.attached_node.clone()?;
        let metadata = test.__test_attr__.test_metadata.as_ref()?;

        let mut kwargs: Vec<(String, String)> = metadata
            .kwargs
            .iter()
            // The `model` kwarg often contains rendered Jinja/ref strings and can vary between engines
            // or manifest producers without indicating a semantic difference in the test.
            .filter(|(k, _)| k.as_str() != "model")
            .map(|(k, v)| {
                let rendered = serde_json::to_string(v).unwrap_or_else(|_| format!("{v:?}"));
                (k.clone(), rendered)
            })
            .collect();
        // Deterministic ordering (even if upstream ever changes map type)
        kwargs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        Some(TestSignature {
            name: metadata.name.clone(),
            namespace: metadata.namespace.clone(),
            attached_node,
            column_name: test.__test_attr__.column_name.clone(),
            kwargs,
        })
    }

    fn build_test_sig_index(
        nodes: &Nodes,
    ) -> std::collections::HashMap<TestSignature, Option<String>> {
        let mut index = std::collections::HashMap::new();
        for (uid, test) in &nodes.tests {
            if let Some(sig) = Self::test_signature(test.as_ref()) {
                index
                    .entry(sig)
                    .and_modify(|v| *v = None) // second occurrence → ambiguous
                    .or_insert_with(|| Some(uid.clone()));
            }
        }
        index
    }

    fn build_test_full_name_index(nodes: &Nodes) -> std::collections::HashMap<String, String> {
        let mut index = std::collections::HashMap::new();
        for uid in nodes.tests.keys() {
            if let Some(name_part) = test_name_from_uid(uid) {
                index.insert(name_part.to_string(), uid.clone());
            }
        }
        index
    }

    /// Seed the truncated-name → state-uid lookup from the current project's
    /// `test_name_truncations` map (built during parsing).  Should be called once
    /// after parsing, before scheduling.
    pub fn set_test_name_truncations(
        &self,
        truncations: &std::collections::HashMap<String, String>,
    ) {
        let mut index = std::collections::HashMap::new();
        for (truncated, full_name) in truncations {
            if let Some(uid) = self.test_full_name_index.get(full_name.as_str()) {
                index.insert(truncated.clone(), uid.clone());
            }
        }
        // OnceLock::set silently no-ops if already set.
        let _ = self.truncated_name_to_state_uid.set(index);
    }

    fn find_previous_test_by_signature<'a>(
        &'a self,
        current: &DbtTest,
        nodes: &'a Nodes,
    ) -> Option<&'a dyn InternalDbtNode> {
        let sig = Self::test_signature(current)?;
        // Look up in the pre-built index; a `None` value means ambiguous.
        let uid = self.test_sig_index.get(&sig)?.as_deref()?;
        nodes
            .tests
            .get(uid)
            .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
    }

    fn find_previous_test_by_truncation_map<'a>(
        &'a self,
        current: &dyn InternalDbtNode,
        nodes: &'a Nodes,
    ) -> Option<&'a dyn InternalDbtNode> {
        let truncation_index = self.truncated_name_to_state_uid.get()?;
        let truncated_name = test_name_from_uid(current.common().unique_id.as_str())?;
        let state_uid = truncation_index.get(truncated_name)?;
        nodes
            .tests
            .get(state_uid.as_str())
            .map(|n| Arc::as_ref(n) as &dyn InternalDbtNode)
    }

    /// Returns true if `node` is a test that exists in the state manifest but was matched
    /// only via the truncation map (Mantle full name ↔ Fusion truncated name).
    /// Such tests are semantically unmodified — the name difference is an artifact.
    fn is_test_matched_only_via_truncation_map(&self, node: &dyn InternalDbtNode) -> bool {
        if node.resource_type() != NodeType::Test {
            return false;
        }
        let Some(nodes) = self.nodes.as_ref() else {
            return false;
        };
        // If found by unique_id, it's a real match — not a truncation map match.
        if nodes.get_node(node.common().unique_id.as_str()).is_some() {
            return false;
        }
        self.find_previous_test_by_truncation_map(node, nodes)
            .is_some()
    }

    /// Strips a trailing `.{10-hex-char}` hash suffix from `test.pkg.name.{hash}` and
    /// looks up the result in the state nodes.  Handles the case where Fusion appends a
    /// hash to singular test UIDs but Mantle does not.
    fn find_previous_test_by_stripping_hash_suffix<'a>(
        &'a self,
        current: &dyn InternalDbtNode,
        nodes: &'a Nodes,
    ) -> Option<&'a dyn InternalDbtNode> {
        let uid = current.common().unique_id.as_str();
        let suffix = uid.rsplit_once('.')?;
        let (base, hash) = suffix;
        // Must be exactly 10 lowercase hex characters.
        if hash.len() != 10 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        nodes.get_node(base).map(|n| n as &dyn InternalDbtNode)
    }

    fn previous_node_for<'a>(
        &'a self,
        current: &dyn InternalDbtNode,
    ) -> Option<&'a dyn InternalDbtNode> {
        let nodes = self.nodes.as_ref()?;

        if let Some(prev) = nodes.get_node(current.common().unique_id.as_str()) {
            return Some(prev as &dyn InternalDbtNode);
        }

        if current.resource_type() == NodeType::Test {
            if let Some(cur_test) = current.as_any().downcast_ref::<DbtTest>() {
                if let Some(found) = self.find_previous_test_by_signature(cur_test, nodes) {
                    return Some(found);
                }
            }
            // Fallback: match via truncation map for Mantle-produced state manifests
            // where unique_ids use the full untruncated test name.
            if let Some(found) = self.find_previous_test_by_truncation_map(current, nodes) {
                return Some(found);
            }
            // Fallback: Fusion appends a `.{10-hex-char}` hash to singular test UIDs
            // (e.g., `test.pkg.my_test.b7f479170d`) while Mantle omits it
            // (`test.pkg.my_test`).  If the direct lookup failed and the UID ends with
            // such a suffix, try again without it.
            if let Some(found) = self.find_previous_test_by_stripping_hash_suffix(current, nodes) {
                return Some(found);
            }
        }

        None
    }

    /// Constructs a minimal `StateArtifacts` for use in tests that only need source freshness data.
    pub fn new_for_source_freshness(
        state_path: PathBuf,
        target_path: Option<PathBuf>,
        source_freshness_results: Option<FreshnessResultsArtifact>,
    ) -> Self {
        Self {
            nodes: None,
            run_results: None,
            source_freshness_results,
            state_path,
            target_path,
            test_sig_index: Default::default(),
            test_full_name_index: Default::default(),
            truncated_name_to_state_uid: Default::default(),
        }
    }

    pub fn try_new(state_path: &Path, root_project_quoting: ResolvedQuoting) -> FsResult<Self> {
        Self::try_new_with_target_path(
            state_path,
            root_project_quoting,
            None,
            OnManifestLoadFailure::Warn,
        )
    }

    /// Creates a new `StateArtifacts` from the given state path.
    ///
    /// # Arguments
    /// * `state_path` - The path to the state directory containing manifest.json and other artifacts
    /// * `root_project_quoting` - The quoting configuration for the root project
    /// * `target_path` - Optional target path for the output directory
    /// * `on_failure` - How to handle a manifest load failure:
    ///   - `Error`: propagate as a hard error (use when `--state` is explicitly provided)
    ///   - `Warn`: emit a warning and continue (use when state is auto-loaded and selector requires manifest)
    ///   - `Ignore`: silently continue (use when state is auto-loaded and selector doesn't require manifest)
    pub fn try_new_with_target_path(
        state_path: &Path,
        root_project_quoting: ResolvedQuoting,
        target_path: Option<PathBuf>,
        on_failure: OnManifestLoadFailure,
    ) -> FsResult<Self> {
        if let Some(ref target) = target_path {
            if state_path == target.as_path() {
                emit_warn_log_message(
                    ErrorCode::WarnStateTargetEqual,
                    format!(
                        "The state and target directories are the same: '{}'. This could lead to missing changes due to overwritten state.",
                        state_path.display()
                    ),
                    None,
                );
            }
        }

        // Try to load manifest.json, but make it optional
        let manifest_path = state_path.join(DBT_MANIFEST_JSON);
        let nodes = match typed_struct_from_json_file::<DbtManifest>(&manifest_path) {
            Ok(manifest) => {
                let dbt_quoting = DbtQuoting {
                    database: Some(root_project_quoting.database),
                    schema: Some(root_project_quoting.schema),
                    identifier: Some(root_project_quoting.identifier),
                    snowflake_ignore_case: None,
                };
                let quoting = if let Some(mut mantle_quoting) = manifest.metadata.quoting {
                    mantle_quoting.default_to(&dbt_quoting);
                    mantle_quoting
                } else {
                    dbt_quoting
                };
                Some(nodes_from_dbt_manifest(manifest, quoting))
            }
            Err(e) => {
                // If the file physically exists but failed to load or parse, that is always
                // a hard error regardless of the caller's policy — a corrupt manifest must
                // never be silently skipped (issue #1319).
                // Only apply the caller's on_failure policy when the file is simply absent.
                if manifest_path.exists() {
                    return Err(fs_err!(
                        ErrorCode::ManifestLoadFailed,
                        "Failed to load manifest.json from state path '{}': {}",
                        state_path.display(),
                        e
                    ));
                }
                match on_failure {
                    OnManifestLoadFailure::Error => {
                        return Err(fs_err!(
                            ErrorCode::ManifestLoadFailed,
                            "Failed to load manifest.json from state path '{}': {}",
                            state_path.display(),
                            e
                        ));
                    }
                    OnManifestLoadFailure::Warn => {
                        emit_warn_log_message(
                            ErrorCode::ManifestLoadFailed,
                            format!(
                                "Failed to load manifest.json from state path '{}': {}",
                                state_path.display(),
                                e
                            ),
                            None,
                        );
                    }
                    OnManifestLoadFailure::Ignore => {}
                }
                None
            }
        };

        let test_sig_index = nodes
            .as_ref()
            .map(Self::build_test_sig_index)
            .unwrap_or_default();
        let test_full_name_index = nodes
            .as_ref()
            .map(Self::build_test_full_name_index)
            .unwrap_or_default();

        Ok(Self {
            nodes,
            run_results: RunResultsArtifact::from_file(&state_path.join("run_results.json")).ok(),
            source_freshness_results: typed_struct_from_json_file(&state_path.join("sources.json"))
                .ok(),
            state_path: state_path.to_path_buf(),
            target_path,
            test_sig_index,
            test_full_name_index,
            truncated_name_to_state_uid: std::sync::OnceLock::new(),
        })
    }

    // Check if a node exists in the previous state
    pub fn exists(&self, node: &dyn InternalDbtNode) -> bool {
        if node.is_never_new_if_previous_missing() {
            true
        } else {
            self.previous_node_for(node).is_some()
        }
    }

    // Check if a node is new (doesn't exist in previous state)
    pub fn is_new(&self, node: &dyn InternalDbtNode) -> bool {
        !self.exists(node)
    }

    // Check if a node has been modified, optionally checking for a specific type of modification
    pub fn is_modified(
        &self,
        node: &dyn InternalDbtNode,
        modification_type: Option<ModificationType>,
        current_nodes: Option<&Nodes>,
        adapter_type: AdapterType,
    ) -> bool {
        // If it's new, it's also considered modified
        if self.is_new(node) {
            log_state_mod_diff(
                &node.common().unique_id,
                node.resource_type().as_static_ref(),
                [("new node", false, None)],
            );
            return true;
        }

        // Tests matched via the truncation map are semantically identical to their state
        // counterpart — the unique_id difference is purely an artifact of Fusion truncating
        // long test names while Mantle preserves them in full. Treat such tests as unmodified.
        if self.is_test_matched_only_via_truncation_map(node) {
            return false;
        }

        match modification_type {
            Some(ModificationType::Body) => self.check_body_modified(node),
            Some(ModificationType::Configs) => self.check_configs_modified(node, adapter_type),
            Some(ModificationType::Relation) => self.check_relation_modified(node),
            Some(ModificationType::PersistedDescriptions) => {
                self.check_persisted_descriptions_modified(node)
            }
            // Macro modification is checked by iteraring through depends_on.macros
            // for each node and checking if the dependent macros are modified.
            Some(ModificationType::Macros) => self.check_modified_macros(node, current_nodes),
            Some(ModificationType::Contract) => self.check_contract_modified(node),
            Some(ModificationType::Any) | None => {
                self.check_contract_modified(node)
                    || self.check_configs_modified(node, adapter_type)
                    || self.check_relation_modified(node)
                    || self.check_persisted_descriptions_modified(node)
                    || self.check_modified_macros(node, current_nodes)
                    || self.check_modified_content(node, adapter_type) // Order is important here, check_modified_content should be last as it is the most generic and could potentially match previous cases
            }
        }
    }

    fn check_modified_macros(
        &self,
        current_node: &dyn InternalDbtNode,
        current_nodes: Option<&Nodes>,
    ) -> bool {
        if let (Some(current_nodes), Some(prev_nodes)) = (current_nodes, self.nodes.as_ref()) {
            for macro_uid in &current_node.base().depends_on.macros {
                let current_macro = current_nodes.macros.get(macro_uid);
                let previous_macro = prev_nodes.macros.get(macro_uid);
                match (current_macro, previous_macro) {
                    (Some(cur), Some(prev)) => {
                        if cur.macro_sql.trim() != prev.macro_sql.trim() {
                            log_state_mod_diff(
                                &current_node.common().unique_id,
                                "macro_dependency",
                                [(
                                    "macro_content_changed",
                                    false,
                                    Some((macro_uid.clone(), macro_uid.clone())),
                                )],
                            );
                            log_state_mod_diff(
                                macro_uid,
                                "macro",
                                [(
                                    "macro_content_changed",
                                    false,
                                    Some((
                                        format!("{:?}", cur.macro_sql),
                                        format!("{:?}", prev.macro_sql),
                                    )),
                                )],
                            );
                            return true;
                        }
                    }
                    (None, Some(_)) | (Some(_), None) => {
                        // TODO: This code path has been intentionally disabled for now
                        // because it is triggered by auto-generated macro calls created
                        // by tests such as not_null as can be seen from the trace output
                        // below where macro.dbt.get_where_subquery is in an
                        // auto-generated macro from a not_null test:
                        // [state_mod_diff] unique_id=test.simplified_client.not_null_cont_bespoke_calendar_effective_date.01fb677460, node_type_or_category=macro_dependency, check=macro_added_or_removed
                        //    self:  "macro.dbt.get_where_subquery"
                        //    other:  "macro.dbt.get_where_subquery"
                        // [state_mod_diff] unique_id=test.simplified_client.unique_cont_bespoke_calendar_effective_date.faaf6305b3, node_type_or_category=macro_dependency, check=macro_added_or_removed
                        //    self:  "macro.dbt.get_where_subquery"
                        //    other:  "macro.dbt.get_where_subquery"
                        //
                        // Even with this branch disabled, the code will work correctly for
                        // most known cases because removal of a macro should also lead
                        // to a code change which the previous branch will detect.
                        // This branch exists for completeness, and can be fully
                        // tightened once we have the time to come up with a solution
                        // that handles auto-generated macro calls.
                        /*
                        log_state_mod_diff(
                            &current_node.common().unique_id,
                            "macro_dependency",
                            [(
                                "macro_added_or_removed",
                                false,
                                Some((macro_uid.clone(), macro_uid.clone())),
                            )],
                        );
                        return true;
                        */
                    }
                    (None, None) => {}
                }
            }
        }
        false
    }

    // Private helper methods to check specific types of modifications
    fn check_modified_content(
        &self,
        current_node: &dyn InternalDbtNode,
        adapter_type: AdapterType,
    ) -> bool {
        // Get the previous node from the manifest (unique_id first, then test signature fallback).
        let Some(previous_node) = self.previous_node_for(current_node) else {
            // If previous node doesn't exist, consider it modified.
            return true;
        };

        // For models, treat "modified content" as a *body* comparison (checksum/raw_code),
        // not a full same_contents comparison. Config/relation/persisted-description diffs
        // are handled by dedicated checks in `state:modified` selection.
        if current_node.resource_type() == NodeType::Model
            && previous_node.resource_type() == NodeType::Model
        {
            // Fast path: identical checksums => body is unchanged.
            if current_node.common().checksum == previous_node.common().checksum {
                return false;
            }
        }

        if current_node.has_same_content(previous_node, adapter_type) {
            return false;
        }

        true
    }

    fn check_configs_modified(
        &self,
        current_node: &dyn InternalDbtNode,
        adapter_type: AdapterType,
    ) -> bool {
        // Get the previous node from the manifest (unique_id first, then test signature fallback).
        let Some(previous_node) = self.previous_node_for(current_node) else {
            // If previous node doesn't exist, consider it modified.
            return true;
        };

        let rt = current_node.resource_type(); // also the type of previous_node

        match rt {
            // Unit tests are Structural/checksum, not a config-comparison type: dbt-core's
            // UnitTestDefinition extends GraphNode (no same_config/same_body) and its same_contents
            // is a checksum over model/given/expect/overrides only (dbt-mantle
            // core/dbt/contracts/graph/nodes.py:1237). Config is never a state:modified trigger for
            // unit tests, so return "not config-modified" and never consult has_same_config. (A
            // *new* unit test is still selected via node presence / check_modified_content, not
            // config.)
            NodeType::UnitTest => false,

            // Approach A — full `unrendered_config` comparison (models, sources, seeds, snapshots,
            // data tests, and functions).
            //
            // For these node types, `unrendered_config` is populated with every authored key, so
            // comparing the raw Jinja strings is an authoritative authoring-intent check: identical
            // strings across targets means the author changed nothing, even if the rendered values
            // differ per target (e.g. `"{{ 'table' if target.name == 'prod' else 'view' }}"`).
            NodeType::Model
            | NodeType::Source
            | NodeType::Seed
            | NodeType::Snapshot
            | NodeType::Test
            | NodeType::Function => {
                // Stage 1: if every `unrendered_config` key that is relevant for the node type (see
                // `UnrenderedKeyRelevance`) is equal, the node is not config-modified — return early
                // without touching rendered values.
                if unrendered_configs_eq(
                    rt,
                    &previous_node.base().unrendered_config,
                    &current_node.base().unrendered_config,
                    &current_node.common().unique_id,
                ) {
                    return false;
                }
                // Stage 2: if Stage 1 finds a difference, fall through to `has_same_config`
                // (rendered comparison). This preserves backward-compatibility when the state
                // manifest was produced by Mantle or an older Fusion with a sparse
                // `unrendered_config`: Stage 1 would see keys only on the current side and report
                // a spurious diff; Stage 2 resolves it via rendered values, matching the
                // pre-existing behavior.
                !current_node.has_same_config(previous_node, adapter_type)
            }

            // Approach B — surgical per-key unrendered comparisons (remaining node kinds:
            // exposures, analyses, macros, semantic models, metrics, and saved queries).
            //
            // For these node types, `unrendered_config` may be incomplete, so the full-
            // `unrendered_config` shortcut above is unsound. Instead, each node type's
            // `has_same_config` implementation contains targeted unrendered comparisons for the
            // specific keys where env-aware Jinja is known to appear. A new false positive for
            // those types requires a new per-key fix; the wholesale approach cannot yet be applied
            // to them.
            NodeType::Exposure
            | NodeType::Analysis
            | NodeType::Macro
            | NodeType::SemanticModel
            | NodeType::Metric
            | NodeType::SavedQuery => !current_node.has_same_config(previous_node, adapter_type),

            // Never returned by any `InternalDbtNode::resource_type()` impl (see nodes.rs) —
            // `DocsMacro`/`Operation` describe non-node telemetry concepts and `Unspecified` is a
            // protobuf default. Listed only for match exhaustiveness; treated like Approach B if
            // ever reached.
            NodeType::DocsMacro | NodeType::Operation | NodeType::Unspecified => {
                !current_node.has_same_config(previous_node, adapter_type)
            }
        }
    }

    fn check_relation_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        if is_invalid_for_relation_comparison(current_node) {
            return false;
        }

        // Get the previous node from the manifest (unique_id first, then test signature fallback).
        let Some(previous_node) = self.previous_node_for(current_node) else {
            // If previous node doesn't exist, consider it modified.
            return true;
        };

        // Check if database representation changed (database, schema, alias).
        //
        // Prefer comparing unrendered (configured) values, matching dbt-core semantics for
        // state selection: differences that come purely from target rendering should not
        // count as modifications.
        let current_uc = &current_node.base().unrendered_config;
        let previous_uc = &previous_node.base().unrendered_config;

        fn get<'a>(
            m: &'a std::collections::BTreeMap<String, dbt_yaml::Value>,
            k: &str,
        ) -> Option<&'a str> {
            m.get(k).and_then(|v| v.as_str())
        }

        #[allow(clippy::too_many_arguments)]
        fn log_relation_modified(
            current_node: &dyn InternalDbtNode,
            db_eq: bool,
            schema_eq: bool,
            alias_eq: bool,
            current_db: String,
            previous_db: String,
            current_schema: String,
            previous_schema: String,
            current_alias: String,
            previous_alias: String,
        ) {
            log_state_mod_diff(
                &current_node.common().unique_id,
                "relation",
                [
                    ("database", db_eq, Some((current_db, previous_db))),
                    ("schema", schema_eq, Some((current_schema, previous_schema))),
                    ("alias", alias_eq, Some((current_alias, previous_alias))),
                ],
            );
        }

        // Sources are a special case: some manifest producers omit relation keys from
        // `unrendered_config` even though the rendered/database representation is stable.
        // If we treat `Some(...)` vs `None` as a diff here, `state:modified+` can end up selecting
        // large parts of the graph from a source-only representation mismatch.
        //
        // Match dbt-core semantics by only comparing unrendered relation keys when both manifests
        // include them; otherwise compare the rendered/base representation.
        if let (Some(current_source), Some(previous_source)) = (
            current_node.as_any().downcast_ref::<DbtSource>(),
            previous_node.as_any().downcast_ref::<DbtSource>(),
        ) {
            // dbt-core might also produce `unrendered_database` and `unrendered_schema` outside of the unrendered config.
            // If so, we need to compare them and use unrendered keys.
            if (current_source.__source_attr__.unrendered_database.is_some()
                && previous_source
                    .__source_attr__
                    .unrendered_database
                    .is_some())
                && (current_source.__source_attr__.unrendered_schema.is_some()
                    && previous_source.__source_attr__.unrendered_schema.is_some())
            {
                let db_eq = current_source.__source_attr__.unrendered_database
                    == previous_source.__source_attr__.unrendered_database;
                let schema_eq = current_source.__source_attr__.unrendered_schema
                    == previous_source.__source_attr__.unrendered_schema;
                let alias_eq = get(current_uc, "alias") == get(previous_uc, "alias");
                let is_same_relation = db_eq && schema_eq && alias_eq;

                if !is_same_relation {
                    log_relation_modified(
                        current_node,
                        db_eq,
                        schema_eq,
                        alias_eq,
                        format!("{:?}", &current_node.base().database),
                        format!("{:?}", &previous_node.base().database),
                        format!("{:?}", &current_node.base().schema),
                        format!("{:?}", &previous_node.base().schema),
                        format!("{:?}", &current_node.base().alias),
                        format!("{:?}", &previous_node.base().alias),
                    );
                }

                return !is_same_relation;
            }

            let uc_has_both = ["database", "schema", "alias"]
                .iter()
                .any(|k| current_uc.contains_key(*k) && previous_uc.contains_key(*k));

            if !uc_has_both {
                let db_eq = current_node.base().database == previous_node.base().database;
                let schema_eq = current_node.base().schema == previous_node.base().schema;
                let alias_eq = current_node.base().alias == previous_node.base().alias;
                let is_same_relation = db_eq && schema_eq && alias_eq;

                if !is_same_relation {
                    log_relation_modified(
                        current_node,
                        db_eq,
                        schema_eq,
                        alias_eq,
                        format!("{:?}", &current_node.base().database),
                        format!("{:?}", &previous_node.base().database),
                        format!("{:?}", &current_node.base().schema),
                        format!("{:?}", &previous_node.base().schema),
                        format!("{:?}", &current_node.base().alias),
                        format!("{:?}", &previous_node.base().alias),
                    );
                }

                return !is_same_relation;
            }
        }

        // Match dbt-core / Mantle semantics: compare only the configured representation
        // (unrendered_config), not the rendered values derived from the target (e.g.
        // generate_*_name macros).
        //
        // Missing keys compare as `None`, which intentionally ignores target-only differences.
        let db_eq = get(current_uc, "database") == get(previous_uc, "database");
        let schema_eq = get(current_uc, "schema") == get(previous_uc, "schema");
        let alias_eq = get(current_uc, "alias") == get(previous_uc, "alias");
        let is_same_relation = db_eq && schema_eq && alias_eq;

        if !is_same_relation {
            log_relation_modified(
                current_node,
                db_eq,
                schema_eq,
                alias_eq,
                format!("{:?}", get(current_uc, "database")),
                format!("{:?}", get(previous_uc, "database")),
                format!("{:?}", get(current_uc, "schema")),
                format!("{:?}", get(previous_uc, "schema")),
                format!("{:?}", get(current_uc, "alias")),
                format!("{:?}", get(previous_uc, "alias")),
            );
        }

        !is_same_relation
    }

    fn check_persisted_descriptions_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest (unique_id first, then test signature fallback).
        let Some(previous_node) = self.previous_node_for(current_node) else {
            // If previous node doesn't exist, consider it modified.
            return true;
        };

        !same_persisted_description(
            current_node.common(),
            current_node.base(),
            previous_node.common(),
            previous_node.base(),
        )
    }

    fn check_contract_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest (unique_id first, then test signature fallback).
        let Some(previous_node) = self.previous_node_for(current_node) else {
            // If previous node doesn't exist, consider it modified.
            return true;
        };

        if let (Some(current_model), Some(previous_model)) = (
            current_node.as_any().downcast_ref::<DbtModel>(),
            previous_node.as_any().downcast_ref::<DbtModel>(),
        ) {
            let is_same_contract = current_model.same_contract(previous_model);
            if !is_same_contract {
                log_state_mod_diff(
                    &current_node.common().unique_id,
                    "contract",
                    [("contract", false, None)],
                );
            }
            !is_same_contract
        } else {
            false
        }
    }

    fn check_body_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest (unique_id first, then test signature fallback).
        let Some(previous_node) = self.previous_node_for(current_node) else {
            // If previous node doesn't exist, consider it modified.
            return true;
        };

        let same_body = current_node.has_same_body(previous_node);

        if !same_body {
            log_state_mod_diff(
                &current_node.common().unique_id,
                "body",
                [("body", false, None)],
            );
        }

        !same_body
    }
}

/// Do previous and current `unrendered_config`s have the same authoring intent?
///
/// Roughly, the configs must have the same pre-rendering Jinja contents, but certain variations
/// are considered insignificant: key spelling (`pre_hook`/`pre-hook`; see [`canonicalize_hook_keys`])
/// and whitespace/emptiness (see [`unrendered_value_eq`]).
/// Which keys are compared at all depends on the node type's [`UnrenderedKeyRelevance`].
fn unrendered_configs_eq(
    node_type: NodeType,
    previous_uc: &std::collections::BTreeMap<String, dbt_yaml::Value>,
    current_uc: &std::collections::BTreeMap<String, dbt_yaml::Value>,
    unique_id: &str,
) -> bool {
    let relevance = UnrenderedKeyRelevance::for_node_type(node_type);

    let previous = canonicalize_hook_keys(previous_uc);
    let current = canonicalize_hook_keys(current_uc);

    let all_keys: std::collections::BTreeSet<&str> = previous
        .keys()
        .chain(current.keys())
        .map(String::as_str)
        .collect();

    let mut all_eq = true;
    for key in all_keys {
        if !relevance.key_is_relevant(key) {
            continue;
        }
        let a = previous.get(key);
        let b = current.get(key);
        if !unrendered_value_eq(a, b) {
            all_eq = false;
            log_state_mod_diff(
                unique_id,
                "unrendered_config",
                [(
                    "value",
                    false,
                    Some((format!("{key}: {:?}", a), format!("{key}: {:?}", b))),
                )],
            );
        }
    }
    all_eq
}

/// Which `unrendered_config` keys are *relevant* to the config-modified comparison for a given node
/// type. It encodes the two dbt-core comparison methods as one of two explicit rules:
/// - `Denylist` (models, seeds, snapshots, sources) mirrors dbt-core's `BaseConfig.same_contents`,
///   which treats a node as config-modified iff any config key present on either side differs,
///   EXCEPT keys whose config-class field is marked `CompareBehavior.Exclude`.
///   (<https://github.com/dbt-labs/dbt-common/blob/main/dbt_common/contracts/config/base.py>)
/// - `Allowlist` (data tests) corresponds to dbt-core's `TestConfig.same_contents`, a bespoke
///   override that ignores `CompareBehavior` and treats ONLY a fixed set of modifier keys as
///   relevant.
enum UnrenderedKeyRelevance {
    ///  Every key present on either side is relevant EXCEPT these. (Core's `BaseConfig.same_contents`)
    Denylist(&'static [&'static str]),
    /// ONLY these keys are relevant; everything else is ignored. (Core's `TestConfig.same_contents`)
    Allowlist(&'static [&'static str]),
}

impl UnrenderedKeyRelevance {
    fn for_node_type(node_type: NodeType) -> Self {
        match node_type {
            NodeType::Test => Self::Allowlist(DBTTEST_CONFIG_MODIFIERS),
            _ => Self::Denylist(Self::base_config_excluded_keys(node_type)),
        }
    }

    /// Is `key` relevant to the comparison under this rule?
    fn key_is_relevant(&self, key: &str) -> bool {
        match self {
            UnrenderedKeyRelevance::Denylist(excluded) => !excluded.contains(&key),
            UnrenderedKeyRelevance::Allowlist(allowed) => allowed.contains(&key),
        }
    }

    /// Config keys that must NOT count as a config modification under a `Denylist`
    /// (models/seeds/snapshots/sources) — the per-type counterpart of dbt-core's
    /// `CompareBehavior.Exclude` metadata.
    ///
    /// A key is excluded for one of two reasons:
    /// - *Parity-exclude*: dbt-core does not treat the key as a config modification at all, and
    ///   checks it nowhere else — so neither do we.
    /// - *Ownership-exclude*: the key IS a modification, but in Fusion's decomposition it is owned
    ///   by a sibling sub-check (relation identity → `check_relation_modified`, mirroring dbt-core's
    ///   separate `same_database_representation`). Excluding it here only avoids double-counting;
    ///   the correctness claim is that the *union* of `is_modified`'s sub-checks equals dbt-core's
    ///   comparison.
    fn base_config_excluded_keys(node_type: NodeType) -> &'static [&'static str] {
        match node_type {
            // Models, seeds, snapshots, and functions use dbt-core's `BaseConfig.same_contents`,
            // whose set of non-modification keys is exactly the `CompareBehavior.Exclude` fields of
            // `NodeAndTestConfig` — the five below (core/dbt/artifacts/resources/v1/config.py @
            // v1.10.0).
            NodeType::Model | NodeType::Seed | NodeType::Snapshot | NodeType::Function => {
                &[
                    "tags", "group", // parity-excludes
                    "schema", "database",
                    "alias", // ownership-excludes, counted in `check_relation_modified`
                ]
            }
            // Sources: dbt-core's `SourceConfig` marks nothing `Exclude`, but what counts as a
            // source change is defined by `SourceDefinition.same_contents`
            // (core/dbt/contracts/graph/nodes.py @ v1.10.0), which deliberately ignores tags
            // ("metadata/tags changes are not changes") and compares relation identity separately
            // via `same_database_representation`.
            NodeType::Source => {
                &[
                    "tags", // parity-exclude
                    "schema", "database",
                    "alias", // ownership-excludes, counted in `check_relation_modified`
                ]
            }
            // Other node types do not yet take the full-`unrendered_config` path (see
            // `check_configs_modified`); exclude nothing rather than guess.
            _ => &[],
        }
    }
}

/// Normalise hook key aliases so `pre_hook` and `pre-hook` compare as equal;
/// they have been long-term aliases in dbt.
fn canonicalize_hook_keys(
    map: &std::collections::BTreeMap<String, dbt_yaml::Value>,
) -> std::collections::BTreeMap<String, dbt_yaml::Value> {
    map.iter()
        .map(|(k, v)| {
            let k = match k.as_str() {
                "pre_hook" => "pre-hook".to_string(),
                "post_hook" => "post-hook".to_string(),
                _ => k.clone(),
            };
            (k, v.clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::nodes::{DbtTestAttr, Nodes, TestMetadata};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    fn make_test(uid: &str, attached_node: &str, test_name: &str) -> DbtTest {
        let mut t = DbtTest::default();
        t.__common_attr__.unique_id = uid.to_string();
        t.__test_attr__ = DbtTestAttr {
            attached_node: Some(attached_node.to_string()),
            test_metadata: Some(TestMetadata {
                name: test_name.to_string(),
                kwargs: BTreeMap::default(),
                namespace: None,
            }),
            ..DbtTestAttr::default()
        };
        t
    }

    /// Regression test: `test_signature` must be called O(N) times, not O(N²).
    ///
    /// Before the fix, `find_previous_test_by_signature` recomputes `test_signature`
    /// for every previous test on every current-test lookup, giving N*N calls.
    /// After the fix (pre-built index), total calls should be proportional to N.
    #[test]
    fn test_signature_calls_are_linear_not_quadratic() {
        const N: usize = 200;

        // Previous state: N tests whose unique_ids will NOT match the current tests,
        // forcing every lookup to fall through to `find_previous_test_by_signature`.
        let mut prev_nodes = Nodes::default();
        for i in 0..N {
            let uid = format!("test.pkg.prev_{i}");
            let t = make_test(&uid, &format!("model.pkg.m{i}"), "not_null");
            prev_nodes.tests.insert(uid, Arc::new(t));
        }

        // Current tests: different unique_ids but identical signatures to the prev tests.
        let current_tests: Vec<DbtTest> = (0..N)
            .map(|i| {
                make_test(
                    &format!("test.pkg.curr_{i}"),
                    &format!("model.pkg.m{i}"),
                    "not_null",
                )
            })
            .collect();

        let test_sig_index = StateArtifacts::build_test_sig_index(&prev_nodes);
        let test_full_name_index = StateArtifacts::build_test_full_name_index(&prev_nodes);
        let state = StateArtifacts {
            nodes: Some(prev_nodes),
            run_results: None,
            source_freshness_results: None,
            state_path: PathBuf::from("/tmp/fake_state"),
            target_path: None,
            test_sig_index,
            test_full_name_index,
            truncated_name_to_state_uid: std::sync::OnceLock::new(),
        };

        TEST_SIG_CALLS.store(0, Ordering::SeqCst);
        for test in &current_tests {
            state.is_new(test);
        }
        let calls = TEST_SIG_CALLS.load(Ordering::SeqCst);

        // Linear bound: O(N) calls expected (e.g. N for index build + N for lookups).
        // Quadratic would give N*N = 40_000 calls.
        assert!(
            calls <= 3 * N,
            "test_signature called {calls} times for N={N} tests; \
             expected O(N) ≤ {} but got O(N²) behavior",
            3 * N,
        );
    }

    /// How a single `base_config_excluded_keys` key is expected to relate to its type's
    /// Stage 2 (`has_same_config`) comparator — see `base_config_excluded_keys`'s own doc comment
    /// for the parity/ownership distinction.
    #[derive(Debug, Clone, Copy)]
    enum ExcludeKind {
        /// Not excluded at Stage 1: Stage 2 MUST also observe a change on this key, otherwise a
        /// genuine change is silently invisible on the sparse/fallback path (false negative).
        Relevant,
        /// dbt-core checks this key nowhere: Stage 2 MUST NOT observe it either, otherwise a
        /// benign/env-aware change is wrongly selected on the fallback path (false positive).
        Parity,
        /// A real dbt-core modification trigger, but owned by the sibling `check_relation_modified`
        /// sub-check. Stage 2 may harmlessly compare it too (redundant, not wrong) or not — either
        /// is fine, so there is no Stage1-vs-Stage2 assertion for it.
        Ownership,
    }

    /// Shared body for the five Denylist-type drift-guard tests below: for every key `Stage 1`
    /// (`base_config_excluded_keys`) treats as excluded, and for every key case supplied, assert
    /// that Stage 1's classification agrees with what `T`'s actual rendered `has_same_config`
    /// does. Generalizes `data_test_modifier_set_agrees_across_stage1_and_stage2` (nodes.rs) to
    /// the Denylist rule (see the `state-modified-config-drift-guard` plan).
    #[allow(clippy::type_complexity)]
    fn assert_denylist_keys_agree_across_stage1_and_stage2<T>(
        node_type: NodeType,
        comparator_name: &str,
        cases: &[(&str, ExcludeKind, Box<dyn Fn(&mut T)>)],
    ) where
        T: InternalDbtNode + Clone + Default,
    {
        let excluded = UnrenderedKeyRelevance::base_config_excluded_keys(node_type);

        // Coverage: every excluded key must be exercised, else a modifier could drift untested.
        for key in excluded {
            assert!(
                cases.iter().any(|(k, _, _)| k == key),
                "{node_type:?}: base_config_excluded_keys key `{key}` is not exercised by this \
                 test; add a case."
            );
        }

        let base = T::default();
        for (key, kind, mutate) in cases {
            let mut mutated = base.clone();
            mutate(&mut mutated);

            // `has_same_config` returns false when Stage 2 considers the config changed.
            let stage2_sensitive =
                !base.has_same_config(&mutated as &dyn InternalDbtNode, AdapterType::DuckDB);
            let stage1_relevant = !excluded.contains(key);

            match kind {
                ExcludeKind::Relevant => assert!(
                    stage1_relevant && stage2_sensitive,
                    "{node_type:?} config key `{key}` disagrees between the two \
                     `state:modified` config comparisons: Stage 1 \
                     (`base_config_excluded_keys`, unrendered) says relevant={stage1_relevant}, \
                     but Stage 2 (`{comparator_name}`, rendered) says \
                     sensitive={stage2_sensitive}. A genuine change on `{key}` would otherwise be \
                     silently invisible on the sparse/fallback path. To reconcile: make sure \
                     `{key}` is compared in `{comparator_name}` AND is absent from \
                     `base_config_excluded_keys(NodeType::{node_type:?})` \
                     (prev_state/mod.rs)."
                ),
                ExcludeKind::Parity => assert!(
                    !stage1_relevant && !stage2_sensitive,
                    "{node_type:?} config key `{key}` is a dbt-core parity-exclude (dbt-core \
                     checks it nowhere), but Stage 2 (`{comparator_name}`, rendered) still \
                     observes it (sensitive={stage2_sensitive}). This over-selects on the \
                     sparse/fallback path (the dbt-core-mantle#15286 class of bug). To \
                     reconcile: stop comparing `{key}` in `{comparator_name}`."
                ),
                ExcludeKind::Ownership => assert!(
                    !stage1_relevant,
                    "{node_type:?}: ownership-exclude key `{key}` is unexpectedly \
                     Stage-1-relevant; it should be listed in \
                     `base_config_excluded_keys(NodeType::{node_type:?})` (prev_state/mod.rs)."
                ),
            }
        }

        // Once per type (not per key): confirm the sibling sub-check that's supposed to own the
        // ownership-excludes (schema/database/alias) actually runs for this node type.
        assert!(
            !is_invalid_for_relation_comparison(&base as &dyn InternalDbtNode),
            "{node_type:?}: `check_relation_modified` does not run for this node type (see \
             `is_invalid_for_relation_comparison`), so nothing checks its ownership-exclude keys \
             at all."
        );
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn source_config_keys_agree_across_stage1_and_stage2() {
        use crate::schemas::serde::StringOrArrayOfStrings;
        use dbt_common::io_args::StaticAnalysisKind;
        use dbt_yaml::Spanned;

        let cases: Vec<(&str, ExcludeKind, Box<dyn Fn(&mut DbtSource)>)> = vec![
            // --- fields DbtSource::has_same_config actually compares ---
            (
                "enabled",
                ExcludeKind::Relevant,
                Box::new(|n| n.__base_attr__.enabled = true),
            ),
            (
                "event_time",
                ExcludeKind::Relevant,
                Box::new(|n: &mut DbtSource| {
                    n.deprecated_config.event_time = Some("updated_at".to_string())
                }),
            ),
            (
                "quoting",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.__source_attr__.user_quoting = Some(DbtQuoting {
                        database: None,
                        schema: None,
                        identifier: Some(false),
                        snowflake_ignore_case: None,
                    })
                }),
            ),
            (
                "loaded_at_field",
                ExcludeKind::Relevant,
                Box::new(|n| n.__source_attr__.loaded_at_field = Some("loaded_at".to_string())),
            ),
            (
                "loaded_at_query",
                ExcludeKind::Relevant,
                Box::new(|n| n.__source_attr__.loaded_at_query = Some("select 1".to_string())),
            ),
            (
                "static_analysis",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.static_analysis =
                        Some(Spanned::new(StaticAnalysisKind::Off))
                }),
            ),
            // --- parity-exclude: dbt-core's `SourceDefinition.same_contents` ignores tags ---
            (
                "tags",
                ExcludeKind::Parity,
                Box::new(|n| {
                    n.deprecated_config.tags =
                        Some(StringOrArrayOfStrings::String("a_tag".to_string()))
                }),
            ),
            // --- ownership-excludes: owned by `check_relation_modified`, not this comparator ---
            (
                "schema",
                ExcludeKind::Ownership,
                Box::new(|n| n.__base_attr__.schema = "a_schema".to_string()),
            ),
            (
                "database",
                ExcludeKind::Ownership,
                Box::new(|n| n.__base_attr__.database = "a_db".to_string()),
            ),
            (
                "alias",
                ExcludeKind::Ownership,
                Box::new(|n| n.__base_attr__.alias = "an_alias".to_string()),
            ),
        ];

        // Not exercised above: `freshness` is compared via `same_freshness_value`, a deliberately
        // separate axis outside `UnrenderedKeyRelevance` (see `has_same_content`'s doc comment on
        // rendered freshness) — out of scope for this drift guard. `warehouse_config` is nested
        // adapter-specific config with its own dedicated test coverage.
        assert_denylist_keys_agree_across_stage1_and_stage2::<DbtSource>(
            NodeType::Source,
            "DbtSource::has_same_config",
            &cases,
        );
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn seed_config_keys_agree_across_stage1_and_stage2() {
        use crate::schemas::common::{DbtMaterialization, DocsConfig, Hooks};
        use crate::schemas::nodes::DbtSeed;
        use crate::schemas::serde::{GrantConfig, OmissibleGrantConfig, StringOrArrayOfStrings};
        use dbt_common::serde_utils::Omissible;
        use dbt_yaml::{Spanned, Verbatim};

        let cases: Vec<(&str, ExcludeKind, Box<dyn Fn(&mut DbtSeed)>)> = vec![
            // --- fields `seed_configs_equal` actually compares ---
            (
                "column_types",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.column_types = Some(BTreeMap::from([(
                        Spanned::new("id".to_string()),
                        "int".to_string(),
                    )]))
                }),
            ),
            (
                "docs",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.docs = Some(DocsConfig {
                        show: false,
                        node_color: None,
                    })
                }),
            ),
            (
                "enabled",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.enabled = Some(false)),
            ),
            (
                "grants",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.grants = OmissibleGrantConfig(Omissible::Present(
                        GrantConfig(indexmap::IndexMap::from([(
                            "select".to_string(),
                            StringOrArrayOfStrings::String("role1".to_string()),
                        )])),
                    ))
                }),
            ),
            (
                "quote_columns",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.quote_columns = Some(true)),
            ),
            (
                "event_time",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.event_time = Some("updated_at".to_string())),
            ),
            (
                "full_refresh",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.full_refresh = Some(true)),
            ),
            (
                "meta",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    let mut m = indexmap::IndexMap::new();
                    m.insert("owner".to_string(), dbt_yaml::to_value("bob").unwrap());
                    n.deprecated_config.meta = Some(m);
                }),
            ),
            (
                "persist_docs",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.persist_docs =
                        Some(crate::schemas::common::PersistDocsConfig {
                            columns: Some(true),
                            relation: Some(true),
                        })
                }),
            ),
            (
                "post_hook",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.post_hook =
                        Verbatim::from(Some(Hooks::ArrayOfStrings(vec!["select 1".to_string()])))
                }),
            ),
            (
                "pre_hook",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.pre_hook =
                        Verbatim::from(Some(Hooks::ArrayOfStrings(vec!["select 1".to_string()])))
                }),
            ),
            (
                "materialized",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.materialized = Some(DbtMaterialization::View)),
            ),
            // --- parity-excludes: dbt-core's `CompareBehavior.Exclude` on `NodeAndTestConfig` ---
            (
                "tags",
                ExcludeKind::Parity,
                Box::new(|n| {
                    n.deprecated_config.tags =
                        Some(StringOrArrayOfStrings::String("a_tag".to_string()))
                }),
            ),
            (
                "group",
                ExcludeKind::Parity,
                Box::new(|n| n.deprecated_config.group = Some("a_group".to_string())),
            ),
            // --- ownership-excludes: owned by `check_relation_modified`, not this comparator ---
            (
                "schema",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.schema = Some("a_schema".to_string())),
            ),
            (
                "database",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.database = Some("a_db".to_string())),
            ),
            (
                "alias",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.alias = Some("an_alias".to_string())),
            ),
        ];

        // Not exercised above: `warehouse_config` is nested adapter-specific config with its own
        // dedicated test coverage.
        assert_denylist_keys_agree_across_stage1_and_stage2::<DbtSeed>(
            NodeType::Seed,
            "seed_configs_equal (DbtSeed::has_same_config)",
            &cases,
        );
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn function_config_keys_agree_across_stage1_and_stage2() {
        use crate::schemas::common::{Access, DbtQuoting, DocsConfig};
        use crate::schemas::nodes::DbtFunction;
        use crate::schemas::project::configs::function_config::FunctionSnowflakeConfig;
        use crate::schemas::properties::{FunctionKind, Volatility};
        use crate::schemas::serde::{GrantConfig, OmissibleGrantConfig, StringOrArrayOfStrings};
        use dbt_common::io_args::StaticAnalysisKind;
        use dbt_common::serde_utils::Omissible;
        use dbt_yaml::Spanned;

        let cases: Vec<(&str, ExcludeKind, Box<dyn Fn(&mut DbtFunction)>)> = vec![
            // --- fields `FunctionConfig::same_config` actually compares ---
            (
                "access",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.access = Some(Access::Public)),
            ),
            (
                "enabled",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.enabled = Some(false)),
            ),
            (
                "meta",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    let mut m = indexmap::IndexMap::new();
                    m.insert("owner".to_string(), dbt_yaml::to_value("bob").unwrap());
                    n.deprecated_config.meta = Some(m);
                }),
            ),
            (
                "docs",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.docs = Some(DocsConfig {
                        show: false,
                        node_color: None,
                    })
                }),
            ),
            (
                "grants",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.grants = OmissibleGrantConfig(Omissible::Present(
                        GrantConfig(indexmap::IndexMap::from([(
                            "select".to_string(),
                            StringOrArrayOfStrings::String("role1".to_string()),
                        )])),
                    ))
                }),
            ),
            (
                "quoting",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.quoting = Some(DbtQuoting::default())),
            ),
            (
                "on_configuration_change",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.on_configuration_change = Some("skip".to_string())
                }),
            ),
            (
                "static_analysis",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.static_analysis =
                        Some(Spanned::new(StaticAnalysisKind::Off))
                }),
            ),
            (
                "function_kind",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.function_kind = Some(FunctionKind::Aggregate)),
            ),
            (
                "volatility",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.volatility = Some(Volatility::Deterministic)),
            ),
            (
                "packages",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.packages =
                        Some(StringOrArrayOfStrings::String("pkg1".to_string()))
                }),
            ),
            (
                "snowflake",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.snowflake = Some(FunctionSnowflakeConfig {
                        quote_args: Some(true),
                    })
                }),
            ),
            // --- parity-excludes: dbt-core's `CompareBehavior.Exclude` on `NodeAndTestConfig` ---
            (
                "tags",
                ExcludeKind::Parity,
                Box::new(|n| {
                    n.deprecated_config.tags =
                        Some(StringOrArrayOfStrings::String("a_tag".to_string()))
                }),
            ),
            (
                "group",
                ExcludeKind::Parity,
                Box::new(|n| n.deprecated_config.group = Some("a_group".to_string())),
            ),
            // --- ownership-excludes: owned by `check_relation_modified`, not this comparator ---
            (
                "schema",
                ExcludeKind::Ownership,
                Box::new(|n| {
                    n.deprecated_config.schema = Omissible::Present(Some("a_schema".to_string()))
                }),
            ),
            (
                "database",
                ExcludeKind::Ownership,
                Box::new(|n| {
                    n.deprecated_config.database = Omissible::Present(Some("a_db".to_string()))
                }),
            ),
            (
                "alias",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.alias = Some("an_alias".to_string())),
            ),
        ];

        // Not exercised above: `runtime_version`/`entry_point` are not compared by
        // `FunctionConfig::same_config` at all (outside this drift guard's scope — see the plan's
        // non-goals on completeness beyond the existing comparator). `warehouse_config` is nested
        // adapter-specific config with its own dedicated test coverage.
        assert_denylist_keys_agree_across_stage1_and_stage2::<DbtFunction>(
            NodeType::Function,
            "FunctionConfig::same_config",
            &cases,
        );
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn snapshot_config_keys_agree_across_stage1_and_stage2() {
        use crate::schemas::common::{
            DbtMaterialization, DbtQuoting, HardDeletes, Hooks, PersistDocsConfig,
        };
        use crate::schemas::nodes::DbtSnapshot;
        use crate::schemas::project::configs::snapshot_config::SnapshotMetaColumnNames;
        use crate::schemas::serde::{GrantConfig, OmissibleGrantConfig, StringOrArrayOfStrings};
        use dbt_common::io_args::StaticAnalysisKind;
        use dbt_common::serde_utils::Omissible;
        use dbt_yaml::{Spanned, Verbatim};

        let cases: Vec<(&str, ExcludeKind, Box<dyn Fn(&mut DbtSnapshot)>)> = vec![
            // --- fields `DbtSnapshot::has_same_config` actually compares ---
            (
                "materialized",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.materialized = Some(DbtMaterialization::View)),
            ),
            (
                "strategy",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.strategy = Some("timestamp".to_string())),
            ),
            (
                "unique_key",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.unique_key =
                        Some(StringOrArrayOfStrings::String("id".to_string()))
                }),
            ),
            (
                "check_cols",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.check_cols =
                        Some(StringOrArrayOfStrings::String("all".to_string()))
                }),
            ),
            (
                "updated_at",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.updated_at = Some("updated_at".to_string())),
            ),
            (
                "dbt_valid_to_current",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.dbt_valid_to_current = Some("NULL".to_string())),
            ),
            (
                "snapshot_meta_column_names",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.snapshot_meta_column_names = Some(SnapshotMetaColumnNames {
                        dbt_scd_id: Some("scd_id".to_string()),
                        ..Default::default()
                    })
                }),
            ),
            (
                "hard_deletes",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.hard_deletes = Some(HardDeletes::Invalidate)),
            ),
            (
                "target_database",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.target_database = Some("a_db".to_string())),
            ),
            (
                "target_schema",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.target_schema = Some("a_schema".to_string())),
            ),
            (
                "enabled",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.enabled = Some(false)),
            ),
            (
                "pre_hook",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.pre_hook =
                        Verbatim::from(Some(Hooks::ArrayOfStrings(vec!["select 1".to_string()])))
                }),
            ),
            (
                "post_hook",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.post_hook =
                        Verbatim::from(Some(Hooks::ArrayOfStrings(vec!["select 1".to_string()])))
                }),
            ),
            (
                "persist_docs",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.persist_docs = Some(PersistDocsConfig {
                        columns: Some(true),
                        relation: Some(true),
                    })
                }),
            ),
            (
                "meta",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    let mut m = indexmap::IndexMap::new();
                    m.insert("owner".to_string(), dbt_yaml::to_value("bob").unwrap());
                    n.deprecated_config.meta = Some(m);
                }),
            ),
            (
                "grants",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.grants = OmissibleGrantConfig(Omissible::Present(
                        GrantConfig(indexmap::IndexMap::from([(
                            "select".to_string(),
                            StringOrArrayOfStrings::String("role1".to_string()),
                        )])),
                    ))
                }),
            ),
            (
                "event_time",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.event_time = Some("updated_at".to_string())),
            ),
            (
                "quoting",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.quoting = Some(DbtQuoting {
                        database: None,
                        schema: None,
                        identifier: Some(false),
                        snowflake_ignore_case: None,
                    })
                }),
            ),
            (
                "static_analysis",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.static_analysis =
                        Some(Spanned::new(StaticAnalysisKind::Off))
                }),
            ),
            (
                "quote_columns",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.quote_columns = Some(true)),
            ),
            (
                "invalidate_hard_deletes",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.invalidate_hard_deletes = Some(true)),
            ),
            // --- parity-excludes: dbt-core's `CompareBehavior.Exclude` on `NodeAndTestConfig` ---
            (
                "tags",
                ExcludeKind::Parity,
                Box::new(|n| {
                    n.deprecated_config.tags =
                        Some(StringOrArrayOfStrings::String("a_tag".to_string()))
                }),
            ),
            (
                "group",
                ExcludeKind::Parity,
                Box::new(|n| n.deprecated_config.group = Some("a_group".to_string())),
            ),
            // --- ownership-excludes: owned by `check_relation_modified`, not this comparator ---
            (
                "schema",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.schema = Some("a_schema".to_string())),
            ),
            (
                "database",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.database = Some("a_db".to_string())),
            ),
            (
                "alias",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.alias = Some("an_alias".to_string())),
            ),
        ];

        // Not exercised above: `full_refresh`/`compute`/`docs`/`sync` are not compared by
        // `DbtSnapshot::has_same_config` at all (outside this drift guard's scope). `warehouse_config`
        // is nested adapter-specific config with its own dedicated test coverage.
        assert_denylist_keys_agree_across_stage1_and_stage2::<DbtSnapshot>(
            NodeType::Snapshot,
            "DbtSnapshot::has_same_config",
            &cases,
        );
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn model_config_keys_agree_across_stage1_and_stage2() {
        use crate::schemas::common::{
            Access, DbtBatchSize, DbtIncrementalStrategy, DbtMaterialization, DbtUniqueKey,
            DocsConfig, Hooks, OnError, OnSchemaChange, PersistDocsConfig,
        };
        use crate::schemas::project::configs::model_config::LatestVersionPointer;
        use crate::schemas::properties::{ModelFreshness, ModelState};
        use crate::schemas::serde::{GrantConfig, OmissibleGrantConfig, StringOrArrayOfStrings};
        use dbt_common::serde_utils::Omissible;
        use dbt_yaml::{Spanned, Verbatim};

        let cases: Vec<(&str, ExcludeKind, Box<dyn Fn(&mut DbtModel)>)> = vec![
            // --- fields `ModelConfig::same_config` actually compares ---
            (
                "enabled",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.enabled = Some(false)),
            ),
            (
                "catalog_name",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.catalog_name = Some("a_catalog".to_string())),
            ),
            (
                "meta",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    let mut m = indexmap::IndexMap::new();
                    m.insert("owner".to_string(), dbt_yaml::to_value("bob").unwrap());
                    n.deprecated_config.meta = Some(m);
                }),
            ),
            (
                "materialized",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.materialized = Some(DbtMaterialization::Table)),
            ),
            (
                "incremental_strategy",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.incremental_strategy = Some(DbtIncrementalStrategy::Merge)
                }),
            ),
            (
                "batch_size",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.batch_size = Some(DbtBatchSize::Day)),
            ),
            (
                "lookback",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.lookback = Some(2)),
            ),
            (
                "begin",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.begin = Some("2024-01-01".to_string())),
            ),
            (
                "persist_docs",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.persist_docs = Some(PersistDocsConfig {
                        columns: Some(true),
                        relation: Some(true),
                    })
                }),
            ),
            (
                "post_hook",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.post_hook =
                        Verbatim::from(Some(Hooks::ArrayOfStrings(vec!["select 1".to_string()])))
                }),
            ),
            (
                "pre_hook",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.pre_hook =
                        Verbatim::from(Some(Hooks::ArrayOfStrings(vec!["select 1".to_string()])))
                }),
            ),
            (
                "column_types",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.column_types = Some(BTreeMap::from([(
                        Spanned::new("id".to_string()),
                        "int".to_string(),
                    )]))
                }),
            ),
            (
                "full_refresh",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.full_refresh = Some(true)),
            ),
            (
                "unique_key",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.unique_key = Some(DbtUniqueKey::Single("id".to_string()))
                }),
            ),
            (
                "on_schema_change",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.on_schema_change = Some(OnSchemaChange::AppendNewColumns)
                }),
            ),
            (
                "on_configuration_change",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.on_configuration_change =
                        Some(crate::schemas::common::OnConfigurationChange::Continue)
                }),
            ),
            (
                "on_error",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.on_error = Some(OnError::Continue)),
            ),
            (
                "grants",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.grants = OmissibleGrantConfig(Omissible::Present(
                        GrantConfig(indexmap::IndexMap::from([(
                            "select".to_string(),
                            StringOrArrayOfStrings::String("role1".to_string()),
                        )])),
                    ))
                }),
            ),
            (
                "packages",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.packages =
                        Some(StringOrArrayOfStrings::String("pkg1".to_string()))
                }),
            ),
            (
                "imports",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.imports =
                        Some(StringOrArrayOfStrings::String("import1".to_string()))
                }),
            ),
            (
                "python_version",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.python_version = Some("3.11".to_string())),
            ),
            (
                "docs",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.docs = Some(DocsConfig {
                        show: false,
                        node_color: None,
                    })
                }),
            ),
            (
                "concurrent_batches",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.concurrent_batches = Some(true)),
            ),
            (
                "merge_update_columns",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.merge_update_columns =
                        Some(StringOrArrayOfStrings::String("col1".to_string()))
                }),
            ),
            (
                "merge_exclude_columns",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.merge_exclude_columns =
                        Some(StringOrArrayOfStrings::String("col2".to_string()))
                }),
            ),
            (
                "access",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.access = Some(Access::Public)),
            ),
            (
                "table_format",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.table_format = Some("iceberg".to_string())),
            ),
            (
                "freshness",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.freshness = Some(ModelFreshness { build_after: None })
                }),
            ),
            (
                "state",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.state = Some(ModelState {
                        lag_tolerance: None,
                        require_fresh_data_from: None,
                        evaluate_volatile_sql: Some(true),
                        pre_clone: None,
                        execute_hooks_on_any_reuse: None,
                    })
                }),
            ),
            (
                "latest_version_pointer",
                ExcludeKind::Relevant,
                Box::new(|n| {
                    n.deprecated_config.latest_version_pointer = Some(LatestVersionPointer {
                        enabled: Some(true),
                        alias: None,
                    })
                }),
            ),
            (
                "sql_header",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.sql_header = Some("set x = 1".to_string())),
            ),
            (
                "location",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.location = Some("s3://bucket/path".to_string())),
            ),
            (
                "predicates",
                ExcludeKind::Relevant,
                Box::new(|n| n.deprecated_config.predicates = Some(vec!["1=1".to_string()])),
            ),
            // --- parity-excludes: dbt-core's `CompareBehavior.Exclude` on `NodeAndTestConfig` ---
            (
                "tags",
                ExcludeKind::Parity,
                Box::new(|n| {
                    n.deprecated_config.tags =
                        Some(StringOrArrayOfStrings::String("a_tag".to_string()))
                }),
            ),
            (
                "group",
                ExcludeKind::Parity,
                Box::new(|n| n.deprecated_config.group = Some("a_group".to_string())),
            ),
            // --- ownership-excludes: owned by `check_relation_modified`, not this comparator ---
            (
                "schema",
                ExcludeKind::Ownership,
                Box::new(|n| {
                    n.deprecated_config.schema = Omissible::Present(Some("a_schema".to_string()))
                }),
            ),
            (
                "database",
                ExcludeKind::Ownership,
                Box::new(|n| {
                    n.deprecated_config.database = Omissible::Present(Some("a_db".to_string()))
                }),
            ),
            (
                "alias",
                ExcludeKind::Ownership,
                Box::new(|n| n.deprecated_config.alias = Some("an_alias".to_string())),
            ),
        ];

        // Not exercised above: `quoting` and `event_time` are declared on `ModelConfig` but their
        // comparisons are commented out in `ModelConfig::same_config` (env-aware / project-level,
        // deliberately not compared there) — outside this drift guard's scope, which only
        // transcribes the comparator's actual active field list. `warehouse_config` is nested
        // adapter-specific config with its own dedicated test coverage.
        assert_denylist_keys_agree_across_stage1_and_stage2::<DbtModel>(
            NodeType::Model,
            "ModelConfig::same_config",
            &cases,
        );
    }
}
