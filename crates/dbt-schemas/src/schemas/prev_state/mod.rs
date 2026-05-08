use super::{RunResultsArtifact, manifest::DbtManifest, sources::FreshnessResultsArtifact};
use crate::schemas::common::{DbtQuoting, ResolvedQuoting};
use crate::schemas::manifest::nodes_from_dbt_manifest;
use crate::schemas::project::configs::common::log_state_mod_diff;
use crate::schemas::serde::typed_struct_from_json_file;
use crate::schemas::{
    InternalDbtNode, Nodes, nodes::DbtModel, nodes::DbtTest,
    nodes::is_invalid_for_relation_comparison, nodes::same_persisted_description,
};
use dbt_adapter_core::AdapterType;
use dbt_common::string_utils::test_name_from_uid;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, constants::DBT_MANIFEST_JSON, fs_err};
use dbt_telemetry::NodeType;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
pub static TEST_SIG_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Controls how a manifest load failure is handled in [`PreviousState::try_new_with_target_path`].
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
pub struct PreviousState {
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

impl fmt::Display for PreviousState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PreviousState from {}", self.state_path.display())
    }
}

impl PreviousState {
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

    /// Constructs a minimal `PreviousState` for use in tests that only need source freshness data.
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

    /// Creates a new `PreviousState` from the given state path.
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

        // Mantle semantics for `state:modified` configs are based on configured/unrendered config,
        // not rendered config. Compare key config knobs from `unrendered_config` when present.
        if current_node.resource_type() == NodeType::Model
            && previous_node.resource_type() == NodeType::Model
        {
            use dbt_yaml::Value as YmlValue;

            let current_uc = &current_node.base().unrendered_config;
            let previous_uc = &previous_node.base().unrendered_config;

            fn is_effectively_empty(v: &YmlValue) -> bool {
                match v {
                    YmlValue::Null(_) => true,
                    YmlValue::Sequence(seq, _) => seq.is_empty(),
                    YmlValue::Mapping(map, _) => map.is_empty(),
                    _ => false,
                }
            }

            fn canonicalize_str(s: &str) -> &str {
                s.strip_suffix("\r\n")
                    .or_else(|| s.strip_suffix('\n'))
                    .unwrap_or(s)
            }

            fn uc_eq(a: Option<&YmlValue>, b: Option<&YmlValue>) -> bool {
                match (a, b) {
                    (None, None) => true,
                    (None, Some(v)) | (Some(v), None) => is_effectively_empty(v),
                    (Some(YmlValue::String(sa, _)), Some(YmlValue::String(sb, _))) => {
                        canonicalize_str(sa) == canonicalize_str(sb)
                    }
                    (Some(va), Some(vb)) => va == vb,
                }
            }

            fn get_any<'a>(
                m: &'a std::collections::BTreeMap<String, YmlValue>,
                keys: &[&str],
            ) -> Option<&'a YmlValue> {
                keys.iter().find_map(|k| m.get(*k))
            }

            // Key groups: dbt-core has historically used both dash and underscore variants for hooks.
            //
            // NOTE: `tags` is intentionally excluded here. In dbt-core/Mantle, tags carry
            // `CompareBehavior::Exclude` and are explicitly skipped in the `same_contents`
            // comparison. Including tags would cause false positives when the state manifest
            // (Mantle-produced) stores only model-level tags while Fusion stores project-level
            // inherited tags — a provenance difference, not a semantic one.
            let checks: [(&'static str, &[&str]); 4] = [
                ("grants", &["grants"]),
                ("pre_hook", &["pre-hook", "pre_hook"]),
                ("post_hook", &["post-hook", "post_hook"]),
                ("persist_docs", &["persist_docs"]),
            ];

            // Only use `unrendered_config` comparisons when *both* current and previous state
            // manifests contain at least one of these keys.
            //
            // Rationale: Mantle-produced state manifests may omit `unrendered_config` entirely
            // (or not include particular keys), in which case dbt-core effectively falls back to
            // rendered config comparisons. If we treat "key present only on one side" as a diff,
            // we'll incorrectly mark nodes modified even when rendered config matches.
            let any_present = checks.iter().any(|(_, keys)| {
                keys.iter()
                    .any(|k| current_uc.contains_key(*k) && previous_uc.contains_key(*k))
            });

            if any_present {
                let mut any_diff = false;
                for (name, keys) in checks {
                    let a = get_any(current_uc, keys);
                    let b = get_any(previous_uc, keys);
                    // Only compare a key when it is present on *both* sides.
                    // If one manifest omits a key (e.g. Mantle records post-hook in
                    // unrendered_config but Fusion does not, or vice-versa), skip it here:
                    // the rendered-config fallback (`has_same_config`) will catch any genuine
                    // change in that field.
                    if a.is_none() || b.is_none() {
                        continue;
                    }
                    let eq = uc_eq(a, b);
                    if !eq {
                        any_diff = true;
                        log_state_mod_diff(
                            &current_node.common().unique_id,
                            "model_config",
                            [(name, eq, Some((format!("{:?}", a), format!("{:?}", b))))],
                        );
                    }
                }
                return any_diff;
            }
        }

        let same_config = current_node.has_same_config(previous_node, adapter_type);
        !same_config
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
        if current_node.resource_type() == NodeType::Source
            && previous_node.resource_type() == NodeType::Source
        {
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

        let test_sig_index = PreviousState::build_test_sig_index(&prev_nodes);
        let test_full_name_index = PreviousState::build_test_full_name_index(&prev_nodes);
        let state = PreviousState {
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
}
