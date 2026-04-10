use super::{RunResultsArtifact, manifest::DbtManifest, sources::FreshnessResultsArtifact};
use crate::schemas::common::{DbtQuoting, ResolvedQuoting};
use crate::schemas::manifest::nodes_from_dbt_manifest;
use crate::schemas::project::configs::common::log_state_mod_diff;
use crate::schemas::serde::typed_struct_from_json_file;
use crate::schemas::{
    InternalDbtNode, Nodes, nodes::DbtModel, nodes::DbtTest,
    nodes::is_invalid_for_relation_comparison, nodes::same_persisted_description,
};
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, constants::DBT_MANIFEST_JSON, fs_err};
use dbt_telemetry::NodeType;
use std::fmt;
use std::path::{Path, PathBuf};

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

    fn find_previous_test_by_signature<'a>(
        &'a self,
        current: &DbtTest,
        nodes: &'a Nodes,
    ) -> Option<&'a dyn InternalDbtNode> {
        let sig = Self::test_signature(current)?;

        let mut found: Option<&'a dyn InternalDbtNode> = None;
        for prev in nodes.tests.values() {
            if let Some(prev_sig) = Self::test_signature(prev.as_ref()) {
                if prev_sig == sig {
                    if found.is_some() {
                        // Ambiguous match; avoid incorrect "exists" classification.
                        return None;
                    }
                    found = Some(prev.as_ref() as &dyn InternalDbtNode);
                }
            }
        }

        found
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
                return self.find_previous_test_by_signature(cur_test, nodes);
            }
        }

        None
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

        Ok(Self {
            nodes,
            run_results: RunResultsArtifact::from_file(&state_path.join("run_results.json")).ok(),
            source_freshness_results: typed_struct_from_json_file(&state_path.join("sources.json"))
                .ok(),
            state_path: state_path.to_path_buf(),
            target_path,
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

        match modification_type {
            Some(ModificationType::Body) => self.check_body_modified(node),
            Some(ModificationType::Configs) => self.check_configs_modified(node),
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
                    || self.check_configs_modified(node)
                    || self.check_relation_modified(node)
                    || self.check_persisted_descriptions_modified(node)
                    || self.check_modified_macros(node, current_nodes)
                    || self.check_modified_content(node) // Order is important here, check_modified_content should be last as it is the most generic and could potentially match previous cases
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
                        if cur.macro_sql != prev.macro_sql {
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
    fn check_modified_content(&self, current_node: &dyn InternalDbtNode) -> bool {
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

        if current_node.has_same_content(previous_node) {
            return false;
        }

        true
    }

    fn check_configs_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
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

            // dbt-core (and Mantle-produced manifests) may represent a single tag as a scalar
            // string, while Fusion may produce a one-element list. These are semantically equal.
            fn tags_eq(a: Option<&YmlValue>, b: Option<&YmlValue>) -> Option<bool> {
                fn norm(v: Option<&YmlValue>) -> Option<Vec<String>> {
                    use dbt_yaml::Value as YmlValue;
                    match v {
                        None => Some(vec![]),
                        Some(YmlValue::Null(_)) => Some(vec![]),
                        Some(YmlValue::String(s, _)) => Some(vec![canonicalize_str(s).to_string()]),
                        Some(YmlValue::Sequence(seq, _)) => {
                            let mut out = Vec::with_capacity(seq.len());
                            for item in seq {
                                match item {
                                    YmlValue::String(s, _) => {
                                        out.push(canonicalize_str(s).to_string());
                                    }
                                    // Unexpected element types: fall back to raw equality.
                                    _ => return None,
                                }
                            }
                            // Tags are order-insensitive.
                            out.sort_unstable();
                            out.dedup();
                            Some(out)
                        }
                        // Unexpected tags type: fall back to raw equality.
                        _ => None,
                    }
                }

                Some(norm(a)? == norm(b)?)
            }

            fn get_any<'a>(
                m: &'a std::collections::BTreeMap<String, YmlValue>,
                keys: &[&str],
            ) -> Option<&'a YmlValue> {
                keys.iter().find_map(|k| m.get(*k))
            }

            // Key groups: dbt-core has historically used both dash and underscore variants for hooks.
            let checks: [(&'static str, &[&str]); 5] = [
                ("grants", &["grants"]),
                ("pre_hook", &["pre-hook", "pre_hook"]),
                ("post_hook", &["post-hook", "post_hook"]),
                ("tags", &["tags"]),
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
                    let eq = if name == "tags" {
                        tags_eq(a, b).unwrap_or_else(|| uc_eq(a, b))
                    } else {
                        uc_eq(a, b)
                    };
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

        let same_config = current_node.has_same_config(previous_node);

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
