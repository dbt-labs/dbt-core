use super::{RunResultsArtifact, manifest::DbtManifest, sources::FreshnessResultsArtifact};
use crate::schemas::common::{DbtQuoting, ResolvedQuoting};
use crate::schemas::manifest::nodes_from_dbt_manifest;
use crate::schemas::project::configs::common::log_state_mod_diff;
use crate::schemas::serde::typed_struct_from_json_file;
use crate::schemas::{
    InternalDbtNode, Nodes, nodes::DbtModel, nodes::is_invalid_for_relation_comparison,
    nodes::normalize_description,
};
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult, constants::DBT_MANIFEST_JSON};
use dbt_telemetry::NodeType;
use std::fmt;
use std::path::{Path, PathBuf};

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

impl fmt::Display for PreviousState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PreviousState from {}", self.state_path.display())
    }
}

impl PreviousState {
    pub fn try_new(state_path: &Path, root_project_quoting: ResolvedQuoting) -> FsResult<Self> {
        Self::try_new_with_target_path(state_path, root_project_quoting, None, true)
    }

    /// Creates a new `PreviousState` from the given state path.
    ///
    /// # Arguments
    /// * `state_path` - The path to the state directory containing manifest.json and other artifacts
    /// * `root_project_quoting` - The quoting configuration for the root project
    /// * `target_path` - Optional target path for the output directory
    /// * `warn_on_manifest_load_failure` - If true, emits a warning when manifest.json fails to load.
    ///   This should be set to true when the selector includes `state:modified` or `state:new`,
    ///   and false for other selectors like `source_status:fresher+` that don't require the manifest.
    pub fn try_new_with_target_path(
        state_path: &Path,
        root_project_quoting: ResolvedQuoting,
        target_path: Option<PathBuf>,
        warn_on_manifest_load_failure: bool,
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
                if warn_on_manifest_load_failure {
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
            self.nodes
                .as_ref()
                .and_then(|nodes| nodes.get_node(node.common().unique_id.as_str()))
                .is_some()
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
            Some(ModificationType::Body) => self.check_modified_content(node),
            Some(ModificationType::Configs) => self.check_configs_modified(node),
            Some(ModificationType::Relation) => self.check_relation_modified(node),
            Some(ModificationType::PersistedDescriptions) => {
                self.check_persisted_descriptions_modified(node)
            }
            // Macro modification is check_modified_content as per dbt-core
            Some(ModificationType::Macros) => self.check_modified_content(node),
            Some(ModificationType::Contract) => self.check_contract_modified(node),
            Some(ModificationType::Any) | None => {
                self.check_contract_modified(node)
                    || self.check_configs_modified(node)
                    || self.check_relation_modified(node)
                    || self.check_persisted_descriptions_modified(node)
                    || self.check_modified_content(node) // Order is important here, check_modified_content should be last as it is the most generic and could potentially match previous cases
            }
        }
    }

    // Private helper methods to check specific types of modifications
    fn check_modified_content(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .as_ref()
            .and_then(|nodes| nodes.get_node(current_node.common().unique_id.as_str()))
        {
            Some(node) => node,
            // TODO test is currently ignored in the state selector because fusion generate test name different from dbt-mantle.
            None => return !current_node.is_never_new_if_previous_missing(), // If previous node doesn't exist, consider it modified
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
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .as_ref()
            .and_then(|nodes| nodes.get_node(current_node.common().unique_id.as_str()))
        {
            Some(node) => node,
            None => return !current_node.is_never_new_if_previous_missing(), // If previous node doesn't exist, consider it modified
        };

        // Mantle semantics for `state:modified` configs are based on configured/unrendered config,
        // not rendered config. Compare key config knobs from `unrendered_config` when present.
        if current_node.resource_type() == NodeType::Model
            && previous_node.resource_type() == NodeType::Model
        {
            use dbt_serde_yaml::Value as YmlValue;

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

        let same_config = current_node.has_same_config(previous_node);

        !same_config
    }

    fn check_relation_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        if is_invalid_for_relation_comparison(current_node) {
            return false;
        }

        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .as_ref()
            .and_then(|nodes| nodes.get_node(current_node.common().unique_id.as_str()))
        {
            Some(node) => node,
            None => return !current_node.is_never_new_if_previous_missing(), // If previous node doesn't exist, consider it modified
        };

        // Check if database representation changed (database, schema, alias).
        //
        // Prefer comparing unrendered (configured) values, matching dbt-core semantics for
        // state selection: differences that come purely from target rendering should not
        // count as modifications.
        let current_uc = &current_node.base().unrendered_config;
        let previous_uc = &previous_node.base().unrendered_config;

        fn get<'a>(
            m: &'a std::collections::BTreeMap<String, dbt_serde_yaml::Value>,
            k: &str,
        ) -> Option<&'a str> {
            m.get(k).and_then(|v| v.as_str())
        }

        if !current_uc.is_empty() && !previous_uc.is_empty() {
            let db_eq = get(current_uc, "database") == get(previous_uc, "database");
            let schema_eq = get(current_uc, "schema") == get(previous_uc, "schema");
            let alias_eq = get(current_uc, "alias") == get(previous_uc, "alias");
            let is_same_relation = db_eq && schema_eq && alias_eq;

            if !is_same_relation {
                log_state_mod_diff(
                    &current_node.common().unique_id,
                    "relation",
                    [
                        (
                            "database",
                            db_eq,
                            Some((
                                format!("{:?}", get(current_uc, "database")),
                                format!("{:?}", get(previous_uc, "database")),
                            )),
                        ),
                        (
                            "schema",
                            schema_eq,
                            Some((
                                format!("{:?}", get(current_uc, "schema")),
                                format!("{:?}", get(previous_uc, "schema")),
                            )),
                        ),
                        (
                            "alias",
                            alias_eq,
                            Some((
                                format!("{:?}", get(current_uc, "alias")),
                                format!("{:?}", get(previous_uc, "alias")),
                            )),
                        ),
                    ],
                );
            }

            return !is_same_relation;
        }

        // Fallback: compare just the rendered alias (legacy behavior).
        let current_alias = &current_node.base().alias;
        let previous_alias = &previous_node.base().alias;

        // Helper function to normalize alias by trimming whitespace, newlines, and quotes
        fn normalize_alias(alias: &str) -> &str {
            alias.trim_matches(|c: char| c.is_whitespace() || c == '\n' || c == '"')
        }

        let normalized_current = normalize_alias(current_alias);
        let normalized_previous = normalize_alias(previous_alias);

        let is_same_relation = normalized_current == normalized_previous;
        if !is_same_relation {
            log_state_mod_diff(
                &current_node.common().unique_id,
                "relation",
                [(
                    "alias",
                    false,
                    Some((
                        format!("{:?}", current_alias),
                        format!("{:?}", previous_alias),
                    )),
                )],
            );
        }
        !is_same_relation
    }

    fn check_persisted_descriptions_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .as_ref()
            .and_then(|nodes| nodes.get_node(current_node.common().unique_id.as_str()))
        {
            Some(node) => node,
            None => return !current_node.is_never_new_if_previous_missing(), // If previous node doesn't exist, consider it modified
        };

        // Check if persisted descriptions changed
        // Persist docs for relations and columns are deprecated in fusion, so they are not used
        // as additional check flags as they are in dbt-core.
        // https://github.com/dbt-labs/dbt-core/blob/906e07c1f2161aaf8873f17ba323221a3cf48c9f/core/dbt/contracts/graph/nodes.py#L330-L345

        let is_same_desc = normalize_description(&current_node.common().description)
            == normalize_description(&previous_node.common().description);
        if !is_same_desc {
            log_state_mod_diff(
                &current_node.common().unique_id,
                "persisted_descriptions",
                [(
                    "description",
                    false,
                    Some((
                        format!("{:?}", &current_node.common().description),
                        format!("{:?}", &previous_node.common().description),
                    )),
                )],
            );
        }
        !is_same_desc
    }

    fn check_contract_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .as_ref()
            .and_then(|nodes| nodes.get_node(current_node.common().unique_id.as_str()))
        {
            Some(node) => node,
            None => return !current_node.is_never_new_if_previous_missing(), // If previous node doesn't exist, consider it modified
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
}
