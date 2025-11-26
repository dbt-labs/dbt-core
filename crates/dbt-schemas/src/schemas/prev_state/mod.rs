use super::{RunResultsArtifact, manifest::DbtManifest, sources::FreshnessResultsArtifact};
use crate::schemas::common::{DbtQuoting, ResolvedQuoting};
use crate::schemas::manifest::nodes_from_dbt_manifest;
use crate::schemas::serde::typed_struct_from_json_file;
use crate::schemas::{
    InternalDbtNode, Nodes, nodes::DbtModel, nodes::is_invalid_for_relation_comparison,
};
use dbt_common::{FsResult, constants::DBT_MANIFEST_JSON};
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct PreviousState {
    pub nodes: Option<Nodes>,
    pub run_results: Option<RunResultsArtifact>,
    pub source_freshness_results: Option<FreshnessResultsArtifact>,
    pub state_path: PathBuf,
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
        // Try to load manifest.json, but make it optional
        let nodes = if let Ok(manifest) =
            typed_struct_from_json_file::<DbtManifest>(&state_path.join(DBT_MANIFEST_JSON))
        {
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
        } else {
            None
        };

        Ok(Self {
            nodes,
            run_results: RunResultsArtifact::from_file(&state_path.join("run_results.json")).ok(),
            source_freshness_results: typed_struct_from_json_file(&state_path.join("sources.json"))
                .ok(),
            state_path: state_path.to_path_buf(),
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
                    || self.check_modified_content(node) // Order is important here, check_modified_content should be last as it is the most generic and could potentially match prevuous cases
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

        !current_node.has_same_content(previous_node)
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

        !current_node.has_same_config(previous_node)
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

        // Check if database representation changed (database, schema, alias)
        // For now, we just compare the alias as the database and schema are not
        // yet available in fusion as underendered values, which is what the dbt-core does
        let current_alias = &current_node.base().alias;
        let previous_alias = &previous_node.base().alias;

        // Helper function to normalize alias by trimming whitespace, newlines, and quotes
        fn normalize_alias(alias: &str) -> &str {
            alias.trim_matches(|c: char| c.is_whitespace() || c == '\n' || c == '"')
        }

        let normalized_current = normalize_alias(current_alias);
        let normalized_previous = normalize_alias(previous_alias);

        normalized_current != normalized_previous
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

        // Helper function to normalize descriptions: treat None and Some("") as equal
        // and trim leading/trailing newlines for non-empty descriptions
        fn normalize_description(desc: &Option<String>) -> Option<String> {
            desc.as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| s.trim_matches('\n').to_string())
        }

        normalize_description(&current_node.common().description)
            != normalize_description(&previous_node.common().description)
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
            !current_model.same_contract(previous_model)
        } else {
            false
        }
    }
}
