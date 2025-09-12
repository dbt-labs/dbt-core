use super::{RunResultsArtifact, manifest::DbtManifest};
use crate::schemas::common::{DbtQuoting, ResolvedQuoting};
use crate::schemas::manifest::nodes_from_dbt_manifest;
use crate::schemas::serde::typed_struct_from_json_file;
use crate::schemas::{InternalDbtNode, Nodes, nodes::DbtModel};
use dbt_common::{FsResult, constants::DBT_MANIFEST_JSON};
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct PreviousState {
    pub nodes: Nodes,
    pub run_results: Option<RunResultsArtifact>,
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
        let manifest: DbtManifest =
            typed_struct_from_json_file(&state_path.join(DBT_MANIFEST_JSON))?;
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

        Ok(Self {
            nodes: nodes_from_dbt_manifest(manifest, quoting),
            run_results: RunResultsArtifact::from_file(&state_path.join("run_results.json")).ok(),
            state_path: state_path.to_path_buf(),
        })
    }

    // Check if a node exists in the previous state
    pub fn exists(&self, node: &dyn InternalDbtNode) -> bool {
        if node.is_test() {
            true
        } else {
            self.nodes
                .get_node(node.common().unique_id.as_str())
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
            .get_node(current_node.common().unique_id.as_str())
        {
            Some(node) => node,
            // TODO test is currently ignored in the state selector because fusion generate test name different from dbt-mantle.
            None => return !current_node.is_test(), // If previous node doesn't exist, consider it modified
        };

        !current_node.has_same_content(previous_node)
    }

    fn check_configs_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .get_node(current_node.common().unique_id.as_str())
        {
            Some(node) => node,
            None => return true, // If previous node doesn't exist, consider it modified
        };

        !current_node.has_same_config(previous_node)
    }

    fn check_relation_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .get_node(current_node.common().unique_id.as_str())
        {
            Some(node) => node,
            None => return true, // If previous node doesn't exist, consider it modified
        };

        // Check if database representation changed (database, schema, alias)
        // Compare the database representation fields from the base attributes
        let current_database = &current_node.base().database;
        let current_schema = &current_node.base().schema;
        let current_alias = &current_node.base().alias;

        let previous_database = &previous_node.base().database;
        let previous_schema = &previous_node.base().schema;
        let previous_alias = &previous_node.base().alias;

        current_database != previous_database
            || current_schema != previous_schema
            || current_alias != previous_alias
    }

    fn check_persisted_descriptions_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .get_node(current_node.common().unique_id.as_str())
        {
            Some(node) => node,
            None => return true, // If previous node doesn't exist, consider it modified
        };

        // Check if persisted descriptions changed
        // Persist docs for relations and columns are deprecated in fusion, so they are not used
        // as additional check flags as they are in dbt-core.
        // https://github.com/dbt-labs/dbt-core/blob/906e07c1f2161aaf8873f17ba323221a3cf48c9f/core/dbt/contracts/graph/nodes.py#L330-L345

        // Helper function to normalize descriptions: treat None and Some("") as equal
        fn normalize_description(desc: &Option<String>) -> Option<&str> {
            desc.as_deref().filter(|s| !s.is_empty())
        }

        normalize_description(&current_node.common().description)
            != normalize_description(&previous_node.common().description)
    }

    fn check_contract_modified(&self, current_node: &dyn InternalDbtNode) -> bool {
        // Get the previous node from the manifest
        let previous_node = match self
            .nodes
            .get_node(current_node.common().unique_id.as_str())
        {
            Some(node) => node,
            None => return true, // If previous node doesn't exist, consider it modified
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
