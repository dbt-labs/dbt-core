use std::{
    any::Any,
    collections::{BTreeMap, HashSet},
    iter::Iterator,
};

use dbt_common::{
    err, io_args::IoArgs, show_error, unexpected_err, CodeLocation, ErrorCode, FsResult,
};
use dbt_fusion_adapter::relation_object::create_relation_from_node;
use dbt_schemas::{
    schemas::{
        common::DbtQuoting,
        ref_and_source::{DbtRef, DbtSourceWrapper},
        DbtSource, InternalDbtNodeAttributes, Nodes,
    },
    state::{ModelStatus, RefsAndSourcesTracker},
};
use minijinja::Value as MinijinjaValue;

/// A wrapper around refs and sources with methods to get and insert refs and sources
#[derive(Debug, Default, Clone)]
pub struct RefsAndSources {
    /// Map of ref_name (either {project}.{ref_name}, {ref_name}) to (unique_id, relation, status)
    #[allow(clippy::type_complexity)]
    pub refs: BTreeMap<String, Vec<(String, MinijinjaValue, ModelStatus)>>,
    /// Map of (package_name.source_name.name ) to (unique_id, relation, status)
    pub sources: BTreeMap<String, Vec<(String, MinijinjaValue, ModelStatus)>>,
    /// Root project name (needed for resolving refs)
    pub root_package_name: String,
    /// Optional Quoting Config produced by mantle/core manifest needed for back compatibility for defer in fusion
    pub mantle_quoting: Option<DbtQuoting>,
}

impl RefsAndSources {
    /// Create a new RefsAndSources from a DbtManifest
    pub fn from_dbt_nodes(
        nodes: &Nodes,
        adapter_type: &str,
        root_package_name: String,
        mantle_quoting: Option<DbtQuoting>,
    ) -> FsResult<Self> {
        let mut refs_and_sources = RefsAndSources {
            root_package_name,
            mantle_quoting,
            ..Default::default()
        };
        for (_, node) in nodes.iter() {
            if let Some(source) = node.as_any().downcast_ref::<DbtSource>() {
                refs_and_sources.insert_source(
                    &node.common().package_name,
                    source,
                    adapter_type,
                    ModelStatus::Enabled,
                )?;
            } else {
                refs_and_sources.insert_ref(node, adapter_type, ModelStatus::Enabled, false)?;
            }
        }
        Ok(refs_and_sources)
    }

    /// Merge another RefsAndSources into this one, avoiding duplicates
    /// This uses functional programming style for cleaner code
    pub fn merge(&mut self, source: RefsAndSources) {
        for (key, source_entries) in source.refs {
            let target_entries = self.refs.entry(key).or_default();
            let existing_ids: HashSet<String> =
                target_entries.iter().map(|(id, _, _)| id.clone()).collect();

            // Add only entries that don't exist in target
            target_entries.extend(
                source_entries
                    .into_iter()
                    .filter(|(unique_id, _, _)| !existing_ids.contains(unique_id)),
            );
        }

        for (key, source_entries) in source.sources {
            let target_entries = self.sources.entry(key).or_default();
            let existing_ids: HashSet<String> =
                target_entries.iter().map(|(id, _, _)| id.clone()).collect();

            // Add only entries that don't exist in target
            target_entries.extend(
                source_entries
                    .into_iter()
                    .filter(|(unique_id, _, _)| !existing_ids.contains(unique_id)),
            );
        }
    }
}

impl RefsAndSourcesTracker for RefsAndSources {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// Insert or overwrite a ref from a node into the refs map
    fn insert_ref(
        &mut self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: &str,
        status: ModelStatus,
        override_existing: bool,
    ) -> FsResult<()> {
        // If the latest version and current version are the same, the unversioned ref must point to the latest
        let package_name = &node.common().package_name;
        let model_name = node.common().name.clone();
        let unique_id = node.common().unique_id.clone();
        let (maybe_version, maybe_latest_version) = if node.resource_type() == "model" {
            (node.version(), node.latest_version())
        } else {
            (None, None)
        };
        let relation = create_relation_from_node(adapter_type.to_string(), node)?.as_value();
        if maybe_version == maybe_latest_version {
            // Lookup by ref name
            let ref_entry = self.refs.entry(model_name.clone()).or_default();
            if override_existing {
                if let Some(existing) = ref_entry.iter_mut().find(|(id, _, _)| id == &unique_id) {
                    *existing = (unique_id.to_string(), relation.clone(), status);
                } else {
                    ref_entry.push((unique_id.to_string(), relation.clone(), status));
                }
            } else {
                ref_entry.push((unique_id.to_string(), relation.clone(), status));
            }

            // Lookup by package and ref name
            let package_ref_entry = self
                .refs
                .entry(format!("{}.{}", package_name, model_name))
                .or_default();
            if override_existing {
                if let Some(existing) = package_ref_entry
                    .iter_mut()
                    .find(|(id, _, _)| id == &unique_id)
                {
                    *existing = (unique_id.to_string(), relation.clone(), status);
                } else {
                    package_ref_entry.push((unique_id.to_string(), relation.clone(), status));
                }
            } else {
                package_ref_entry.push((unique_id.to_string(), relation.clone(), status));
            }
        }

        // All other entries are versioned, if one exists
        if let Some(version) = maybe_version {
            let model_name_with_version = format!("{}.v{}", model_name, version);

            // Lookup by ref name (optional version)
            let versioned_ref_entry = self
                .refs
                .entry(model_name_with_version.to_owned())
                .or_default();
            if override_existing {
                if let Some(existing) = versioned_ref_entry
                    .iter_mut()
                    .find(|(id, _, _)| id == &unique_id)
                {
                    *existing = (unique_id.to_string(), relation.clone(), status);
                } else {
                    versioned_ref_entry.push((unique_id.to_string(), relation.clone(), status));
                }
            } else {
                versioned_ref_entry.push((unique_id.to_string(), relation.clone(), status));
            }

            let package_versioned_ref_entry = self
                .refs
                .entry(format!("{}.{}", package_name, model_name_with_version))
                .or_default();
            if override_existing {
                if let Some(existing) = package_versioned_ref_entry
                    .iter_mut()
                    .find(|(id, _, _)| id == &unique_id)
                {
                    *existing = (unique_id.clone(), relation, status);
                } else {
                    package_versioned_ref_entry.push((unique_id, relation, status));
                }
            } else if !package_versioned_ref_entry
                .iter()
                .any(|(id, _, _)| id == &unique_id)
            {
                package_versioned_ref_entry.push((unique_id, relation, status));
            }
        }
        Ok(())
    }

    /// Insert a source into the refs and sources map
    fn insert_source(
        &mut self,
        package_name: &str,
        source: &DbtSource,
        adapter_type: &str,
        status: ModelStatus,
    ) -> FsResult<()> {
        let relation = create_relation_from_node(adapter_type.to_string(), source)?.as_value();

        self.sources
            .entry(format!(
                "{}.{}.{}",
                package_name, source.source_name, source.common_attr.name
            ))
            .or_default()
            .push((
                source.common_attr.unique_id.clone(),
                relation.clone(),
                status,
            ));
        self.sources
            .entry(format!(
                "{}.{}",
                source.source_name, source.common_attr.name
            ))
            .or_default()
            .push((source.common_attr.unique_id.clone(), relation, status));
        Ok(())
    }

    /// Lookup a ref by package name, model name, and optional version
    fn lookup_ref(
        &self,
        maybe_package_name: &Option<String>,
        name: &str,
        version: &Option<String>,
        maybe_node_package_name: &Option<String>,
    ) -> FsResult<(String, MinijinjaValue, ModelStatus)> {
        // Create a list of packages to search, where None means to
        // search non-package limited names
        let root_package = Some(self.root_package_name.clone());
        let search_packages = match (maybe_package_name, maybe_node_package_name) {
            // If maybe_package_name is specified, only search that package
            (Some(_), _) => vec![maybe_package_name],
            // If maybe_node_package_name is specified, and this is the root package,
            // search this package and the global refs
            (None, Some(node_pkg)) if *node_pkg == self.root_package_name => {
                vec![&root_package, &None]
            }
            // If maybe_node_package_name is specified, and this is not the root package,
            // search this package, the root package, and then finally global refs
            (None, Some(_)) => vec![maybe_node_package_name, &root_package, &None],
            // If maybe_package_name and maybe_node_package_name are not specified,
            // search only the global refs
            (None, None) => vec![&None],
        };

        // Construct possibly versioned ref_name
        let ref_name = format!(
            "{}{}",
            name,
            version
                .as_ref()
                .map(|v| format!(".v{}", v))
                .unwrap_or_default()
        );
        let mut enabled_ref: Option<(String, MinijinjaValue, ModelStatus)> = None;
        let mut disabled_ref: Option<(String, MinijinjaValue, ModelStatus)> = None;
        let mut search_ref_names: Vec<String> = Vec::new();
        for maybe_package in search_packages.iter() {
            // If this is a package, use the package name + ref_name to search
            let search_ref_name = if let Some(package_name) = maybe_package {
                format!("{}.{}", package_name.clone(), ref_name)
            } else {
                // If this is not a package, just use the ref_name to search
                ref_name.clone()
            };
            search_ref_names.push(search_ref_name.clone());
            if let Some(res) = self.refs.get(&search_ref_name) {
                let (enabled_refs, disabled_refs): (Vec<_>, Vec<_>) = res
                    .iter()
                    .partition(|(_, _, status)| *status != ModelStatus::Disabled);
                // We got a ref or we wouldn't be here
                if !disabled_refs.is_empty() {
                    disabled_ref = Some(disabled_refs[0].clone());
                }
                match enabled_refs.len() {
                    // If there is one enabled ref, use it
                    1 => {
                        enabled_ref = Some(enabled_refs[0].clone());
                        break;
                    }
                    n if n > 1 => {
                        // More than one enabled ref with the same name, issue error
                        return err!(
                            ErrorCode::InvalidConfig,
                            "Found ambiguous ref('{}') pointing to multiple nodes: [{}]",
                            ref_name,
                            res.iter()
                                .map(|(r, _, _)| format!("'{}'", r))
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                    }
                    // If there are no enabled refs, continue to next package
                    _ => {}
                };
            }
        }
        // If ref not found issue error
        match enabled_ref {
            Some(ref_result) => Ok(ref_result),
            None => {
                if disabled_ref.is_some() {
                    err!(
                        ErrorCode::DisabledDependency,
                        "Attempted to use disabled ref '{}'",
                        ref_name
                    )
                } else {
                    err!(
                        ErrorCode::InvalidConfig,
                        "Ref '{}' not found in project. Searched for '{}'",
                        ref_name,
                        search_ref_names.join(", ")
                    )
                }
            }
        }
    }

    /// Lookup a source by package name, source name, and table name
    fn lookup_source(
        &self,
        package_name: &str,
        source_name: &str,
        table_name: &str,
    ) -> FsResult<(String, MinijinjaValue, ModelStatus)> {
        // This might not be correct if there is overlap in source names amongst projects
        let source_table_name = format!("{}.{}", source_name, table_name);
        let project_source_name = format!("{}.{}", package_name, source_table_name);
        if let Some(res) = self.sources.get(&project_source_name) {
            if res.len() != 1 {
                return unexpected_err!("There should only be one entry for {project_source_name}");
            }
            let (_, _, status) = res[0].clone();
            if status == ModelStatus::Disabled {
                err!(
                    ErrorCode::DisabledDependency,
                    "Attempted to use disabled source '{}'",
                    project_source_name
                )
            } else {
                Ok(res[0].clone())
            }
        } else if let Some(res) = self.sources.get(&source_table_name) {
            let enabled_sources: Vec<_> = res
                .iter()
                .filter(|(_, _, status)| *status != ModelStatus::Disabled)
                .collect();
            if enabled_sources.len() == 1 {
                Ok(enabled_sources[0].clone())
            } else if enabled_sources.is_empty() {
                err!(
                    ErrorCode::DisabledDependency,
                    "Attempted to use disabled source '{}'",
                    source_table_name
                )
            } else {
                err!(
                    ErrorCode::InvalidConfig,
                    "Found ambiguous source('{}') pointing to multiple nodes: [{}]",
                    source_table_name,
                    res.iter()
                        .map(|(r, _, _)| format!("'{}'", r))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        } else {
            err!(
                ErrorCode::InvalidConfig,
                "Source '{}' not found in project. Searched for '{}'",
                source_table_name,
                table_name
            )
        }
    }
}

/// Resolve the dependencies for a model
pub fn resolve_dependencies(
    io: &IoArgs,
    nodes: &mut Nodes,
    disabled_nodes: &mut Nodes,
    refs_and_sources: &RefsAndSources,
) {
    let mut tests_to_disable = Vec::new();

    // First pass: identify tests with disabled dependencies
    for node in nodes.iter_values_mut() {
        // Clone needed values first to avoid borrowing issues
        let node_path = node.common().path.clone();
        let node_package_name = node.common().package_name.clone();
        let node_unique_id = node.common().unique_id.clone();
        let is_test = node.is_test();

        let node_base = node.base_mut();
        if let Some(node_base) = node_base {
            let mut has_disabled_dependency = false;

            // Check refs
            let node_package_name_value = &Some(node_package_name.clone());
            for DbtRef {
                name,
                package,
                version,
                location,
            } in node_base.refs.iter()
            {
                let location = if let Some(location) = location {
                    location.clone().with_file(&node_path)
                } else {
                    CodeLocation::default()
                };
                match refs_and_sources.lookup_ref(
                    package,
                    name,
                    &version.as_ref().map(|v| v.to_string()),
                    node_package_name_value,
                ) {
                    Ok((dependency_id, _, _)) => {
                        node_base.depends_on.nodes.push(dependency_id.clone());
                        node_base
                            .depends_on
                            .nodes_with_ref_location
                            .push((dependency_id, location));
                    }
                    Err(e) => {
                        // Check if this is a disabled dependency error
                        if is_test && e.code == ErrorCode::DisabledDependency {
                            has_disabled_dependency = true;
                        } else {
                            show_error!(io, e.with_location(location));
                        }
                    }
                };
            }

            // Check sources
            for DbtSourceWrapper { source, location } in node_base.sources.iter() {
                // Source is &Vec<String> (first two elements are source and table)
                let source_name = source[0].clone();
                let table_name = source[1].clone();

                let location = if let Some(location) = location {
                    location.clone().with_file(&node_path)
                } else {
                    CodeLocation::default()
                };

                match refs_and_sources.lookup_source(&node_package_name, &source_name, &table_name)
                {
                    Ok((dependency_id, _, _)) => {
                        node_base.depends_on.nodes.push(dependency_id.clone());
                        node_base
                            .depends_on
                            .nodes_with_ref_location
                            .push((dependency_id, location));
                    }
                    Err(e) => {
                        // Check if this is a disabled dependency error
                        if is_test && e.code == ErrorCode::DisabledDependency {
                            has_disabled_dependency = true;
                        } else {
                            show_error!(io, e.with_location(location));
                        }
                    }
                };
            }

            if is_test && has_disabled_dependency {
                tests_to_disable.push(node_unique_id);
            }
        }
    }

    // Second pass: move disabled tests to disabled_nodes
    for test_id in &tests_to_disable {
        if let Some(node) = nodes.tests.remove(test_id) {
            disabled_nodes.tests.insert(test_id.clone(), node);
        }
    }
}
