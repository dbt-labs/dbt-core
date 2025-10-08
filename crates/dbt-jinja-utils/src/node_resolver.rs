use std::{
    any::Any,
    collections::{BTreeMap, HashSet},
    iter::Iterator,
};

use dbt_common::{
    CodeLocation, ErrorCode, FsResult, adapter::AdapterType, err, fs_err, io_args::IoArgs,
    show_error, unexpected_err,
};
use dbt_fusion_adapter::relation_object::{
    RelationObject, create_relation_from_node, create_relation_internal,
};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::{
    filter::RunFilter,
    schemas::{
        DbtFunction, DbtSource, InternalDbtNodeAttributes, Nodes,
        common::DbtQuoting,
        ref_and_source::{DbtRef, DbtSourceWrapper},
        telemetry::NodeType,
    },
    state::{ModelStatus, NodeResolverTracker},
};
use minijinja::{Value as MinijinjaValue, value::function_object::FunctionObject};

type RefRecord = (String, MinijinjaValue, ModelStatus, Option<MinijinjaValue>);

/// A wrapper around refs and sources with methods to get and insert refs and sources.
///
/// Carries optional `sample_plan` which, when present, remaps source relations to the
/// sampled schema in remote runs (e.g., `<schema>_SAMPLE` or an explicit `remote.schema`).
#[derive(Debug, Default, Clone)]
pub struct NodeResolver {
    /// Map of ref_name (either {project}.{ref_name}, {ref_name}) to (unique_id, relation, status, deferred_relation)
    #[allow(clippy::type_complexity)]
    pub refs: BTreeMap<String, Vec<RefRecord>>,
    /// Map of (package_name.source_name.name ) to (unique_id, relation, status)
    pub sources: BTreeMap<String, Vec<(String, MinijinjaValue, ModelStatus)>>,
    /// Map of function_name (either {project}.{function_name}, {function_name}) to (unique_id, function_object, status)
    pub functions: BTreeMap<String, Vec<(String, MinijinjaValue, ModelStatus)>>,
    /// Root project name (needed for resolving refs)
    pub root_package_name: String,
    /// Optional Quoting Config produced by mantle/core manifest needed for back compatibility for defer in fusion
    pub mantle_quoting: Option<DbtQuoting>,
    /// Filters that will be applied to `run` or `build` (supports --empty or --sample)
    pub run_filter: RunFilter,
    /// Optional remap plan for sources when sampling is enabled
    pub renaming: BTreeMap<String, (String, String, String)>,
    /// Whether this is a compile or test command
    pub compile_or_test: bool,
}

impl NodeResolver {
    /// Create a new NodeResolver from a DbtManifest
    pub fn from_dbt_nodes(
        nodes: &Nodes,
        adapter_type: AdapterType,
        root_package_name: String,
        mantle_quoting: Option<DbtQuoting>,
        run_filter: RunFilter,
        renaming: BTreeMap<String, (String, String, String)>,
        compile_or_test: bool,
    ) -> FsResult<Self> {
        let mut node_resolver = NodeResolver {
            root_package_name,
            mantle_quoting,
            run_filter,
            renaming,
            compile_or_test,
            ..Default::default()
        };
        for (_, node) in nodes.iter() {
            if let Some(source) = node.as_any().downcast_ref::<DbtSource>() {
                node_resolver.insert_source(
                    &node.common().package_name,
                    source,
                    adapter_type,
                    ModelStatus::Enabled,
                )?;
            } else if let Some(function) = node.as_any().downcast_ref::<DbtFunction>() {
                node_resolver.insert_function(function, adapter_type, ModelStatus::Enabled)?;
            } else {
                match node.resource_type() {
                    NodeType::Model | NodeType::Snapshot | NodeType::Seed => {
                        node_resolver.insert_ref(node, adapter_type, ModelStatus::Enabled, false)?
                    }
                    _ => (),
                }
            }
        }
        Ok(node_resolver)
    }

    /// Merge another NodeResolver into this one, avoiding duplicates
    /// This uses functional programming style for cleaner code
    pub fn merge(&mut self, source: NodeResolver) {
        for (key, source_entries) in source.refs {
            let target_entries = self.refs.entry(key).or_default();
            let existing_ids: HashSet<String> = target_entries
                .iter()
                .map(|(id, _, _, _)| id.clone())
                .collect();

            // Add only entries that don't exist in target
            target_entries.extend(
                source_entries
                    .into_iter()
                    .filter(|(unique_id, _, _, _)| !existing_ids.contains(unique_id)),
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

        for (key, source_entries) in source.functions {
            let target_entries = self.functions.entry(key).or_default();
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

    fn push_or_replace_entry(
        entries: &mut Vec<RefRecord>,
        unique_id: &str,
        relation: &MinijinjaValue,
        status: ModelStatus,
        override_existing: bool,
    ) {
        if override_existing
            && let Some(existing) = entries.iter_mut().find(|(id, _, _, _)| id == unique_id)
        {
            *existing = (unique_id.to_string(), relation.clone(), status, None);
            return;
        }

        entries.push((unique_id.to_string(), relation.clone(), status, None));
    }

    fn push_or_replace_function_entry(
        entries: &mut Vec<(String, MinijinjaValue, ModelStatus)>,
        unique_id: &str,
        function_object: &MinijinjaValue,
        status: ModelStatus,
        override_existing: bool,
    ) {
        if override_existing
            && let Some(existing) = entries.iter_mut().find(|(id, _, _)| id == unique_id)
        {
            *existing = (unique_id.to_string(), function_object.clone(), status);
            return;
        }

        entries.push((unique_id.to_string(), function_object.clone(), status));
    }

    fn set_deferred_relation(
        entries: &mut [RefRecord],
        unique_id: &str,
        deferred_relation: &MinijinjaValue,
        is_frontier: bool,
    ) {
        // For each entry that matches the unique_id, set the deferred relation
        entries
            .iter_mut()
            .filter(|(id, _, _, _)| id == unique_id)
            .for_each(|(_, relation, _, deferred)| {
                *deferred = Some(deferred_relation.clone());
                // If this is a frontier node, we **always** defer if the relation is available
                if is_frontier {
                    *relation = deferred_relation.clone();
                }
            });
    }
}

impl NodeResolverTracker for NodeResolver {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Insert or overwrite a ref from a node into the refs map
    fn insert_ref(
        &mut self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: AdapterType,
        status: ModelStatus,
        override_existing: bool,
    ) -> FsResult<()> {
        // If the latest version and current version are the same, the unversioned ref must point to the latest
        let package_name = &node.package_name();
        let model_name = node.name();
        let unique_id = node.unique_id();
        let (maybe_version, maybe_latest_version) = if node.resource_type() == NodeType::Model {
            (node.version(), node.latest_version())
        } else {
            (None, None)
        };

        let relation = RelationObject::new_with_filter(
            create_relation_from_node(adapter_type, node, Some(self.run_filter.clone()))?,
            self.run_filter.clone(),
            node.event_time(),
        )
        .into_value();

        if maybe_version == maybe_latest_version {
            // Lookup by ref name
            let ref_entry = self.refs.entry(model_name.clone()).or_default();
            Self::push_or_replace_entry(
                ref_entry,
                &unique_id,
                &relation,
                status,
                override_existing,
            );

            // Lookup by package and ref name
            let package_ref_entry = self
                .refs
                .entry(format!("{package_name}.{model_name}"))
                .or_default();
            Self::push_or_replace_entry(
                package_ref_entry,
                &unique_id,
                &relation,
                status,
                override_existing,
            );
        }

        // All other entries are versioned, if one exists
        if let Some(version) = maybe_version {
            let model_name_with_version = format!("{model_name}.v{version}");

            // Lookup by ref name (optional version)
            let versioned_ref_entry = self
                .refs
                .entry(model_name_with_version.to_owned())
                .or_default();
            Self::push_or_replace_entry(
                versioned_ref_entry,
                &unique_id,
                &relation,
                status,
                override_existing,
            );

            let package_versioned_ref_entry = self
                .refs
                .entry(format!("{package_name}.{model_name_with_version}"))
                .or_default();
            if override_existing {
                Self::push_or_replace_entry(
                    package_versioned_ref_entry,
                    &unique_id,
                    &relation,
                    status,
                    true,
                );
            } else if !package_versioned_ref_entry
                .iter()
                .any(|(id, _, _, _)| id == &unique_id)
            {
                package_versioned_ref_entry.push((
                    unique_id.to_string(),
                    relation.clone(),
                    status,
                    None,
                ));
            }
        }
        Ok(())
    }

    /// Insert or overwrite a function() from a node into the functions map
    fn insert_function(
        &mut self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: AdapterType,
        status: ModelStatus,
    ) -> FsResult<()> {
        let package_name = &node.package_name();
        let function_name = node.name();
        let unique_id = node.unique_id();

        // For functions, create a FunctionObject that renders function calls
        let function_object = create_function_object_from_node(adapter_type, node)?.into_value();

        // Lookup by function name
        let function_entry = self.functions.entry(function_name.clone()).or_default();
        Self::push_or_replace_function_entry(
            function_entry,
            &unique_id,
            &function_object,
            status,
            true,
        );

        // Lookup by package and function name
        let package_function_entry = self
            .functions
            .entry(format!("{package_name}.{function_name}"))
            .or_default();
        Self::push_or_replace_function_entry(
            package_function_entry,
            &unique_id,
            &function_object,
            status,
            true,
        );
        Ok(())
    }

    /// Insert a source into the refs and sources map
    fn insert_source(
        &mut self,
        package_name: &str,
        source: &DbtSource,
        adapter_type: AdapterType,
        status: ModelStatus,
    ) -> FsResult<()> {
        // Build base relation and apply sample remapping if configured
        let mut database = source.base().database.clone();
        let mut schema = source.base().schema.clone();
        let mut identifier = source.base().alias.clone();
        let mapper = &self.renaming;
        if mapper.contains_key(&source.unique_id()) {
            // When a plan is present, remap all sources.
            (database, schema, identifier) = mapper[&source.unique_id()].clone();
        }

        let base_rel = create_relation_internal(
            adapter_type,
            database,
            schema,
            Some(identifier),
            None,
            source.quoting(),
        )?;
        let relation = RelationObject::new_with_filter(
            base_rel,
            self.run_filter.clone(),
            source.deprecated_config.event_time.clone(),
        )
        .into_value();

        self.sources
            .entry(format!(
                "{}.{}.{}",
                package_name,
                source.__source_attr__.source_name,
                source.common().name
            ))
            .or_default()
            .push((source.common().unique_id.clone(), relation.clone(), status));
        self.sources
            .entry(format!(
                "{}.{}",
                source.__source_attr__.source_name,
                source.common().name
            ))
            .or_default()
            .push((source.common().unique_id.clone(), relation, status));
        Ok(())
    }

    /// Lookup a ref by package name, model name, and optional version
    fn lookup_ref(
        &self,
        maybe_package_name: &Option<String>,
        name: &str,
        version: &Option<String>,
        maybe_node_package_name: &Option<String>,
    ) -> FsResult<RefRecord> {
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
                .map(|v| format!(".v{v}"))
                .unwrap_or_default()
        );
        let mut enabled_ref: Option<RefRecord> = None;
        let mut disabled_ref: Option<RefRecord> = None;
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
                    .partition(|(_, _, status, _)| *status != ModelStatus::Disabled);
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
                                .map(|(r, _, _, _)| format!("'{r}'"))
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
        let source_table_name = format!("{source_name}.{table_name}");
        let project_source_name = format!("{package_name}.{source_table_name}");
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
                        .map(|(r, _, _)| format!("'{r}'"))
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

    /// Lookup a function by package name and function name
    fn lookup_function(
        &self,
        maybe_package_name: &Option<String>,
        function_name: &str,
        maybe_node_package_name: &Option<String>,
    ) -> FsResult<(String, MinijinjaValue, ModelStatus)> {
        // Create a list of packages to search, where None means to
        // search non-package limited names
        let root_package = Some(self.root_package_name.clone());
        let search_packages = match (maybe_package_name, maybe_node_package_name) {
            // If maybe_package_name is specified, only search that package
            (Some(_), _) => vec![maybe_package_name],
            // If maybe_node_package_name is specified, and this is the root package,
            // search this package and the global functions
            (None, Some(node_pkg)) if *node_pkg == self.root_package_name => {
                vec![&root_package, &None]
            }
            // If maybe_node_package_name is specified, and this is not the root package,
            // search this package, the root package, and then finally global functions
            (None, Some(_)) => vec![maybe_node_package_name, &root_package, &None],
            // If maybe_package_name and maybe_node_package_name are not specified,
            // search only the global functions
            (None, None) => vec![&None],
        };

        let mut enabled_function: Option<(String, MinijinjaValue, ModelStatus)> = None;
        let mut disabled_function: Option<(String, MinijinjaValue, ModelStatus)> = None;
        let mut search_function_names: Vec<String> = Vec::new();

        for maybe_package in search_packages.iter() {
            // If this is a package, use the package name + function_name to search
            let search_function_name = if let Some(package_name) = maybe_package {
                format!("{}.{}", package_name.clone(), function_name)
            } else {
                // If this is not a package, just use the function_name to search
                function_name.to_string()
            };
            search_function_names.push(search_function_name.clone());

            if let Some(res) = self.functions.get(&search_function_name) {
                let (enabled_functions, disabled_functions): (Vec<_>, Vec<_>) = res
                    .iter()
                    .partition(|(_, _, status)| *status != ModelStatus::Disabled);

                // We got a function or we wouldn't be here
                if !disabled_functions.is_empty() {
                    disabled_function = Some(disabled_functions[0].clone());
                }

                match enabled_functions.len() {
                    // If there is one enabled function, use it
                    1 => {
                        enabled_function = Some(enabled_functions[0].clone());
                        break;
                    }
                    n if n > 1 => {
                        // More than one enabled function with the same name, issue error
                        return err!(
                            ErrorCode::InvalidConfig,
                            "Found ambiguous function('{}') pointing to multiple nodes: [{}]",
                            function_name,
                            res.iter()
                                .map(|(r, _, _)| format!("'{r}'"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                    }
                    // If there are no enabled functions, continue to next package
                    _ => {}
                };
            }
        }

        // If function not found issue error
        match enabled_function {
            Some(function_result) => Ok(function_result),
            None => {
                if disabled_function.is_some() {
                    err!(
                        ErrorCode::DisabledDependency,
                        "Attempted to use disabled function '{}'",
                        function_name
                    )
                } else {
                    err!(
                        ErrorCode::InvalidConfig,
                        "Function '{}' not found in project. Searched for '{}'",
                        function_name,
                        search_function_names.join(", ")
                    )
                }
            }
        }
    }

    fn update_ref_with_deferral(
        &mut self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: AdapterType,
        is_frontier: bool,
    ) -> FsResult<()> {
        let package_name = &node.package_name();
        let model_name = node.name();
        let unique_id = node.unique_id();
        let (maybe_version, maybe_latest_version) = if node.resource_type() == NodeType::Model {
            (node.version(), node.latest_version())
        } else {
            (None, None)
        };

        let deferred_relation = RelationObject::new_with_filter(
            create_relation_from_node(adapter_type, node, Some(self.run_filter.clone()))?,
            self.run_filter.clone(),
            node.event_time(),
        )
        .into_value();

        if maybe_version == maybe_latest_version {
            let ref_entry = self.refs.entry(model_name.clone()).or_default();
            Self::set_deferred_relation(ref_entry, &unique_id, &deferred_relation, is_frontier);

            let package_ref_entry = self
                .refs
                .entry(format!("{package_name}.{model_name}"))
                .or_default();
            Self::set_deferred_relation(
                package_ref_entry,
                &unique_id,
                &deferred_relation,
                is_frontier,
            );
        }

        if let Some(version) = maybe_version {
            let model_name_with_version = format!("{model_name}.v{version}");
            let versioned_ref_entry = self
                .refs
                .entry(model_name_with_version.to_owned())
                .or_default();
            Self::set_deferred_relation(
                versioned_ref_entry,
                &unique_id,
                &deferred_relation,
                is_frontier,
            );

            let package_versioned_ref_entry = self
                .refs
                .entry(format!("{package_name}.{model_name_with_version}"))
                .or_default();
            Self::set_deferred_relation(
                package_versioned_ref_entry,
                &unique_id,
                &deferred_relation,
                is_frontier,
            );
        }

        Ok(())
    }

    fn compile_or_test(&self) -> bool {
        self.compile_or_test
    }
}

/// Resolve the dependencies for a model
/// Returns a set of node unique_ids that had resolution errors
#[allow(clippy::cognitive_complexity)]
pub fn resolve_dependencies(
    io: &IoArgs,
    nodes: &mut Nodes,
    disabled_nodes: &mut Nodes,
    operations: &mut dbt_schemas::state::Operations,
    node_resolver: &NodeResolver,
) -> HashSet<String> {
    let mut tests_to_disable = Vec::new();
    let mut nodes_with_errors = HashSet::new();

    // First pass: identify tests with disabled dependencies
    for node in nodes.iter_values_mut() {
        // Clone needed values first to avoid borrowing issues
        let node_path = node.common().path.clone();
        let node_package_name = node.package_name();
        let node_unique_id = node.unique_id();
        let is_test = node.is_test();

        let node_base = node.base_mut();

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
            match node_resolver.lookup_ref(
                package,
                name,
                &version.as_ref().map(|v| v.to_string()),
                node_package_name_value,
            ) {
                Ok((dependency_id, _, _, _)) => {
                    // Check for self-reference
                    if dependency_id == node_unique_id {
                        show_error!(
                            io,
                            fs_err!(
                                ErrorCode::CyclicDependency,
                                "Model '{}' cannot reference itself",
                                name
                            )
                            .with_location(location)
                        );
                    } else {
                        node_base.depends_on.nodes.push(dependency_id.clone());
                        node_base
                            .depends_on
                            .nodes_with_ref_location
                            .push((dependency_id, location));
                    }
                }
                Err(e) => {
                    // Check if this is a disabled dependency error
                    if is_test && e.code == ErrorCode::DisabledDependency {
                        has_disabled_dependency = true;
                    } else {
                        // Track this node as having an error (unresolved ref/source)
                        nodes_with_errors.insert(node_unique_id.clone());
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

            match node_resolver.lookup_source(&node_package_name, &source_name, &table_name) {
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
                        // Track this node as having an error (unresolved ref/source)
                        nodes_with_errors.insert(node_unique_id.clone());
                        show_error!(io, e.with_location(location));
                    }
                }
            };
        }

        // Check functions
        for DbtRef {
            name,
            package,
            version: _,
            location,
        } in node_base.functions.iter()
        {
            let location = if let Some(location) = location {
                location.clone().with_file(&node_path)
            } else {
                CodeLocation::default()
            };

            match node_resolver.lookup_function(node_package_name_value, name, package) {
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
                        // Track this node as having an error (unresolved function)
                        nodes_with_errors.insert(node_unique_id.clone());
                        show_error!(io, e.with_location(location));
                    }
                }
            };
        }

        if is_test && has_disabled_dependency {
            tests_to_disable.push(node_unique_id);
        }
    }

    // Second pass: move disabled tests to disabled_nodes
    for test_id in &tests_to_disable {
        if let Some(node) = nodes.tests.remove(test_id) {
            disabled_nodes.tests.insert(test_id.clone(), node);
        }
    }

    // Process operations (on_run_start and on_run_end)
    [&mut operations.on_run_start, &mut operations.on_run_end]
        .iter_mut()
        .for_each(|ops| {
            ops.iter_mut().for_each(|operation_spanned| {
                let mut operation = (**operation_spanned).clone();
                let operation_package = operation.__common_attr__.package_name.clone();
                let operation_unique_id = operation.__common_attr__.unique_id.clone();

                // Process refs
                operation.__base_attr__.refs.iter().for_each(|dbt_ref| {
                    let location = dbt_ref
                        .location
                        .as_ref()
                        .map_or_else(CodeLocation::default, |loc| {
                            loc.clone().with_file(&operation.__common_attr__.path)
                        });

                    match node_resolver.lookup_ref(
                        &dbt_ref.package,
                        &dbt_ref.name,
                        &dbt_ref.version.as_ref().map(|v| v.to_string()),
                        &Some(operation_package.clone()),
                    ) {
                        Ok((dependency_id, _, _, _)) => {
                            operation
                                .__base_attr__
                                .depends_on
                                .nodes
                                .push(dependency_id.clone());
                            operation
                                .__base_attr__
                                .depends_on
                                .nodes_with_ref_location
                                .push((dependency_id, location));
                        }
                        Err(e) => {
                            nodes_with_errors.insert(operation_unique_id.clone());
                            show_error!(io, e.with_location(location));
                        }
                    }
                });

                // Process sources
                operation
                    .__base_attr__
                    .sources
                    .iter()
                    .filter(|source_wrapper| source_wrapper.source.len() == 2)
                    .for_each(|source_wrapper| {
                        let source_name = &source_wrapper.source[0];
                        let table_name = &source_wrapper.source[1];
                        let location = source_wrapper
                            .location
                            .as_ref()
                            .map_or_else(CodeLocation::default, |loc| {
                                loc.clone().with_file(&operation.__common_attr__.path)
                            });

                        match node_resolver.lookup_source(
                            &operation_package,
                            source_name,
                            table_name,
                        ) {
                            Ok((dependency_id, _, _)) => {
                                operation.__base_attr__.depends_on.nodes.push(dependency_id);
                            }
                            Err(e) => {
                                nodes_with_errors.insert(operation_unique_id.clone());
                                show_error!(io, e.with_location(location));
                            }
                        }
                    });

                // Replace with updated operation
                *operation_spanned = operation_spanned.clone().map(|_| operation);
            });
        });

    // Return the set of nodes that had resolution errors
    nodes_with_errors
}

/// Create a FunctionObject from a node (specifically for dbt functions)
pub fn create_function_object_from_node(
    adapter_type: AdapterType,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<FunctionObject> {
    let relation = create_relation_internal(
        adapter_type,
        node.database(),
        node.schema(),
        Some(node.base().alias.clone()),
        Some(RelationType::from(node.materialized())),
        node.quoting(),
    )?;

    // Create the qualified function name
    let rendered = relation.render_self_as_str();
    Ok(FunctionObject::new(rendered))
}
