//! Utility functions for the resolver
use crate::resolve::resolve_properties::MinimalPropertiesEntry;
use dbt_common::io_args::IoArgs;
use dbt_common::{ErrorCode, FsError, FsResult, fs_err, show_error};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_jinja_utils::utils::{generate_component_name, generate_relation_name};
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::common::{DbtMaterialization, ResolvedQuoting, normalize_quoting};
use dbt_schemas::schemas::project::DefaultTo;
use dbt_schemas::schemas::properties::ModelProperties;
use minijinja::compiler::ast::{MacroKind, Stmt};
use minijinja::compiler::parser::Parser;
use minijinja::machinery::WhitespaceConfig;
use minijinja::syntax::SyntaxConfig;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
/// Coalesce a list of optional values into a single value
pub fn coalesce<T: Clone>(values: Vec<Option<T>>) -> Option<T> {
    for value in values {
        if value.is_some() {
            return value;
        }
    }
    None
}

/// generate the unique id for a model (can be made more extensible for each type of node)
pub fn get_unique_id(
    model_name: &str,
    package_name: &str,
    version: Option<String>,
    node_type: &str,
) -> String {
    if let Some(version) = version {
        format!("{node_type}.{package_name}.{model_name}.v{version}")
    } else {
        format!("{node_type}.{package_name}.{model_name}")
    }
}

/// generate the fqn
pub fn get_node_fqn(
    package_name: &str,
    original_file_path: PathBuf,
    fqn_components: Vec<String>,
) -> Vec<String> {
    let mut fqn = vec![package_name.to_owned()];

    let mut components = if let Some(parent) = original_file_path.parent() {
        parent.components().collect::<Vec<_>>()
    } else {
        original_file_path.components().collect::<Vec<_>>()
    };

    // Remove the first component, which is the root directory
    if !components.is_empty() {
        components.remove(0);
    }
    for component in components {
        let component_str = component.as_os_str().to_str().unwrap().to_string();
        fqn.push(component_str);
    }

    for fqn_component in fqn_components {
        fqn.push(fqn_component.to_string());
    }
    fqn
}

// TODO: Versions need to have explicit params (not just additional_properties)
// TODO: We need to propgate column test logic correctly for versions
/// Split schema model object to multiple versions if provided
pub fn split_versions(models: Vec<&ModelProperties>) -> Vec<ModelProperties> {
    let mut flattened_models = Vec::new();
    for model in models {
        if let Some(versions) = &model.versions {
            for version in versions {
                let mut new_model = model.clone();
                new_model.name = format!("{}_v{}", model.name, version.v);
                flattened_models.push(new_model);
            }
        } else {
            flattened_models.push(model.clone());
        }
    }
    flattened_models
}

/// Returns the original or relative file path for a dbt asset.
///
/// If `base_path` differs from `in_dir`, attempts to compute a relative path
/// from `base_path.join(sub_path)` to `in_dir`. If that fails, returns `sub_path`.
/// Otherwise, if `base_path` equals `in_dir`, returns `sub_path` directly.
pub fn get_original_file_path(
    base_path: &PathBuf,
    in_dir: &PathBuf,
    sub_path: &PathBuf,
) -> PathBuf {
    if base_path != in_dir {
        pathdiff::diff_paths(base_path.join(sub_path), in_dir)
            .unwrap_or_else(|| sub_path.to_owned())
    } else {
        sub_path.to_owned()
    }
}

/// Prepares package dependencies for resolution and sets thread local dependencies.
///
/// This function:
/// 1. Collects all package names
/// 2. Builds a dependency map for topological sorting
/// 3. Creates a comprehensive dependency map for thread local storage
/// 4. Sets the thread local dependencies
/// 5. Returns the packages in topological order
///
/// # Arguments
/// * `dbt_state` - The current DBT state containing packages and dependencies
///
/// # Returns
/// A vector of package names in topological order for processing
pub fn prepare_package_dependency_levels(
    dbt_state: Arc<dbt_schemas::state::DbtState>,
) -> Vec<Vec<String>> {
    // Build dependency map (similar to dbt's load_dependencies)
    let dependency_map = dbt_state
        .packages
        .iter()
        .map(|p| (p.dbt_project.name.clone(), p.dependencies.clone()))
        .collect::<BTreeMap<_, _>>();
    // dbg!(dependency_map.clone());

    // Return packages in topological order
    dbt_dag::deps_mgmt::topological_levels(&dependency_map)
}
/// Register a resource definition for a model
pub fn prepare_package_dependencies(dbt_state: Arc<dbt_schemas::state::DbtState>) -> Vec<String> {
    // Build dependency map (similar to dbt's load_dependencies)
    let dependency_map = dbt_state
        .packages
        .iter()
        .map(|p| (p.dbt_project.name.clone(), p.dependencies.clone()))
        .collect::<BTreeMap<_, _>>();

    // Return packages in topological order
    dbt_dag::deps_mgmt::topological_sort(&dependency_map)
}

/// Register a duplicate resource definition for a model
pub fn register_duplicate_resource(
    mpe: &MinimalPropertiesEntry,
    node_name: &str,
    node_type: &str,
    duplicate_collector: &mut Vec<FsError>,
) {
    let mut all_dup_paths: BTreeSet<PathBuf> = mpe.duplicate_paths.clone().into_iter().collect();
    all_dup_paths.insert(mpe.relative_path.clone());

    let err_msg = format!(
        "Found duplicate resource definitions for {} named '{}' in [{}]",
        node_type,
        node_name,
        all_dup_paths
            .iter()
            .map(|p| format!("'{}'", p.display()))
            .collect::<Vec<_>>()
            .join(", ")
    );
    duplicate_collector.push(
        *fs_err!(code => ErrorCode::InvalidConfig, loc => mpe.relative_path.clone(), "{}", err_msg),
    );
}

/// Trigger duplicate errors
pub fn trigger_duplicate_errors(io: &IoArgs, duplicate_errors: &mut Vec<FsError>) -> FsResult<()> {
    if !duplicate_errors.is_empty() {
        while let Some(err_msg) = duplicate_errors.pop() {
            if duplicate_errors.is_empty() {
                return Err(Box::new(err_msg));
            } else {
                show_error!(io, Box::new(err_msg));
            }
        }
    }
    Ok(())
}

/// Generate relation components (database, schema, alias) and relation name
/// Returns components that can be used to update a node
/// https://github.com/dbt-labs/dbt-core/blob/a1958c119399f765ad43e49b8b12c88cf3ec1245/core/dbt/parser/base.py#L287
pub fn generate_relation_components(
    env: &JinjaEnv,
    root_project_name: &str,
    current_project_name: &str,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    components: &RelationComponents,
    node: &dyn InternalDbtNodeAttributes,
    adapter_type: &str,
) -> FsResult<(String, String, String, String, ResolvedQuoting)> {
    // Determine node type
    let is_snapshot = node.resource_type() == "snapshot";
    // TODO handle jinja rendering errors on each component name rendering
    // Get default values from the node
    let (default_database, default_schema, default_alias) = (
        node.base().database.clone(),
        node.base().schema.clone(),
        node.base().alias.clone(),
    );
    // Generate database name
    let database = if is_snapshot && components.database.is_some() {
        components.database.clone().unwrap()
    } else {
        generate_component_name(
            env,
            "database",
            root_project_name,
            current_project_name,
            base_ctx,
            components.database.clone(),
            Some(node),
        )
        .unwrap_or_else(|_| default_database.to_owned()) // todo handle this error
    };

    // Generate schema name
    let schema = if is_snapshot && components.schema.is_some() {
        components.schema.clone().unwrap()
    } else {
        generate_component_name(
            env,
            "schema",
            root_project_name,
            current_project_name,
            base_ctx,
            components.schema.clone(),
            Some(node),
        )
        .unwrap_or_else(|_| default_schema.to_owned()) // todo handle this error
    };

    // Generate alias
    let alias = generate_component_name(
        env,
        "alias",
        root_project_name,
        current_project_name,
        base_ctx,
        components.alias.clone(),
        Some(node),
    )
    .unwrap_or_else(|_| default_alias.to_owned()); // todo handle this error

    let (database, schema, alias, quoting) =
        normalize_quoting(&node.quoting(), adapter_type, &database, &schema, &alias);

    // Only generate relation_name if not ephemeral
    let parse_adapter = env
        .get_parse_adapter()
        .expect("Failed to get parse adapter");
    let database_name = if !matches!(node.materialized(), DbtMaterialization::Ephemeral) {
        database.as_str()
    } else {
        &format!("{database}_ephemeral")
    };
    let schema_name = if !matches!(node.materialized(), DbtMaterialization::Ephemeral) {
        schema.as_str()
    } else {
        &format!("{schema}_ephemeral")
    };
    let alias_name = if !matches!(node.materialized(), DbtMaterialization::Ephemeral) {
        alias.as_str()
    } else {
        &format!("{alias}_ephemeral")
    };
    let relation_name = generate_relation_name(
        parse_adapter,
        database_name,
        schema_name,
        alias_name,
        quoting,
    )?;

    Ok((database, schema, alias, relation_name, quoting))
}

/// Relation components for a node
pub struct RelationComponents {
    /// The database name
    pub database: Option<String>,
    /// The schema name
    pub schema: Option<String>,
    /// The alias name
    pub alias: Option<String>,
    /// Whether to store failures
    pub store_failures: Option<bool>,
}

/// Updates a InternalDbtNode with generated relation components (database, schema, alias, relation_name)
///
/// This consolidates a common pattern across resolver modules.
pub fn update_node_relation_components(
    node: &mut dyn InternalDbtNodeAttributes,
    jinja_env: &JinjaEnv,
    root_project_name: &str,
    package_name: &str,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    components: &RelationComponents,
    adapter_type: &str,
) -> FsResult<()> {
    // Source and unit test nodes do not have relation components
    if ["source", "unit_test"].contains(&node.resource_type()) {
        return Ok(());
    }
    let (database, schema, alias, relation_name, quoting) = generate_relation_components(
        jinja_env,
        root_project_name,
        package_name,
        base_ctx,
        components,
        node,
        adapter_type,
    )?;
    {
        let base_attr = node.base_mut();

        base_attr.database = database;
        base_attr.schema = schema;
        node.set_quoting(quoting);
    }

    // Only set relation_name for:
    // - Test nodes with store_failures=true
    // - Nodes that are relational and not ephemeral models
    if node.resource_type() == "test" {
        if let Some(store_failures) = components.store_failures {
            if store_failures {
                let base_attr = node.base_mut();
                base_attr.relation_name = Some(relation_name);
            }
        }
    } else {
        // Check if node is relational and not ephemeral
        let is_ephemeral = matches!(node.materialized(), DbtMaterialization::Ephemeral);
        if !is_ephemeral {
            let base_attr = node.base_mut();
            base_attr.relation_name = Some(relation_name);
        }
    }

    let base_attr = node.base_mut();
    base_attr.alias = alias;
    Ok(())
}

/// A no-op config for the [parse_macro_statements] function
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct NoOpConfig {}

impl DefaultTo<NoOpConfig> for NoOpConfig {
    fn default_to(&mut self, _other: &Self) {
        // no-op
    }
}

/// Parse the macro sql and return the [SqlResource]s macro wrappers that are
/// observed during the rendering phase.
/// path is the path relative to the in_dir
pub fn parse_macro_statements(
    sql: &str,
    path: &Path,
    statement_types: &[&str],
) -> FsResult<Vec<SqlResource<NoOpConfig>>> {
    let file_name = path.display().to_string();
    let mut parser = Parser::new(
        sql,
        &file_name,
        false,
        #[allow(clippy::default_constructed_unit_structs)]
        SyntaxConfig::builder().build().unwrap(),
        WhitespaceConfig::default(),
    );
    // We should throw an error here if we can't process the macro because we shouldn't see any non macro's here
    let ast = parser
        .parse_top_level_statements(statement_types)
        .map_err(|e| FsError::from_jinja_err(e, "Failed to parse macro SQL"))?;
    let mut sql_resources = Vec::new();
    parse_macro_ast(&ast, &mut sql_resources);
    Ok(sql_resources)
}

fn parse_macro_ast<T: DefaultTo<T>>(ast: &Stmt, sql_resources: &mut Vec<SqlResource<T>>) {
    match ast {
        Stmt::Macro((macro_node, macro_kind, meta)) => {
            let span = macro_node.span;
            let macro_name = macro_node.name;
            match macro_kind {
                MacroKind::Macro => {
                    sql_resources.push(SqlResource::Macro(macro_name.to_string(), span));
                }
                MacroKind::Test => {
                    sql_resources.push(SqlResource::Test(macro_name.to_string(), span));
                }
                MacroKind::Doc => {
                    sql_resources.push(SqlResource::Doc(macro_name.to_string(), span));
                }
                MacroKind::Snapshot => {
                    sql_resources.push(SqlResource::Snapshot(macro_name.to_string(), span));
                }
                MacroKind::Materialization => {
                    let adapter_type = meta.get("adapter").expect("adapter is required");
                    sql_resources.push(SqlResource::Materialization(
                        macro_name.to_string(),
                        adapter_type.as_str().unwrap().to_string(),
                        span,
                    ));
                }
            }
            // recursively parse the body of the macro for nested macros
            for stmt in &macro_node.body {
                parse_macro_ast(stmt, sql_resources);
            }
        }
        Stmt::Template(template_stmt) => {
            template_stmt
                .children
                .iter()
                .for_each(|x| parse_macro_ast(x, sql_resources));
        }
        _ => {}
    }
}

/// Convert macro names to unique IDs
/// For now, we'll use a simple heuristic to determine the package name
/// In the future, this should be improved to look up macros in the macro registry
pub fn convert_macro_names_to_unique_ids(macro_calls: &HashSet<String>) -> Vec<String> {
    macro_calls
        .iter()
        .filter_map(|name| {
            // Check if the macro name already contains a package prefix
            if name.contains('.') {
                // It's already in the format package.macro_name
                Some(format!("macro.{name}"))
            } else {
                // If name doesn't contain '.', assume it's a function in context, don't collect
                None
            }
        })
        .collect()
}
