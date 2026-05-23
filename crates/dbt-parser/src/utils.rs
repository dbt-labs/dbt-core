//! Utility functions for the resolver
use crate::dbt_project_config::strip_resource_paths_from_ref_path;
use crate::resolve::resolve_properties::MinimalPropertiesEntry;
use dbt_adapter_core::AdapterType;
use dbt_common::io_args::IoArgs;
use dbt_common::path::DbtPath;
use dbt_common::tracing::emit::emit_error_log_from_fs_error;
use dbt_common::{ErrorCode, FsError, FsResult, fs_err, stdfs};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_jinja_utils::utils::{generate_component_name, generate_relation_name};
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::common::{DbtMaterialization, ResolvedQuoting, normalize_quoting};
use dbt_schemas::schemas::project::{ResolvableConfig, ResolvedConfig};
use dbt_schemas::schemas::properties::ModelProperties;
use dbt_schemas::schemas::telemetry::NodeType;
use dbt_schemas::state::DbtPackage;
use minijinja::ArgSpec;
use minijinja::compiler::ast::{CallArg, Expr, MacroKind, Stmt};
use minijinja::compiler::parser::Parser;
use minijinja::machinery::{Span, WhitespaceConfig};
use minijinja::syntax::SyntaxConfig;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// A raw (unrendered) project config tree built from a `dbt_project.yml` models hierarchy.
/// Mirrors `DbtProjectConfig<T>` but stores raw `dbt_yaml::Value` so Jinja strings are preserved.
#[derive(Debug, Default)]
pub struct RawProjectConfig {
    /// Merged config values at this level of the hierarchy, keyed by config name (without `+` prefix).
    pub config: BTreeMap<String, dbt_yaml::Value>,
    /// Child nodes keyed by package/folder name, each with their own inherited config.
    pub children: BTreeMap<String, RawProjectConfig>,
}

impl RawProjectConfig {
    /// Returns an empty config tree with no config values and no children.
    pub fn empty() -> Self {
        Self {
            config: BTreeMap::new(),
            children: BTreeMap::new(),
        }
    }

    /// Returns the merged config for the deepest FQN component that exists in the tree.
    pub fn get_config_for_fqn(&self, fqn: &[String]) -> &BTreeMap<String, dbt_yaml::Value> {
        let mut cur = self;
        for component in fqn {
            if let Some(child) = cur.children.get(component.as_str()) {
                cur = child;
            } else {
                break;
            }
        }
        &cur.config
    }
}

/// Merges a parent raw config map with config keys from a child raw YAML mapping.
/// Keys prefixed with `+` are config keys (prefix stripped before inserting).
/// Non-`+` keys are hierarchy keys (package/folder names) and are ignored.
/// Child values overwrite parent values.
pub fn merge_raw_config_mappings(
    parent: &BTreeMap<String, dbt_yaml::Value>,
    child_mapping: &dbt_yaml::Mapping,
) -> BTreeMap<String, dbt_yaml::Value> {
    let mut merged = parent.clone();
    for (k, v) in child_mapping.iter() {
        if let Some(key_str) = k.as_str() {
            if let Some(stripped) = key_str.strip_prefix('+') {
                merged.insert(stripped.to_string(), v.clone());
            }
        }
    }
    merged
}

/// Recursively builds a `RawProjectConfig` tree from a raw YAML mapping.
/// At each level, `+`-prefixed keys are merged into the config; non-`+` keys with mapping values are recursed into as children.
pub fn recur_raw_project_config(
    mapping: &dbt_yaml::Mapping,
    parent_config: &BTreeMap<String, dbt_yaml::Value>,
) -> RawProjectConfig {
    let current_config = merge_raw_config_mappings(parent_config, mapping);
    let mut children = BTreeMap::new();
    for (k, v) in mapping.iter() {
        if let Some(key_str) = k.as_str() {
            if !key_str.starts_with('+') {
                if let Some(child_mapping) = v.as_mapping() {
                    children.insert(
                        key_str.to_string(),
                        recur_raw_project_config(child_mapping, &current_config),
                    );
                }
            }
        }
    }
    RawProjectConfig {
        config: current_config,
        children,
    }
}

/// Coalesce a list of optional values into a single value
pub fn coalesce<T: Clone>(values: Vec<Option<T>>) -> Option<T> {
    for value in values {
        if value.is_some() {
            return value;
        }
    }
    None
}

/// generate the unique id for a dbt resource (can be made more extensible for each type of node)
pub fn get_unique_id(
    resource_name: &str,
    package_name: &str,
    version: Option<String>,
    node_type: &str,
) -> String {
    if let Some(version) = version {
        format!("{node_type}.{package_name}.{resource_name}.v{version}")
    } else {
        format!("{node_type}.{package_name}.{resource_name}")
    }
}

/// generate the fqn
pub fn get_node_fqn(
    package_name: &str,
    original_file_path: PathBuf,
    fqn_components: Vec<String>,
    resource_paths: &[String],
) -> Vec<String> {
    let mut fqn = vec![package_name.to_owned()];

    // Strip resource paths from the file path
    let stripped_path = strip_resource_paths_from_ref_path(&original_file_path, resource_paths);

    let components = if let Some(parent) = stripped_path.parent() {
        parent.components().collect::<Vec<_>>()
    } else {
        stripped_path.components().collect::<Vec<_>>()
    };

    // Add path components to fqn (after stripping resource paths)
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
                let version_str = match &version.v {
                    dbt_yaml::Value::String(s, _) => s.clone(),
                    dbt_yaml::Value::Number(n, _) => n.to_string(),
                    _ => format!("{:?}", version.v),
                };
                new_model.name = format!("{}_v{}", model.name, version_str);
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
pub fn get_original_file_path(base_path: &Path, in_dir: &Path, sub_path: &Path) -> PathBuf {
    if base_path != in_dir {
        pathdiff::diff_paths(base_path.join(sub_path), in_dir)
            .unwrap_or_else(|| sub_path.to_owned())
    } else {
        sub_path.to_owned()
    }
}

/// Returns the contents of a file given an original_file_path and in_dir,
pub fn get_original_file_contents(in_dir: &Path, original_file_path: &PathBuf) -> Option<String> {
    let absolute_path = in_dir.join(original_file_path);
    stdfs::read_to_string(&absolute_path).ok()
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
        while let Some(err) = duplicate_errors.pop() {
            if duplicate_errors.is_empty() {
                return Err(Box::new(err));
            } else {
                emit_error_log_from_fs_error(&err, io.status_reporter.as_ref());
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
    adapter_type: AdapterType,
) -> FsResult<(String, String, String, String, ResolvedQuoting)> {
    // Get default values from the node
    let (default_database, default_schema) = (node.database(), node.schema());
    // Generate database name
    let database = if node.skip_generate_database_name_macro() {
        components.database.clone().unwrap_or(default_database)
    } else {
        generate_component_name(
            env,
            "database",
            root_project_name,
            current_project_name,
            base_ctx,
            components.database.clone(),
            Some(node),
        )?
    };

    // Generate schema name
    let schema = if node.skip_generate_schema_name_macro() {
        components.schema.clone().unwrap_or(default_schema)
    } else {
        generate_component_name(
            env,
            "schema",
            root_project_name,
            current_project_name,
            base_ctx,
            components.schema.clone(),
            Some(node),
        )?
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
    )?;

    // Ensure alias is never empty - use node name as ultimate fallback
    let alias = if alias.is_empty() {
        node.common().name.clone()
    } else {
        alias
    };

    let (database, schema, alias, quoting) =
        normalize_quoting(&node.quoting(), adapter_type, &database, &schema, &alias);

    // Only generate relation_name if not ephemeral
    let parse_adapter = env.get_adapter().expect("Failed to get parse adapter");
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

/// Generate only database and schema components.
/// This is the first step in a two-phase generation process that allows
/// generate_alias_name to access the computed schema via node.schema.
fn generate_database_and_schema(
    env: &JinjaEnv,
    root_project_name: &str,
    current_project_name: &str,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    components: &RelationComponents,
    node: &dyn InternalDbtNodeAttributes,
    adapter_type: AdapterType,
) -> FsResult<(String, String, ResolvedQuoting)> {
    let (default_database, default_schema) = (node.database(), node.schema());

    // Generate database name
    let database = if node.skip_generate_database_name_macro() {
        components.database.clone().unwrap_or(default_database)
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
        .unwrap_or_else(|_| default_database.to_owned())
    };

    // Generate schema name
    let schema = if node.skip_generate_schema_name_macro() {
        components.schema.clone().unwrap_or(default_schema)
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
        .unwrap_or_else(|_| default_schema.to_owned())
    };

    // Normalize quoting for database and schema (use empty alias for now, will be updated later)
    let (database, schema, _, quoting) =
        normalize_quoting(&node.quoting(), adapter_type, &database, &schema, "");

    Ok((database, schema, quoting))
}

/// Generate alias and relation_name after database and schema have been set on the node.
/// This is the second step in a two-phase generation process.
#[allow(clippy::too_many_arguments)]
fn generate_alias_and_relation_name(
    env: &JinjaEnv,
    root_project_name: &str,
    current_project_name: &str,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    components: &RelationComponents,
    node: &dyn InternalDbtNodeAttributes,
    adapter_type: AdapterType,
    database: &str,
    schema: &str,
    quoting: ResolvedQuoting,
) -> FsResult<(String, String)> {
    let default_alias = node.base().alias.clone();

    // Generate alias - node.schema is now set to the computed schema
    let alias = generate_component_name(
        env,
        "alias",
        root_project_name,
        current_project_name,
        base_ctx,
        components.alias.clone(),
        Some(node),
    )
    .unwrap_or_else(|_| {
        if default_alias.is_empty() {
            node.common().name.clone()
        } else {
            default_alias.to_owned()
        }
    });

    // Ensure alias is never empty
    let alias = if alias.is_empty() {
        node.common().name.clone()
    } else {
        alias
    };

    // Normalize quoting for alias
    let (_, _, alias, _) = normalize_quoting(&quoting, adapter_type, database, schema, &alias);

    // Generate relation_name
    let parse_adapter = env.get_adapter().expect("Failed to get parse adapter");
    let database_name = if !matches!(node.materialized(), DbtMaterialization::Ephemeral) {
        database
    } else {
        &format!("{database}_ephemeral")
    };
    let schema_name = if !matches!(node.materialized(), DbtMaterialization::Ephemeral) {
        schema
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

    Ok((alias, relation_name))
}

/// Relation components for a node
#[derive(Debug)]
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
///
/// Note: We generate and update database/schema BEFORE generating alias, so that
/// generate_alias_name macro can access the computed schema via node.schema.
/// This matches dbt-core behavior where custom alias macros can reference node.schema.
pub fn update_node_relation_components(
    node: &mut dyn InternalDbtNodeAttributes,
    jinja_env: &JinjaEnv,
    root_project_name: &str,
    package_name: &str,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    components: &RelationComponents,
    adapter_type: AdapterType,
) -> FsResult<()> {
    // Source and unit test nodes do not have relation components
    if [NodeType::Source, NodeType::UnitTest].contains(&node.resource_type()) {
        return Ok(());
    }

    // Step 1: Generate database and schema first, then update the node.
    // This ensures that when generate_alias_name is called, node.schema reflects
    // the computed schema (not the default profile schema).
    let (database, schema, quoting) = generate_database_and_schema(
        jinja_env,
        root_project_name,
        package_name,
        base_ctx,
        components,
        node,
        adapter_type,
    )?;

    // Update node with database and schema BEFORE generating alias
    {
        let base_attr = node.base_mut();
        base_attr.database = database.clone();
        base_attr.schema = schema.clone();
        node.set_quoting(quoting);
    }

    // Step 2: Now generate alias with the updated node (node.schema is now correct)
    let (alias, relation_name) = generate_alias_and_relation_name(
        jinja_env,
        root_project_name,
        package_name,
        base_ctx,
        components,
        node,
        adapter_type,
        &database,
        &schema,
        quoting,
    )?;

    // Only set relation_name for:
    // - Test nodes with store_failures=true
    // - Nodes that are relational and not ephemeral models
    if node.resource_type() == NodeType::Test {
        if let Some(store_failures) = components.store_failures
            && store_failures
        {
            let base_attr = node.base_mut();
            base_attr.relation_name = Some(relation_name);
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

/// Extracts a resource type subtree from a raw `dbt_project.yml` into a RawProjectConfig struct for building unrendered configs.
pub fn extract_resource_config_from_raw_project(
    raw_yml: &dbt_yaml::Value,
    resource_type: &str,
) -> RawProjectConfig {
    if let Some(raw_subtree) = raw_yml.get(resource_type).cloned().and_then(|v| {
        if let dbt_yaml::Value::Mapping(m, _) = v {
            Some(m)
        } else {
            None
        }
    }) {
        recur_raw_project_config(&raw_subtree, &BTreeMap::new())
    } else {
        RawProjectConfig::empty()
    }
}

/// Statically parses the raw (unrendered) kwargs from a `{{ config(...) }}` call in a SQL file.
/// Uses the minijinja AST and byte-offset spans to extract the raw source text for each kwarg,
/// preserving Jinja expressions as-is. Returns None if no config call is found.
/// Reference: https://github.com/dbt-labs/dbt-mantle/blob/da5abca4f829b167bd1b1d5c6666c12cd8c719c0/core/dbt/clients/jinja_static.py#L205
///
/// WARNING: This performs a duplicate AST parse. minijinja does not expose the AST after compilation, so we need to figure out a way to reuse the parsed AST instead of performing a full parse again.
pub fn parse_unrendered_config(
    sql: &str,
    snapshot: bool,
) -> Option<BTreeMap<String, dbt_yaml::Value>> {
    use minijinja::compiler::tokens::Span;
    use minijinja::value::ValueKind;

    fn expr_span<'a>(expr: &Expr<'a>) -> Option<Span> {
        Some(match expr {
            Expr::Var(s) => s.span,
            Expr::Call(s) => s.span,
            Expr::BinOp(s) => s.span,
            Expr::UnaryOp(s) => s.span,
            Expr::IfExpr(s) => s.span,
            Expr::Filter(s) => s.span,
            Expr::Test(s) => s.span,
            Expr::GetAttr(s) => s.span,
            Expr::GetItem(s) => s.span,
            Expr::List(s) => s.span,
            Expr::Map(s) => s.span,
            Expr::Tuple(s) => s.span,
            Expr::Slice(s) => s.span,
            Expr::Const(s) => s.span,
        })
    }

    let mut parser = Parser::new(
        sql,
        "",
        false,
        #[allow(clippy::default_constructed_unit_structs)]
        SyntaxConfig::builder().build().unwrap(),
        WhitespaceConfig::default(),
    );
    let ast = parser.parse().ok()?;

    fn find_config_call<'a>(stmt: &'a Stmt<'a>, snapshot: bool) -> Option<&'a Vec<CallArg<'a>>> {
        match stmt {
            Stmt::Template(t) => t
                .children
                .iter()
                .find_map(|s| find_config_call(s, snapshot)),
            Stmt::EmitExpr(e) => {
                if let Expr::Call(call) = &e.expr {
                    if let Expr::Var(var) = &call.expr {
                        if var.id == "config" {
                            return Some(&call.args);
                        }
                    }
                }
                None
            }
            Stmt::Macro((macro_node, MacroKind::Snapshot, _)) if snapshot => macro_node
                .body
                .iter()
                .find_map(|s| find_config_call(s, snapshot)),
            _ => None,
        }
    }

    let args = find_config_call(&ast, snapshot)?;
    let sql_bytes = sql.as_bytes();

    let mut map = BTreeMap::new();
    for arg in args {
        if let CallArg::Kwarg(name, expr) = arg {
            // For Expr::Const, use the parsed literal value so the correct type is
            // preserved.
            // For all other expressions, preserve the raw Jinja source text as a string.
            let yml_val: Option<dbt_yaml::Value> = if let Expr::Const(c) = expr {
                match c.value.kind() {
                    ValueKind::String => c
                        .value
                        .as_str()
                        .map(|s| dbt_yaml::Value::string(s.to_string())),
                    ValueKind::Bool => Some(dbt_yaml::Value::bool(c.value.is_true())),
                    ValueKind::Number => c
                        .value
                        .as_i64()
                        .map(|n| dbt_yaml::Value::number(n.into()))
                        .or_else(|| {
                            f64::try_from(c.value.clone())
                                .ok()
                                .map(|f| dbt_yaml::Value::number(f.into()))
                        }),
                    ValueKind::None => Some(dbt_yaml::Value::null()),
                    _ => None,
                }
            } else {
                expr_span(expr).and_then(|span| {
                    let start = span.start_offset as usize;
                    let end = span.end_offset as usize;
                    std::str::from_utf8(&sql_bytes[start..end]).ok().map(|raw| {
                        dbt_yaml::Value::String(raw.trim().to_string(), Default::default())
                    })
                })
            };
            if let Some(val) = yml_val {
                map.insert((*name).to_string(), val);
            }
        }
    }

    if map.is_empty() { None } else { Some(map) }
}

/// A no-op config for the [parse_macro_statements] function
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct NoOpConfig {}

impl ResolvedConfig for NoOpConfig {
    fn enabled(&self) -> bool {
        true
    }
}

impl ResolvableConfig<NoOpConfig> for NoOpConfig {
    type Resolved = Self;
    type PackageDefaults = ();
    type ResolveDefaults = ();

    fn default_to(&mut self, _other: &Self) {}

    fn get_enabled_with_default(&self) -> bool {
        true
    }

    fn disable(&mut self) {}

    fn apply_package_defaults(&mut self, _: ()) {}

    fn finalize(self) -> Self {
        self
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
    let mut last_func_sign = None;
    extract_sql_resources_from_ast(&ast, &mut sql_resources, &mut last_func_sign);
    Ok(sql_resources)
}

fn extract_sql_resources_from_ast<T: ResolvableConfig<T>>(
    ast: &Stmt,
    sql_resources: &mut Vec<SqlResource<T>>,
    last_func_sign: &mut Option<(Span, String)>,
) {
    match ast {
        Stmt::Macro((macro_node, macro_kind, meta)) => {
            let span = macro_node.span;
            let macro_name = macro_node.name;
            let func_sign = if let Some((span, func_sign)) = last_func_sign.take() {
                if span.start_line >= macro_node.span.start_line {
                    panic!("[BUG] funcsign is after macro declaration");
                }
                Some(func_sign)
            } else {
                None
            };
            let non_optional_args_len = macro_node.args.len() - macro_node.defaults.len();
            let args = macro_node
                .args
                .iter()
                .enumerate()
                .map(|(i, arg)| match arg {
                    Expr::Var(spanned) => ArgSpec {
                        name: spanned.id.to_string(),
                        is_optional: i >= non_optional_args_len,
                    },
                    _ => todo!(),
                })
                .collect::<Vec<_>>();
            match macro_kind {
                MacroKind::Macro => {
                    sql_resources.push(SqlResource::Macro(
                        macro_name.to_string(),
                        span,
                        func_sign,
                        args,
                        macro_node.name_span,
                    ));
                }
                MacroKind::Test => {
                    sql_resources.push(SqlResource::Test(
                        macro_name.to_string(),
                        span,
                        args,
                        macro_node.name_span,
                    ));
                }
                MacroKind::Doc => {
                    if let Some(Stmt::EmitRaw(emit_raw)) = macro_node.body.first() {
                        sql_resources.push(SqlResource::Doc(macro_name.to_string(), emit_raw.span));
                    }
                }
                MacroKind::Snapshot => {
                    sql_resources.push(SqlResource::Snapshot(
                        macro_name.to_string(),
                        span,
                        macro_node.name_span,
                    ));
                }
                MacroKind::Materialization => {
                    let adapter_type = meta.get("adapter").expect("adapter is required");
                    sql_resources.push(SqlResource::Materialization(
                        macro_name.to_string(),
                        adapter_type.as_str().unwrap().to_string(),
                        span,
                        macro_node.name_span,
                    ));
                }
            }
            // recursively parse the body of the macro for nested macros
            for stmt in &macro_node.body {
                extract_sql_resources_from_ast(stmt, sql_resources, last_func_sign);
            }
        }
        Stmt::Template(template_stmt) => {
            template_stmt
                .children
                .iter()
                .for_each(|x| extract_sql_resources_from_ast(x, sql_resources, last_func_sign));
        }
        Stmt::EmitRaw(emit_raw) => {
            // find "-- funcsign: " in emit_raw.raw
            let raw = emit_raw.raw.trim();
            if raw.contains("-- funcsign: ") {
                *last_func_sign = Some((
                    emit_raw.span,
                    raw.split("-- funcsign: ")
                        .nth(1)
                        .unwrap()
                        .trim()
                        .to_string(),
                ));
            } else {
                *last_func_sign = None;
            }
        }
        _ => {}
    }
}

/// Clear the diagnostics for a package
pub fn clear_package_diagnostics(io: &IoArgs, package: &DbtPackage) {
    if let Some(status_reporter) = &io.status_reporter {
        let mut file_paths = Vec::new();

        // 1. Add dbt_project.yml if it exists
        let project_file_path = package.package_root_path.join("dbt_project.yml");
        if project_file_path.exists() {
            // Get the relative path to the workspace root (arg.io.in_dir)
            if let Ok(workspace_path) = stdfs::diff_paths(&project_file_path, &io.in_dir) {
                file_paths.push(DbtPath::from_path(io.in_dir.join(workspace_path)));
            }
        }

        // 2. Add dbt_properties files (schema.yml, etc.), macro_files, and docs_files
        for asset in package
            .dbt_properties
            .iter()
            .chain(&package.macro_files)
            .chain(&package.docs_files)
        {
            let file_path = io.in_dir.join(&asset.path);
            file_paths.push(DbtPath::from_path(file_path));
        }

        // Use bulk operation for better performance
        if !file_paths.is_empty() {
            status_reporter.bulk_publish_empty(file_paths);
        }
    }
}
