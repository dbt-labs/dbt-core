//! Module defines the global project configuration, which is used to
//! load and propagate configuration properties from the root `dbt_project.yml`
//! to the individual model directories.

use std::path::{Path, PathBuf};

use dbt_common::warn_error_options::project_flags_get_value;
use dbt_common::{ErrorCode, FsError};
use indexmap::IndexMap;

use dbt_common::{
    FsResult,
    tracing::dbt_emit::{emit_strict_parse_error, emit_warn_log_message},
};
use dbt_schemas::schemas::{common::DbtQuoting, project::DbtProject};
use dbt_schemas::schemas::{
    project::{
        AnalysesConfig, DataTestConfig, ExposureConfig, FunctionConfig, MetricConfig, ModelConfig,
        ResolvableConfig, SavedQueryConfig, SeedConfig, SemanticModelConfig, SnapshotConfig,
        SourceConfig, TypedRecursiveConfig, UnitTestConfig,
    },
    serde::yaml_to_fs_error,
};
use dbt_yaml::ShouldBe;

/// Used to deserialize the top-level `dbt_project.yml` configuration
/// for `models`, `data_tests`, `seeds` etc..
///
/// ```yaml
/// models:
///   dbt_jinja(project_name):
///     adapter(folder_name in project):
///       +schema: 'dbt_jinja'
///       get_relation_cache:
///       +alias: 'dbt_jinja'
/// ```
///
/// This configuration is path based, meaning each key that is not a
/// property of it's configuration <T> is the name of a directory, which may have
/// source files or apply additional configuration. Configuration precedence
/// is given to the most specific path configuration. All unspecified
/// configuration is inherited from the parent.
///
#[derive(Debug, Clone)]
pub struct DbtProjectConfig<T: ResolvableConfig<T>> {
    /// The root configuration (i.e. at the `dbt_project.yml` level or inherited from `profiles.yml`)
    pub config: T,
    /// Child configuration applied by path part (preserves insertion order like Python dicts)
    pub children: IndexMap<String, DbtProjectConfig<T>>,
}

impl<T: ResolvableConfig<T>> DbtProjectConfig<T> {
    /// Create a new [GlobalProjectConfig] from a default configuration and the root dbt_project.yml [DbtProjectConfigs]
    pub fn try_new<S: Into<T> + TypedRecursiveConfig>(
        dbt_config: &T,
        configs: &S,
        dependency_package_name: Option<&str>,
        disallow_plus_prefix: bool,
    ) -> FsResult<Self>
    where
        T: PartialEq,
    {
        let on_error = |variant: &ShouldBe<S>, key: &str, key_path: &str| {
            let key_path = if key_path.is_empty() {
                key.to_string()
            } else {
                format!("{key_path}.{key}")
            };
            match variant {
                ShouldBe::AndIs(_) => {
                    // If we're calling `on_error` on a valid key, it must start with `+`.
                    let detail = format!(
                        "Unrecognized key `{key_path}`. Custom keys must go under `+meta`."
                    );
                    let fs_err = FsError::new(
                        ErrorCode::SerializationError,
                        format!(
                            "Invalid {} definition `{}`: {}",
                            S::type_name(),
                            key_path,
                            detail
                        ),
                    );
                    emit_strict_parse_error(fs_err, dependency_package_name);
                }
                ShouldBe::ButIsnt(_) => {
                    let filename = if let Some(raw) = variant.as_ref_raw()
                        && let Some(filename) = raw.span().get_filename()
                    {
                        Some(filename)
                    } else {
                        None
                    };

                    // An unknown key produces the error message `expected struct <SelfType>` due to
                    // the recursive type. Catch the error here to inject a more descriptive error.
                    let err_msg = variant
                        .as_err_msg()
                        .expect("Error message always present on ShouldBe::ButIsnt variant");
                    let self_type = std::any::type_name::<S>()
                        .rsplit("::")
                        .next()
                        .unwrap_or_default();
                    let detail = if err_msg.contains(&format!("expected struct {self_type}")) {
                        format!("Unrecognized key `{key_path}`. Custom keys must go under `+meta`.")
                    } else {
                        err_msg.to_string()
                    };

                    let err = variant
                        .take_err()
                        .expect("Error always present on ShouldBe::ButIsnt variant");
                    let fs_err = yaml_to_fs_error(err, filename).with_context(format!(
                        "Invalid {} definition `{}`: {}",
                        S::type_name(),
                        key_path,
                        detail
                    ));
                    emit_strict_parse_error(fs_err, dependency_package_name);
                }
            }
        };
        if !disallow_plus_prefix {
            warn_plus_prefixed_resource_paths::<S>("", configs, "", false);
        }
        Ok(recur_build_dbt_project_config(
            dbt_config,
            configs,
            "",
            &on_error,
            disallow_plus_prefix,
        ))
    }

    /// Get the configuration for a fully qualified name (fqn)
    ///
    /// This method is recommended for nodes that don't derive from SQL files or where
    /// the node name doesn't match the filename. Examples include:
    /// - Exposures (defined in YAML files)
    /// - Unit tests (where the test name is separate from the model filename)
    /// - Sources (where the source and table names don't match file paths)
    /// - Any node where the fqn provides a more accurate representation than the file path
    ///
    /// The fqn should contain [package_name, path_component1, path_component2, ..., node_name]
    ///
    /// # Example
    /// ```rust
    /// use dbt_parser::dbt_project_config::DbtProjectConfig;
    /// use dbt_schemas::schemas::project::ModelConfig;
    /// use indexmap::IndexMap;
    ///
    /// // Minimal config tree; in practice this is built from dbt_project.yml
    /// let config = DbtProjectConfig::<ModelConfig> {
    ///     config: ModelConfig::default(),
    ///     children: IndexMap::new(),
    /// };
    /// let fqn = vec!["analytics".to_string(), "weekly_revenue_report".to_string()];
    /// let _cfg = config.get_config_for_fqn(&fqn);
    /// ```
    pub fn get_config_for_fqn(&self, fqn: &[String]) -> &T {
        let mut current_config = self;

        // Traverse through all components in the fqn
        for component in fqn {
            if let Some(child) = current_config.children.get(component) {
                current_config = child;
            } else {
                break;
            }
        }

        &current_config.config
    }

    /// Set the configuration for the root [GlobalProjectConfig]
    pub fn with_config(&mut self, config: T) {
        self.config = config;
    }
}

/// Resolves the final config for a node by merging three layers in increasing order of precedence:
///
/// 1. **Local project config** — `dbt_project.yml` for this package, path-matched by FQN
/// 2. **Properties / inline config** — `schema.yml` or inline `{{ config(...) }}` values
/// 3. **Root overlay** — root project's `dbt_project.yml`, applied only for dependency packages
///
/// Merging uses `ResolvableConfig`: each higher-precedence layer fills in unset fields from the
/// layers below it. `enabled` intentionally has no default until `finalize()` so that the root
/// overlay can disable a dependency node regardless of what lower layers set.
///
/// For the root package, `root` is `None` and no overlay is applied.
#[derive(Clone)]
pub struct ProjectConfigResolver<T: ResolvableConfig<T>> {
    local: DbtProjectConfig<T>,
    root: Option<DbtProjectConfig<T>>,
    resolve_defaults: T::ResolveDefaults,
}

impl<T: ResolvableConfig<T>> ProjectConfigResolver<T> {
    /// Use when the current package is the root project (no root overlay needed).
    pub fn for_root(config: DbtProjectConfig<T>) -> Self {
        ProjectConfigResolver {
            local: config,
            root: None,
            resolve_defaults: T::ResolveDefaults::default(),
        }
    }

    /// Use when the current package is a dependency.
    pub fn for_dependency(local: DbtProjectConfig<T>, root: DbtProjectConfig<T>) -> Self {
        ProjectConfigResolver {
            local,
            root: Some(root),
            resolve_defaults: T::ResolveDefaults::default(),
        }
    }

    /// Sets the resolve defaults, overriding the `Default` value.
    pub fn with_resolve_defaults(mut self, defaults: T::ResolveDefaults) -> Self {
        self.resolve_defaults = defaults;
        self
    }

    /// Builds a resolver from a root config. When `is_dependency` is true, `build_local` is
    /// called to construct the local package config; the closure is never called for root packages
    /// because the `root` argument itself serves as the local config (root packages have no
    /// separate overlay to apply).
    pub fn build<F>(
        root: DbtProjectConfig<T>,
        is_dependency: bool,
        build_local: F,
    ) -> FsResult<Self>
    where
        F: FnOnce() -> FsResult<DbtProjectConfig<T>>,
    {
        if is_dependency {
            Ok(Self::for_dependency(build_local()?, root))
        } else {
            Ok(Self::for_root(root))
        }
    }

    /// Applies the root project config overlay for dependency packages.
    fn apply_root_overlay(&self, config: &mut T, fqn: &[String]) {
        if let Some(root) = &self.root {
            let mut root_config = root.get_config_for_fqn(fqn).clone();
            root_config.default_to(config);
            *config = root_config;
        }
    }

    /// Merges the local project config with additional `configs` layers without applying the root
    /// overlay or calling `finalize`. Use this when the intermediate result is needed as the
    /// Jinja render context before inline `{{ config(...) }}` calls are processed.
    pub fn with_configs(&self, fqn: &[String], configs: &[Option<&T>]) -> T {
        let mut config = self.local.get_config_for_fqn(fqn).clone();
        for c in configs.iter().flatten() {
            let mut c = (*c).clone();
            c.default_to(&config);
            config = c;
        }
        config
    }

    /// Like `with_configs` but also applies the root project overlay. Use this when you need to
    /// validate explicitly-configured values (including root overlay) before
    /// `apply_resolve_defaults` fills in CLI-flag defaults.
    pub fn with_configs_and_root_overlay(&self, fqn: &[String], configs: &[Option<&T>]) -> T {
        let mut config = self.with_configs(fqn, configs);
        self.apply_root_overlay(&mut config, fqn);
        config
    }

    /// Fully resolves config by applying all layers and calling `finalize`.
    ///
    /// `original_fqn` is used for local project config lookup so that nodes whose paths are
    /// transformed by fusion (snapshots, generated tests) still resolve against their original
    /// directory hierarchy. `fqn` is used for root overlay lookup. Pass the same value for both
    /// when no path transformation occurs (models, seeds, etc.).
    pub fn resolve_with_configs(
        &self,
        original_fqn: &[String],
        fqn: &[String],
        configs: &[Option<&T>],
    ) -> T::Resolved {
        self.resolve_with_overrides(original_fqn, fqn, configs, |_| {})
    }

    /// Like `resolve_with_configs` but applies `override_fn` to the merged config after all layers
    /// (including the root overlay and resolve defaults) are applied, just before `finalize`.
    /// Use this when a caller needs to unconditionally force a field value regardless of what the
    /// user configured (e.g. forcing `enabled = false` on a render-error path).
    pub fn resolve_with_overrides(
        &self,
        original_fqn: &[String],
        fqn: &[String],
        configs: &[Option<&T>],
        override_fn: impl FnOnce(&mut T),
    ) -> T::Resolved {
        let mut config = self.with_configs(original_fqn, configs);
        self.apply_root_overlay(&mut config, fqn);
        config.apply_resolve_defaults(self.resolve_defaults.clone());
        override_fn(&mut config);
        config.finalize()
    }

    /// Like `resolve_with_overrides` but `override_fn` may fail. Use this when the override
    /// logic itself can produce an error that must propagate to the caller.
    pub fn try_resolve_with_overrides<E>(
        &self,
        original_fqn: &[String],
        fqn: &[String],
        configs: &[Option<&T>],
        override_fn: impl FnOnce(&mut T) -> Result<(), E>,
    ) -> Result<T::Resolved, E> {
        let mut config = self.with_configs(original_fqn, configs);
        self.apply_root_overlay(&mut config, fqn);
        config.apply_resolve_defaults(self.resolve_defaults.clone());
        override_fn(&mut config)?;
        Ok(config.finalize())
    }

    /// Convenience wrapper: equivalent to `resolve_with_configs(fqn, fqn, &[properties_config])`.
    pub fn resolve_with_properties(
        &self,
        fqn: &[String],
        properties_config: Option<&T>,
    ) -> T::Resolved {
        self.resolve_with_configs(fqn, fqn, &[properties_config])
    }

    /// Returns true if the root overlay explicitly sets `enabled = false` for this FQN.
    /// When true, SQL rendering can be skipped entirely: the root overlay has the highest
    /// precedence for dependency packages, so no inline `{{ config(...) }}` call can re-enable
    /// the node.
    pub fn is_disabled_by_root_overlay(&self, fqn: &[String]) -> bool {
        self.root
            .as_ref()
            .map(|root| !root.get_config_for_fqn(fqn).get_enabled_with_default())
            .unwrap_or(false)
    }
}

/// Recursively build the [DbtProjectConfig] from a parent and child configuration.
///
/// The `on_error` closure is called for each `ShouldBe::ButIsnt` variant encountered
/// during traversal. Use this to emit parse errors or silently skip invalid children.
pub fn recur_build_dbt_project_config<T, S, F>(
    parent_config: &T,
    child: &S,
    key_path: &str,
    on_error: &F,
    disallow_plus_prefix: bool,
) -> DbtProjectConfig<T>
where
    T: ResolvableConfig<T> + PartialEq,
    S: Into<T> + TypedRecursiveConfig,
    F: Fn(&ShouldBe<S>, &str, &str),
{
    let mut child_config: T = child.clone().into();
    child_config.default_to(parent_config);
    let mut children = IndexMap::new();

    // Handle additional properties generically - each child inherits from current config
    for (key, maybe_child_config_variant) in child.iter_children() {
        let child_key_path = if key_path.is_empty() {
            key.clone()
        } else {
            format!("{key_path}.{key}")
        };

        if key.starts_with("+") {
            if disallow_plus_prefix {
                on_error(maybe_child_config_variant, key, key_path);
                continue;
            }
        }

        let child_config_variant = match maybe_child_config_variant {
            ShouldBe::AndIs(config) => config,
            ShouldBe::ButIsnt(..) => {
                on_error(maybe_child_config_variant, key, key_path);
                continue;
            }
        };

        children.insert(
            key.clone(),
            recur_build_dbt_project_config(
                &child_config,
                child_config_variant,
                &child_key_path,
                on_error,
                disallow_plus_prefix,
            ),
        );
    }

    DbtProjectConfig {
        config: child_config,
        children,
    }
}

/// Emit one warning for each `+`-prefixed resource path in the config tree.
///
/// A `+`-prefixed key is a resource path whose name starts with `+` when it carries a
/// non-default config or has a `+`-prefixed child.
///
/// Returns whether `key` itself starts with `+`.
fn warn_plus_prefixed_resource_paths<S>(key: &str, value: &S, path: &str, inside_plus: bool) -> bool
where
    S: TypedRecursiveConfig,
{
    let key_is_plus = key.starts_with('+');

    let key_path = if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    };

    let warn = || {
        emit_warn_log_message(
            ErrorCode::InvalidConfig,
            format!(
                "Resource path `{key_path}` in dbt_project.yml starts with `+`. This will be deprecated in future versions of dbt."
            ),
        );
    };

    // Case 1: `value` is non-default, meaning there is a valid config inside it somewhere.
    // We definitely know a config value was set, because it must have this shape:
    //
    // ```
    // +key:
    //   +some_config: some_value
    // ```
    //
    // This is just an approximation and will miss cases where the config is explicitly set to
    // default.
    let valid_config_exists = value.has_set_fields();

    // We want to throw warnings at the location of the topmost +-prefixed key, so we throw exactly
    // one warning per offending resource path.
    let is_parent_plus = key_is_plus && !inside_plus;
    if is_parent_plus && valid_config_exists {
        warn();
        return key_is_plus;
    }

    // Case 2: Any non-config children start with +, or any descendants fall under case 1.
    let mut any_child_plus = false;
    for (child_key, child_variant) in value.iter_children() {
        let child_is_plus = match child_variant {
            ShouldBe::AndIs(child) => warn_plus_prefixed_resource_paths::<S>(
                child_key,
                child,
                &key_path,
                inside_plus || key_is_plus,
            ),
            ShouldBe::ButIsnt(..) => child_key.starts_with('+'),
        };
        any_child_plus |= child_is_plus;
    }
    if is_parent_plus && any_child_plus {
        warn();
    }

    key_is_plus || valid_config_exists || any_child_plus
}

/// Config wrapping propagated configs for the root project
#[derive(Debug)]
pub struct RootProjectConfigs {
    /// Model configs
    pub models: DbtProjectConfig<ModelConfig>,
    /// Source configs
    pub sources: DbtProjectConfig<SourceConfig>,
    /// Snapshot configs
    pub snapshots: DbtProjectConfig<SnapshotConfig>,
    /// Seed configs
    pub seeds: DbtProjectConfig<SeedConfig>,
    /// Test configs
    pub tests: DbtProjectConfig<DataTestConfig>,
    /// Unit test configs
    pub unit_tests: DbtProjectConfig<UnitTestConfig>,
    /// Exposure configs
    pub exposures: DbtProjectConfig<ExposureConfig>,
    /// Semantic model configs
    pub semantic_models: DbtProjectConfig<SemanticModelConfig>,
    /// Metric configs
    pub metrics: DbtProjectConfig<MetricConfig>,
    /// Saved query configs
    pub saved_queries: DbtProjectConfig<SavedQueryConfig>,
    /// Analysis configs
    pub analyses: DbtProjectConfig<AnalysesConfig>,
    /// Function configs
    pub functions: DbtProjectConfig<FunctionConfig>,
}

/// Read the `require_resource_names_without_plus_prefix` behavior flag from a
/// project's `flags` block. Absent or non-boolean values resolve to `false`.
pub(crate) fn disallow_plus_prefix_from_flags(flags: Option<&dbt_yaml::Value>) -> bool {
    flags
        .and_then(|flags| {
            project_flags_get_value(flags, "require_resource_names_without_plus_prefix")
        })
        .and_then(dbt_yaml::Value::as_bool)
        .unwrap_or(false)
}

/// Build the [RootProjectConfigs] from a [DbtProject]
pub fn build_root_project_configs(
    root_project: &DbtProject,
    root_project_quoting: DbtQuoting,
) -> FsResult<RootProjectConfigs> {
    let maybe_root_project_config =
        match (root_project.tests.clone(), root_project.data_tests.clone()) {
            (Some(_), Some(_)) => {
                unimplemented!("Merge logic for tests and data tests is unimplemented")
            }
            (Some(tests), None) => Some(tests),
            (None, Some(data_tests)) => Some(data_tests),
            (None, None) => None,
        };
    let disallow_plus_prefix = disallow_plus_prefix_from_flags(root_project.flags.as_ref());

    Ok(RootProjectConfigs {
        models: init_project_config(
            &root_project.models,
            root_project_quoting,
            None,
            disallow_plus_prefix,
        )?,
        sources: init_project_config(&root_project.sources, (), None, disallow_plus_prefix)?,
        snapshots: init_project_config(
            &root_project.snapshots,
            root_project_quoting,
            None,
            disallow_plus_prefix,
        )?,
        seeds: init_project_config(
            &root_project.seeds,
            root_project_quoting,
            None,
            disallow_plus_prefix,
        )?,
        tests: init_project_config(
            &maybe_root_project_config,
            root_project_quoting,
            None,
            disallow_plus_prefix,
        )?,
        unit_tests: init_project_config(&root_project.unit_tests, (), None, disallow_plus_prefix)?,
        exposures: init_project_config(&root_project.exposures, (), None, disallow_plus_prefix)?,
        semantic_models: init_project_config(
            &root_project.semantic_models,
            (),
            None,
            disallow_plus_prefix,
        )?,
        metrics: init_project_config(&root_project.metrics, (), None, disallow_plus_prefix)?,
        saved_queries: init_project_config(
            &root_project.saved_queries,
            (),
            None,
            disallow_plus_prefix,
        )?,
        analyses: init_project_config(&root_project.analyses, (), None, disallow_plus_prefix)?,
        functions: init_project_config(
            &root_project.functions,
            root_project_quoting,
            None,
            disallow_plus_prefix,
        )?,
    })
}

/// generate the project config that will be inherited throughout the project
pub fn init_project_config<
    T: ResolvableConfig<T> + PartialEq,
    S: TypedRecursiveConfig + Into<T>,
>(
    dbt_project_configs: &Option<S>,
    package_defaults: T::PackageDefaults,
    dependency_package_name: Option<&str>,
    disallow_plus_prefix: bool,
) -> FsResult<DbtProjectConfig<T>> {
    let mut default_config = T::default();
    default_config.apply_package_defaults(package_defaults);
    let project_config = if let Some(configs) = dbt_project_configs {
        DbtProjectConfig::try_new(
            &default_config,
            configs,
            dependency_package_name,
            disallow_plus_prefix,
        )?
    } else {
        DbtProjectConfig {
            config: default_config,
            children: IndexMap::new(),
        }
    };
    Ok(project_config)
}

/// Strip resource paths from the beginning of a reference path
/// This function tries to find which resource path is a prefix of the ref_path
/// and returns the path with that prefix stripped
pub fn strip_resource_paths_from_ref_path(ref_path: &Path, resource_paths: &[String]) -> PathBuf {
    // Try to find a resource path that is a prefix of the ref_path
    for resource_path in resource_paths {
        let resource_pathbuf = PathBuf::from(resource_path);

        // Use Path::starts_with which properly handles path components
        if ref_path.starts_with(&resource_pathbuf) {
            // Use Path::strip_prefix which is designed for this exact purpose
            if let Ok(stripped) = ref_path.strip_prefix(&resource_pathbuf) {
                // Only return the stripped path if it's not empty
                // (i.e., ref_path was not exactly equal to resource_path)
                if stripped.as_os_str().is_empty() {
                    return ref_path.to_path_buf();
                } else {
                    return stripped.to_path_buf();
                }
            }
        }
    }

    // If no resource path matches, return the original path
    ref_path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_resource_paths_single_level() {
        let ref_path = Path::new("models/my_model.sql");
        let resource_paths = vec!["models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("my_model.sql"));
    }

    #[test]
    fn test_strip_resource_paths_nested_structure() {
        let ref_path = Path::new("dbt/models/example/my_first_model.sql");
        let resource_paths = vec!["dbt/models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("example/my_first_model.sql"));
    }

    #[test]
    fn test_strip_resource_paths_deep_nesting() {
        let ref_path = Path::new("warehouse/staging/models/marts/finance/revenue.sql");
        let resource_paths = vec!["warehouse/staging/models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("marts/finance/revenue.sql"));
    }

    #[test]
    fn test_strip_resource_paths_multiple_paths() {
        let ref_path = Path::new("src/models/staging/customers.sql");
        let resource_paths = vec![
            "models".to_string(),
            "src/models".to_string(),
            "dbt/models".to_string(),
        ];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("staging/customers.sql"));
    }

    #[test]
    fn test_strip_resource_paths_no_match() {
        let ref_path = Path::new("analysis/my_analysis.sql");
        let resource_paths = vec!["models".to_string(), "seeds".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("analysis/my_analysis.sql"));
    }

    #[test]
    fn test_strip_resource_paths_empty_resource_paths() {
        let ref_path = Path::new("models/example/my_model.sql");
        let resource_paths: Vec<String> = vec![];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        assert_eq!(result, PathBuf::from("models/example/my_model.sql"));
    }

    #[test]
    fn test_strip_resource_paths_exact_match() {
        let ref_path = Path::new("models");
        let resource_paths = vec!["models".to_string()];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        // Should return original path since stripping would result in empty string
        assert_eq!(result, PathBuf::from("models"));
    }

    #[test]
    fn test_strip_resource_paths_first_match_wins() {
        // Test that the function uses the first matching path in the array
        let ref_path = Path::new("models/staging/customers.sql");
        let resource_paths = vec![
            "models".to_string(),         // This should match first
            "models/staging".to_string(), // This is more specific but comes later
        ];
        let result = strip_resource_paths_from_ref_path(ref_path, &resource_paths);
        // Should strip "models" (first match), not "models/staging"
        assert_eq!(result, PathBuf::from("staging/customers.sql"));
    }

    #[test]
    fn test_resource_path_edge_cases() {
        // Test various edge cases that could occur in real projects

        // Case 1: Resource path with trailing slash
        let result1 = strip_resource_paths_from_ref_path(
            Path::new("models/my_model.sql"),
            &["models/".to_string()],
        );
        assert_eq!(result1, PathBuf::from("my_model.sql"));

        // Case 2: Very deep nesting
        let result2 = strip_resource_paths_from_ref_path(
            Path::new("data/warehouse/dbt/models/marts/finance/reporting/revenue_monthly.sql"),
            &["data/warehouse/dbt/models".to_string()],
        );
        assert_eq!(
            result2,
            PathBuf::from("marts/finance/reporting/revenue_monthly.sql")
        );

        // Case 3: Path that has similar prefix but different directory
        // This should NOT be stripped because "models_backup" is not the "models" directory
        let result3 = strip_resource_paths_from_ref_path(
            Path::new("models_backup/my_model.sql"),
            &["models".to_string()],
        );
        // Fixed behavior: no stripping since "models_backup" != "models" directory
        assert_eq!(result3, PathBuf::from("models_backup/my_model.sql"));
    }

    #[test]
    fn test_path_component_boundary_matching() {
        // Test that we correctly distinguish between path components vs string prefixes

        // Should strip: exact directory match
        let result1 = strip_resource_paths_from_ref_path(
            Path::new("models/staging/customers.sql"),
            &["models".to_string()],
        );
        assert_eq!(result1, PathBuf::from("staging/customers.sql"));

        // Should NOT strip: different directory with similar name
        let result2 = strip_resource_paths_from_ref_path(
            Path::new("models_v2/customers.sql"),
            &["models".to_string()],
        );
        assert_eq!(result2, PathBuf::from("models_v2/customers.sql"));

        // Should NOT strip: file that starts with resource path name
        let result3 =
            strip_resource_paths_from_ref_path(Path::new("models.sql"), &["models".to_string()]);
        assert_eq!(result3, PathBuf::from("models.sql"));

        // Should strip: nested path with exact component match
        let result4 = strip_resource_paths_from_ref_path(
            Path::new("src/models/staging/customers.sql"),
            &["src/models".to_string()],
        );
        assert_eq!(result4, PathBuf::from("staging/customers.sql"));

        // Should NOT strip: similar but different nested path
        let result5 = strip_resource_paths_from_ref_path(
            Path::new("src/models_new/customers.sql"),
            &["src/models".to_string()],
        );
        assert_eq!(result5, PathBuf::from("src/models_new/customers.sql"));
    }

    #[test]
    fn test_get_config_for_fqn_basic() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);

        // Add a child config for project "test_project"
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        project_config.config.enabled = Some(false);
        config
            .children
            .insert("test_project".to_string(), project_config);

        let fqn = vec!["test_project".to_string()];
        let result = config.get_config_for_fqn(&fqn);

        assert_eq!(result.enabled, Some(false));
    }

    #[test]
    fn test_get_config_for_fqn_nested() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        project_config.config.enabled = Some(false);

        // Add staging subdirectory config
        let mut staging_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        staging_config.config.enabled = Some(true);
        staging_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);

        project_config
            .children
            .insert("staging".to_string(), staging_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        let fqn = vec!["test_project".to_string(), "staging".to_string()];
        let result = config.get_config_for_fqn(&fqn);

        assert_eq!(result.enabled, Some(true));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table)
        );
    }

    #[test]
    fn test_get_config_for_fqn_node_specific() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        project_config.config.enabled = Some(false);

        // Add staging subdirectory config
        let mut staging_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        staging_config.config.enabled = Some(true);

        // Add node-specific config
        let mut node_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        node_config.config.enabled = Some(false);
        node_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental);

        staging_config
            .children
            .insert("stg_customers".to_string(), node_config);
        project_config
            .children
            .insert("staging".to_string(), staging_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        let fqn = vec![
            "test_project".to_string(),
            "staging".to_string(),
            "stg_customers".to_string(),
        ];
        let result = config.get_config_for_fqn(&fqn);

        assert_eq!(result.enabled, Some(false));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental)
        );
    }

    #[test]
    fn test_get_config_for_fqn_partial_match() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        project_config.config.enabled = Some(false);

        // Add staging subdirectory config - only staging exists, not finance
        let mut staging_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        staging_config.config.enabled = Some(false);
        staging_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        project_config
            .children
            .insert("staging".to_string(), staging_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        // FQN has staging/finance but only staging config exists
        let fqn = vec![
            "test_project".to_string(),
            "staging".to_string(),
            "finance".to_string(),
            "customers".to_string(),
        ];
        let result = config.get_config_for_fqn(&fqn);

        // Should get staging config since finance doesn't exist
        assert_eq!(result.enabled, Some(false));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::View)
        );
    }

    #[test]
    fn test_get_config_for_fqn_nonexistent_project() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);
        config.config.materialized = Some(dbt_schemas::schemas::common::DbtMaterialization::Table);

        let fqn = vec![
            "nonexistent_project".to_string(),
            "staging".to_string(),
            "customers".to_string(),
        ];
        let result = config.get_config_for_fqn(&fqn);

        // Should return root config
        assert_eq!(result.enabled, Some(true));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table)
        );
    }

    #[test]
    fn test_get_config_for_fqn_empty() {
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);
        config.config.materialized = Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        let fqn: Vec<String> = vec![];
        let result = config.get_config_for_fqn(&fqn);

        // Should return root config
        assert_eq!(result.enabled, Some(true));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::View)
        );
    }

    #[test]
    fn test_get_config_for_fqn_complex_hierarchy() {
        // Test a complex hierarchy that might occur with non-file-based nodes
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);

        // Set up: my_project -> marts -> finance -> revenue_reports -> monthly_revenue
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        project_config.config.enabled = Some(true);

        let mut marts_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        marts_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);

        let mut finance_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        finance_config.config.enabled = Some(false);

        let mut revenue_reports_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        revenue_reports_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        let mut monthly_revenue_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        monthly_revenue_config.config.enabled = Some(true);
        monthly_revenue_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental);

        revenue_reports_config
            .children
            .insert("monthly_revenue".to_string(), monthly_revenue_config);
        finance_config
            .children
            .insert("revenue_reports".to_string(), revenue_reports_config);
        marts_config
            .children
            .insert("finance".to_string(), finance_config);
        project_config
            .children
            .insert("marts".to_string(), marts_config);
        config
            .children
            .insert("my_project".to_string(), project_config);

        let fqn = vec![
            "my_project".to_string(),
            "marts".to_string(),
            "finance".to_string(),
            "revenue_reports".to_string(),
            "monthly_revenue".to_string(),
        ];
        let result = config.get_config_for_fqn(&fqn);

        // Should get the most specific config (node-level)
        assert_eq!(result.enabled, Some(true));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental)
        );
    }

    #[test]
    fn test_get_config_for_fqn_deep_nested_path() {
        // Test equivalent to get_config_for_path_empty_resource_paths
        // This tests traversing a full deep path hierarchy
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);

        // Add project config with nested subdirectory structure
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        let mut models_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        let mut example_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        example_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);

        models_config
            .children
            .insert("example".to_string(), example_config);
        project_config
            .children
            .insert("models".to_string(), models_config);
        config
            .children
            .insert("test_project".to_string(), project_config);

        // FQN represents the full hierarchy: test_project -> models -> example -> my_model
        let fqn = vec![
            "test_project".to_string(),
            "models".to_string(),
            "example".to_string(),
            "my_model".to_string(),
        ];
        let result = config.get_config_for_fqn(&fqn);

        // Should traverse the full path and get the example config
        // (since my_model doesn't exist, it stops at example)
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table)
        );
    }

    #[test]
    fn test_get_config_for_fqn_integration_realistic_dbt_structure() {
        // Integration test equivalent to test_integration_real_dbt_project_structure
        // Test a realistic DBT project scenario end-to-end with FQN
        let mut config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        config.config.enabled = Some(true);
        config.config.materialized = Some(dbt_schemas::schemas::common::DbtMaterialization::View);

        // Set up project structure like: my_project -> staging -> +materialized: table
        let mut project_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        project_config.config.enabled = Some(true);

        let mut staging_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        staging_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Table);
        staging_config.config.enabled = Some(true);

        // Add specific model config
        let mut customers_config = DbtProjectConfig {
            config: ModelConfig::default(),
            children: IndexMap::new(),
        };
        customers_config.config.materialized =
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental);
        customers_config.config.enabled = Some(false);

        staging_config
            .children
            .insert("stg_customers".to_string(), customers_config);
        project_config
            .children
            .insert("staging".to_string(), staging_config);
        config
            .children
            .insert("my_project".to_string(), project_config);

        // FQN: my_project -> staging -> stg_customers
        // This represents the logical hierarchy similar to path:
        // warehouse/dbt/models/staging/stg_customers.sql with resource_paths stripped
        let fqn = vec![
            "my_project".to_string(),
            "staging".to_string(),
            "stg_customers".to_string(),
        ];
        let result = config.get_config_for_fqn(&fqn);

        // Should get the most specific config (file-level)
        assert_eq!(result.enabled, Some(false));
        assert_eq!(
            result.materialized,
            Some(dbt_schemas::schemas::common::DbtMaterialization::Incremental)
        );
    }
}

#[cfg(test)]
mod init_project_config_tests {
    use super::*;

    use dbt_common::tracing::fs_error_log::get_log_message;
    use dbt_schemas::schemas::project::{ProjectModelConfig, ProjectSnapshotConfig};
    use dbt_tracing::{
        SeverityNumber, TelemetryOutputFlags,
        emit::create_root_info_span,
        init::create_tracing_subcriber_with_layer,
        layer::ConsumerLayer,
        test_support::mocks::{MockDynSpanEvent, TestLayer, test_data_layer},
    };

    #[allow(clippy::type_complexity)]
    fn init_project_config_from_yaml<T, S>(
        yaml: &str,
        disallow_plus_prefix: bool,
    ) -> (
        FsResult<DbtProjectConfig<T>>,
        Vec<(ErrorCode, String)>,
        Vec<(ErrorCode, String)>,
    )
    where
        T: ResolvableConfig<T> + PartialEq,
        S: TypedRecursiveConfig + Into<T> + for<'de> serde::Deserialize<'de>,
        T::PackageDefaults: Default,
    {
        // Diagnostics are emitted through the tracing layer, so capture them with a
        // test consumer rather than a status reporter.
        let (test_layer, _, _, log_records) = TestLayer::new();
        let subscriber = create_tracing_subcriber_with_layer(
            tracing::level_filters::LevelFilter::TRACE,
            test_data_layer(
                1,
                None,
                false,
                std::iter::empty(),
                std::iter::once(Box::new(test_layer) as ConsumerLayer),
            ),
            &[],
        )
        .expect("test tracing subscriber should be valid");

        let configs: S = dbt_jinja_utils::serde::from_yaml_raw(yaml, None, false, None)
            .expect("yaml deserializes into config type");

        // The data layer only records events emitted within an active span, so run
        // `init_project_config` inside a root span.
        let result = tracing::subscriber::with_default(subscriber, || {
            let _root = create_root_info_span(MockDynSpanEvent {
                name: "root".to_string(),
                flags: TelemetryOutputFlags::ALL,
                ..Default::default()
            })
            .entered();

            init_project_config::<T, S>(
                &Some(configs),
                Default::default(),
                None,
                disallow_plus_prefix,
            )
        });

        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        for record in log_records.lock().unwrap().iter() {
            // Diagnostics may arrive as either an `FsErrorLog` or a plain `LogMessage`
            // attribute; `get_log_message` handles both. The tests only inspect the
            // message string, so a placeholder `ErrorCode` is acceptable.
            let code = get_log_message(&record.attributes)
                .and_then(|lm| lm.code)
                .and_then(|c| u16::try_from(c).ok())
                .and_then(|c| ErrorCode::try_from(c).ok())
                .unwrap_or(ErrorCode::Generic);
            let entry = (code, record.body.clone());
            match record.severity_number {
                SeverityNumber::Error => errors.push(entry),
                SeverityNumber::Warn => warnings.push(entry),
                _ => {}
            }
        }

        (result, errors, warnings)
    }

    #[test]
    fn valid_config_emits_no_diagnostics() {
        let yml = r#"
path_1:
  +enabled: true
path_2:
  +enabled: true
  path_3:
    +enabled: false
"#;
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());
    }

    #[test]
    fn plus_prefixed_resource_path_emits_diagnostics() {
        let yml = r#"
+path_1:
  +enabled: false
  +quoting:
    database: true
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert_eq!(warnings.len(), 1);
        let (_, msg) = &warnings[0];
        assert!(msg.contains("Resource path `+path_1` in dbt_project.yml starts with `+`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `+path_1`"));
    }

    #[test]
    fn nested_plus_resource_path_emits_diagnostics() {
        let yml = r#"
path_1:
  +path_2:
    path_3:
      path_4:
        +enabled: true
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert_eq!(warnings.len(), 1);
        let (_, msg) = &warnings[0];
        assert!(msg.contains("Resource path `path_1.+path_2` in dbt_project.yml starts with `+`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+path_2`"));
    }

    #[test]
    fn ambiguous_resource_path_only_errors() {
        let yml = r#"
path_1:
  path_2:
    +enabled: true
    path_3:
      +not_provably_a_resource_path: {}
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(
            msg.contains("Unrecognized key `path_1.path_2.path_3.+not_provably_a_resource_path`")
        );
    }

    #[test]
    fn meta_accepts_plus_prefixes() {
        let yml = r#"
path_1:
  path_2:
    +enabled: true
    +meta:
      +key_1: true
      +key_2: 15451
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert!(warnings.is_empty());
    }

    #[test]
    fn exactly_one_warning_per_plus_prefixed_path() {
        let yml = r#"
path_1:
  +path_2:
    +meta:
      +cool_key: 15451
    path_3:
      +quoting:
        database: true
        schema: false
      path_4:
        +enabled: true
  +path_3:
    +alias: cool_alias
    path_5:
      +description: cool_description
  +not_provably_a_resource_path: {}
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(errors.is_empty());
        assert_eq!(warnings.len(), 2);
        let (_, msg) = &warnings[0];
        assert!(msg.contains("Resource path `path_1.+path_2` in dbt_project.yml starts with `+`"));
        let (_, msg) = &warnings[1];
        assert!(msg.contains("Resource path `path_1.+path_3` in dbt_project.yml starts with `+`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 3);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+not_provably_a_resource_path`"));
        let (_, msg) = &errors[1];
        assert!(msg.contains("Unrecognized key `path_1.+path_2`"));
        let (_, msg) = &errors[2];
        assert!(msg.contains("Unrecognized key `path_1.+path_3`"));
    }

    #[test]
    fn unrecognized_key_always_emits_error() {
        let yml = r#"
path_1:
  +bogus_key: scalar_value
"#;
        // Behavior flag off
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, false);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+bogus_key`"));

        // Behavior flag on
        let (result, errors, warnings) =
            init_project_config_from_yaml::<ModelConfig, ProjectModelConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `path_1.+bogus_key`"));
    }

    #[test]
    fn repro_emits_error_at_key() {
        // https://github.com/dbt-labs/dbt-core/issues/14433
        let yml = r#"
my_project:
  staging:
    +contract:
      enforced: true
"#;
        // Behavior flag off
        // With the behavior flag off, the behavior is the same as in the issue.
        // We don't recognize that +contract is the source of the issue.

        // Behavior flag on
        // With the behavior flag on, we correctly see the unrecognized key
        // error surface at +contract.
        let (result, errors, warnings) =
            init_project_config_from_yaml::<SnapshotConfig, ProjectSnapshotConfig>(yml, true);
        assert!(result.is_ok());
        assert!(warnings.is_empty());
        assert_eq!(errors.len(), 1);
        let (_, msg) = &errors[0];
        assert!(msg.contains("Unrecognized key `my_project.staging.+contract`"));
    }
}
