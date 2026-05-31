// This code was generated from dbt-make-dbt-schemas/json_schemas/dbt_project.json on 2025-03-31T06:22:06. Do not edit.

use std::collections::HashMap;
use std::collections::btree_map::Iter;
use std::fmt::Debug;

use dbt_yaml::DbtSchema;

// Type aliases for clarity
type YmlValue = dbt_yaml::Value;
use dbt_yaml::ShouldBe;
use dbt_yaml::Spanned;
use dbt_yaml::UntaggedEnumDeserialize;
use dbt_yaml::Verbatim;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use strum::{Display, EnumString};

use crate::schemas::common::DbtQuoting;
use crate::schemas::common::SyncConfig;
use crate::schemas::project::ProjectAnalysisConfig;
use crate::schemas::project::ProjectSemanticModelConfig;
use crate::schemas::project::configs::saved_query_config::ProjectSavedQueryConfig;
use crate::schemas::serde::FloatOrString;
use crate::schemas::serde::SpannedStringOrArrayOfStrings;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::StringOrInteger;

use super::ProjectDataTestConfig;
use super::ProjectExposureConfig;
use super::ProjectFunctionConfig;
use super::ProjectMetricConfigs;
use super::ProjectModelConfig;
use super::ProjectSeedConfig;
use super::ProjectSnapshotConfig;
use super::ProjectSourceConfig;
use super::ProjectUnitTestConfig;

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ProjectDbtCloudConfig {
    #[serde(rename = "project-id")]
    pub project_id: Option<StringOrInteger>,
    #[serde(rename = "defer-env-id")]
    pub defer_env_id: Option<StringOrInteger>,

    // unsure if any of these other keys are actually used or expected
    pub account_id: Option<StringOrInteger>,
    #[serde(rename = "account-host")]
    pub account_host: Option<String>,
    #[serde(rename = "job-id")]
    pub job_id: Option<StringOrInteger>,
    #[serde(rename = "run-id")]
    pub run_id: Option<StringOrInteger>,
    pub api_key: Option<StringOrInteger>,
    pub application: Option<StringOrInteger>,
    pub environment: Option<StringOrInteger>,
    pub tenant_hostname: Option<String>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct DbtProjectNameOnly {
    pub name: String,

    pub __ignored__: Verbatim<HashMap<String, dbt_yaml::Value>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct DbtProjectSimplified {
    #[serde(rename = "packages-install-path")]
    pub packages_install_path: Option<String>,
    pub profile: Spanned<Option<String>>,
    #[serde(rename = "dbt-cloud")]
    pub dbt_cloud: Option<ProjectDbtCloudConfig>,
    pub flags: Option<YmlValue>,

    // Deprecated paths
    // When present in the db_project.yml file we will raise an error
    #[serde(rename = "data-paths")]
    pub data_paths: Verbatim<Option<Vec<String>>>,
    #[serde(rename = "source-paths")]
    pub source_paths: Verbatim<Option<Vec<String>>>,
    #[serde(rename = "log-path")]
    pub log_path: Verbatim<Option<String>>,
    #[serde(rename = "target-path")]
    pub target_path: Verbatim<Option<String>>,

    pub __ignored__: Verbatim<HashMap<String, dbt_yaml::Value>>,
}

#[derive(
    Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumString, Display, DbtSchema,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum LogPath {
    #[default]
    Logs,
}

#[derive(
    Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumString, Display, DbtSchema,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TargetPath {
    #[default]
    Target,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct DbtProject {
    pub name: String,
    pub version: Option<FloatOrString>,
    pub profile: Option<String>,
    // Paths
    #[serde(rename = "analysis-paths")]
    pub analysis_paths: Option<Vec<String>>,
    #[serde(rename = "asset-paths")]
    pub asset_paths: Option<Vec<String>>,
    #[serde(rename = "macro-paths")]
    pub macro_paths: Option<Vec<String>>,
    #[serde(rename = "model-paths")]
    pub model_paths: Option<Vec<String>>,
    #[serde(rename = "function-paths")]
    pub function_paths: Option<Vec<String>>,
    #[serde(rename = "seed-paths")]
    pub seed_paths: Option<Vec<String>>,
    #[serde(rename = "snapshot-paths")]
    pub snapshot_paths: Option<Vec<String>>,
    #[serde(rename = "test-paths")]
    pub test_paths: Option<Vec<String>>,
    #[serde(rename = "docs-paths")]
    pub docs_paths: Option<Vec<String>>,
    #[serde(rename = "target-path")]
    pub target_path: Option<TargetPath>,
    #[serde(rename = "log-path")]
    pub log_path: Option<LogPath>,
    #[serde(rename = "packages-install-path")]
    pub packages_install_path: Option<String>,
    // Configs
    pub metrics: Option<ProjectMetricConfigs>,
    pub models: Option<ProjectModelConfig>,
    pub functions: Option<ProjectFunctionConfig>,
    pub snapshots: Option<ProjectSnapshotConfig>,
    pub seeds: Option<ProjectSeedConfig>,
    pub sources: Option<ProjectSourceConfig>,
    pub tests: Option<ProjectDataTestConfig>,
    pub unit_tests: Option<ProjectUnitTestConfig>,
    pub data_tests: Option<ProjectDataTestConfig>,
    pub exposures: Option<ProjectExposureConfig>,
    pub analyses: Option<ProjectAnalysisConfig>,
    #[serde(rename = "saved-queries")]
    pub saved_queries: Option<ProjectSavedQueryConfig>,
    #[serde(rename = "semantic-models")]
    pub semantic_models: Option<ProjectSemanticModelConfig>,
    // Misc
    #[serde(rename = "clean-targets")]
    pub clean_targets: Option<Vec<String>>,
    #[serde(rename = "config-version")]
    pub config_version: Option<i32>,
    #[serde(rename = "dbt-cloud")]
    pub dbt_cloud: Option<ProjectDbtCloudConfig>,
    pub dispatch: Option<Vec<_Dispatch>>,
    pub flags: Option<YmlValue>,
    #[serde(rename = "on-run-end")]
    pub on_run_end: Verbatim<Option<SpannedStringOrArrayOfStrings>>,
    #[serde(rename = "on-run-start")]
    pub on_run_start: Verbatim<Option<SpannedStringOrArrayOfStrings>>,
    #[serde(rename = "query-comment")]
    pub query_comment: Verbatim<Option<QueryComment>>,
    pub quoting: Spanned<Option<DbtQuoting>>,
    pub sync: Option<SyncConfig>,
    #[serde(rename = "require-dbt-version")]
    pub require_dbt_version: Option<StringOrArrayOfStrings>,
    #[serde(rename = "restrict-access")]
    pub restrict_access: Option<bool>,
    pub vars: Verbatim<Option<dbt_yaml::Value>>,
}

impl DbtProject {
    pub fn get_project_id(&self) -> String {
        /*
        Returns the hash of the project name. Can be used for telemetry.
        */
        // TODO: do we really need cryptographic hashing here?
        format!("{:x}", md5::compute(self.name.as_bytes()))
    }

    pub fn all_source_paths(&self) -> Vec<String> {
        /*
        Returns a vector of strings combining all path configurations:
        model_paths, function_paths, seed_paths, snapshot_paths, analysis_paths, macro_paths, and test_paths.
        */
        let mut paths = Vec::new();

        if let Some(ref model_paths) = self.model_paths {
            paths.extend(model_paths.clone());
        }
        if let Some(ref function_paths) = self.function_paths {
            paths.extend(function_paths.clone());
        }
        if let Some(ref seed_paths) = self.seed_paths {
            paths.extend(seed_paths.clone());
        }
        if let Some(ref snapshot_paths) = self.snapshot_paths {
            paths.extend(snapshot_paths.clone());
        }
        if let Some(ref analysis_paths) = self.analysis_paths {
            paths.extend(analysis_paths.clone());
        }
        if let Some(ref macro_paths) = self.macro_paths {
            paths.extend(macro_paths.clone());
        }
        if let Some(ref test_paths) = self.test_paths {
            paths.extend(test_paths.clone());
        }

        paths
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct _Dispatch {
    pub macro_namespace: String,
    pub search_order: Vec<String>,
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, DbtSchema)]
#[serde(untagged)]
pub enum QueryComment {
    String(String),
    Object(YmlValue),
}

/// Common interface for configs that have completed the resolution pipeline.
///
/// Implemented by every `Resolved*Config` config type (generated via `#[derive(Resolvable)]`) and by
/// trivially-resolved configs that implement it directly. All required fields are guaranteed
/// to be set — `enabled()` always returns `bool`, never `Option<bool>`.
///
/// The optional capability methods (`get_pre_hook`, `get_post_hook`, `get_static_analysis`)
/// default to `None` and are overridden only by config types that have those fields.
pub trait ResolvedConfig {
    fn enabled(&self) -> bool;
    fn get_pre_hook(&self) -> Option<&crate::schemas::common::Hooks> {
        None
    }
    fn get_post_hook(&self) -> Option<&crate::schemas::common::Hooks> {
        None
    }
    fn get_static_analysis(&self) -> Option<Spanned<dbt_common::io_args::StaticAnalysisKind>> {
        None
    }
}

/// Full config resolution lifecycle protocol.
///
/// Implement this on every config type that participates in the resolution pipeline.
/// The four lifecycle steps, in order:
/// 1. `default_to` — inherit unset fields from a parent config of the same type
/// 2. `apply_package_defaults` — seed package-level values (e.g. quoting) once per package
/// 3. `apply_resolve_defaults` — fill in runtime values (e.g. CLI flags) after all layers merge
/// 4. `finalize` — consume self and produce `Self::Resolved`, the post-resolution type
///
/// For configs with fields that need `Option<T>` → `T` promotion in the resolved type, use
/// `#[derive(Resolvable)]` to generate the `Resolved*Config` struct and `finalize_resolved()` helper.
/// For trivially-resolved configs (no fields to promote), implement `ResolvedConfig` directly
/// on the struct and set `type Resolved = Self`.
pub trait ResolvableConfig<T>:
    Serialize + DeserializeOwned + Default + Debug + Clone + Send + Sync
{
    /// Post-resolution type returned by `finalize()`.
    ///
    /// For configs with promoted fields use the `#[derive(Resolvable)]`-generated `Resolved*Config` struct.
    /// For trivially-resolved configs set `type Resolved = Self` and impl `ResolvedConfig` directly.
    type Resolved: Send + Sync + ResolvedConfig + Clone;

    /// Values seeded into the root config before parent→child resolution within a package
    /// (e.g. quoting, sync). Applied once per package; cross-package defaults belong in `finalize`.
    /// Use `()` for configs that need no package-level seeding.
    type PackageDefaults;

    /// Runtime values applied after all layers are merged and the root overlay is applied, just
    /// before `finalize()`. Supplied via `ProjectConfigResolver::with_resolve_defaults`.
    ///
    /// Use `()` for configs that need no post-resolution defaults.
    /// Use `StaticAnalysisKind` for configs that carry a `static_analysis` field.
    ///
    /// For fields with a fixed compile-time default (i.e. not dependent on runtime inputs),
    /// prefer the `#[resolved(promote, default = expr)]` or `#[resolved(promote)]` macro
    /// attributes on the struct instead of implementing `apply_resolve_defaults`.
    type ResolveDefaults: Default + Clone + Send + Sync;

    fn default_to(&mut self, parent: &T);

    /// Returns whether this node is enabled, defaulting to `true` if unset.
    fn get_enabled_with_default(&self) -> bool;

    fn apply_package_defaults(&mut self, defaults: Self::PackageDefaults);

    /// Called after all config layers (project, properties, inline) are merged and the root
    /// overlay is applied, but before `finalize()`. Use this to fill in fields that must always
    /// have a value but are not set by `apply_package_defaults` for dependency packages.
    fn apply_resolve_defaults(&mut self, _defaults: Self::ResolveDefaults) {}

    /// Forces `enabled` to `false` unconditionally.
    fn disable(&mut self);

    /// Consumes self and returns the resolved type.
    fn finalize(self) -> Self::Resolved
    where
        Self: Sized;
}

// Improved macro for simple field defaulting with mutable references
#[macro_export]
macro_rules! default_to {
    ($parent:ident, [$($field:ident),* $(,)?]) => {
        $(
            if $field.is_none() {
                *$field = $parent.$field.clone();
            }
        )*
    };
}

/// Yaml configs that can contain nested child configs of the same type.
pub trait TypedRecursiveConfig: Clone {
    /// Returns the type name of the config, e.g., "model", "source", etc.
    fn type_name() -> &'static str;

    /// Returns an iterator over the child configs.
    fn iter_children(&self) -> Iter<'_, String, ShouldBe<Self>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_project_id() {
        let project = DbtProject {
            name: "fishtown_internal_analytics".to_string(),
            version: Some(FloatOrString::String("1.0".to_string())),
            profile: Some("garage-snowflake".to_string()),
            analysis_paths: Some(vec![]),
            asset_paths: Some(vec![]),
            macro_paths: Some(vec![]),
            model_paths: Some(vec![]),
            function_paths: Some(vec![]),
            seed_paths: Some(vec![]),
            snapshot_paths: Some(vec![]),
            test_paths: Some(vec![]),
            docs_paths: Some(vec![]),
            target_path: Some(TargetPath::Target),
            log_path: Some(LogPath::Logs),
            packages_install_path: Some("packages".to_string()),
            metrics: None,
            models: None,
            functions: None,
            snapshots: None,
            seeds: None,
            sources: None,
            tests: None,
            unit_tests: None,
            data_tests: None,
            saved_queries: None,
            semantic_models: None,
            exposures: None,
            analyses: None,
            clean_targets: None,
            config_version: None,
            dbt_cloud: None,
            dispatch: None,
            flags: None,
            on_run_end: Verbatim::from(None),
            on_run_start: Verbatim::from(None),
            query_comment: Verbatim::from(None),
            quoting: Spanned::new(None),
            sync: None,
            require_dbt_version: None,
            restrict_access: None,
            vars: Verbatim::from(None),
        };
        assert_eq!(project.get_project_id(), "92c907bdbc0c4f27451b9b9fdb1bc8ec");
    }
}
