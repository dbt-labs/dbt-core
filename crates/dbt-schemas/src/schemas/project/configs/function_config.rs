use crate::schemas::serde::OmissibleGrantConfig;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::serde_utils::Omissible;
use dbt_yaml::DbtSchema;
use dbt_yaml::ShouldBe;
use dbt_yaml::Spanned;
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_yaml::Value;
use indexmap::IndexMap;
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;
use std::collections::btree_map::Iter;

use super::config_keys::ConfigKeys;
use super::omissible_utils::handle_omissible_override;

use dbt_proc_macros::Resolvable;

use crate::default_to;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::{Access, DbtQuoting};
use crate::schemas::project::configs::common::log_state_mod_diff;
// Import comparison helpers from common
use super::common::{
    access_eq, array_of_strings_eq, docs_eq, grants_eq, meta_eq, omissible_option_eq,
    same_warehouse_config,
};
use crate::schemas::project::configs::common::WarehouseSpecificNodeConfig;
use crate::schemas::project::configs::common::{
    default_meta_and_tags, default_packages, default_quoting, default_to_grants,
};
use crate::schemas::project::dbt_project::{
    ResolvableConfig, ResolvedConfig, TypedRecursiveConfig,
};
use crate::schemas::properties::{FunctionKind, Volatility};
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::{bool_or_string_bool, default_type};

fn default_function_kind() -> Option<FunctionKind> {
    Some(FunctionKind::Scalar)
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ProjectFunctionConfig {
    #[serde(rename = "+access")]
    pub access: Option<Access>,
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+database", alias = "+project")]
    pub database: Omissible<Option<String>>,
    #[serde(rename = "+description")]
    pub description: Option<String>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+grants")]
    pub grants: OmissibleGrantConfig,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<IndexMap<String, YmlValue>>,
    #[serde(rename = "+on_configuration_change")]
    pub on_configuration_change: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+schema")]
    pub schema: Omissible<Option<String>>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+type")]
    pub function_kind: Option<FunctionKind>,
    #[serde(rename = "+volatility")]
    pub volatility: Option<Volatility>,
    #[serde(rename = "+runtime_version")]
    pub runtime_version: Option<String>,
    #[serde(rename = "+entry_point")]
    pub entry_point: Option<String>,
    #[serde(rename = "+packages")]
    pub packages: Option<StringOrArrayOfStrings>,

    // Additional properties for directory structure
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectFunctionConfig>>,
}

impl Default for ProjectFunctionConfig {
    fn default() -> Self {
        Self {
            access: None,
            alias: None,
            database: Omissible::Omitted,
            description: None,
            docs: None,
            enabled: None,
            grants: OmissibleGrantConfig::default(),
            group: None,
            meta: None,
            on_configuration_change: None,
            quoting: None,
            schema: Omissible::Omitted,
            static_analysis: None,
            tags: None,
            function_kind: None,
            volatility: None,
            runtime_version: None,
            entry_point: None,
            packages: None,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl ResolvedConfig for ProjectFunctionConfig {
    fn enabled(&self) -> bool {
        true
    }
}

impl ResolvableConfig<ProjectFunctionConfig> for ProjectFunctionConfig {
    type Resolved = Self;
    type PackageDefaults = ();
    type ResolveDefaults = ();

    fn get_enabled_with_default(&self) -> bool {
        true
    }

    fn disable(&mut self) {}

    fn apply_package_defaults(&mut self, _: ()) {}

    fn finalize(self) -> Self {
        self
    }

    fn default_to(&mut self, parent: &ProjectFunctionConfig) {
        let ProjectFunctionConfig {
            access,
            alias,
            database,
            description,
            docs,
            enabled,
            grants,
            group,
            meta,
            on_configuration_change,
            quoting,
            schema,
            static_analysis,
            tags,
            function_kind,
            volatility,
            runtime_version,
            entry_point,
            packages,
            __additional_properties__: _,
        } = self;

        // Handle special cases
        default_quoting(quoting, &parent.quoting);
        default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        default_to_grants(grants, &parent.grants);
        handle_omissible_override(database, &parent.database);
        handle_omissible_override(schema, &parent.schema);
        #[allow(unused, clippy::let_unit_value)]
        let packages = default_packages(packages, &parent.packages);

        default_to!(
            parent,
            [
                access,
                alias,
                description,
                docs,
                enabled,
                group,
                on_configuration_change,
                static_analysis,
                function_kind,
                volatility,
                runtime_version,
                entry_point,
            ]
        );
    }
}

impl TypedRecursiveConfig for ProjectFunctionConfig {
    fn type_name() -> &'static str {
        "function"
    }

    fn iter_children(&self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Resolvable, Debug, Clone, Serialize, Deserialize, Default, PartialEq, DbtSchema)]
#[serde(rename_all = "snake_case")]
pub struct FunctionConfig {
    pub access: Option<Access>,
    #[resolved(promote, method = get_enabled_with_default)]
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub alias: Option<String>,
    pub database: Omissible<Option<String>>,
    pub schema: Omissible<Option<String>>,
    #[serde(
        default,
        serialize_with = "crate::schemas::nodes::serialize_none_as_empty_list"
    )]
    pub tags: Option<StringOrArrayOfStrings>,
    // need default to ensure None if field is not set
    #[serde(default, deserialize_with = "default_type")]
    pub meta: Option<IndexMap<String, YmlValue>>,
    pub group: Option<String>,
    pub docs: Option<DocsConfig>,
    pub grants: OmissibleGrantConfig,
    #[resolved(promote, expect = "quoting set by apply_package_defaults")]
    pub quoting: Option<DbtQuoting>,
    pub on_configuration_change: Option<String>,
    #[resolved(promote, expect = "static_analysis set by apply_resolve_defaults")]
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    #[serde(default = "default_function_kind", rename = "type")]
    pub function_kind: Option<FunctionKind>,
    pub volatility: Option<Volatility>,
    pub runtime_version: Option<String>,
    pub entry_point: Option<String>,
    pub packages: Option<StringOrArrayOfStrings>,

    // Warehouse-specific configurations
    pub __warehouse_specific_config__: WarehouseSpecificNodeConfig,
}

impl ResolvableConfig<FunctionConfig> for FunctionConfig {
    type Resolved = ResolvedFunctionConfig;
    type PackageDefaults = DbtQuoting;
    type ResolveDefaults = StaticAnalysisKind;

    fn get_enabled_with_default(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    fn disable(&mut self) {
        self.enabled = Some(false);
    }

    fn apply_package_defaults(&mut self, quoting: DbtQuoting) {
        if self.quoting.is_none() {
            self.quoting = Some(quoting);
        }
    }

    fn apply_resolve_defaults(&mut self, static_analysis: StaticAnalysisKind) {
        if self.static_analysis.is_none() {
            self.static_analysis = Some(Spanned::new(static_analysis));
        }
    }

    fn finalize(self) -> ResolvedFunctionConfig {
        self.finalize_resolved()
    }

    fn default_to(&mut self, parent: &FunctionConfig) {
        let FunctionConfig {
            access,
            enabled,
            alias,
            database,
            schema,
            tags,
            meta,
            group,
            docs,
            grants,
            quoting,
            on_configuration_change,
            static_analysis,
            function_kind,
            volatility,
            runtime_version,
            entry_point,
            packages,
            __warehouse_specific_config__: warehouse_config,
        } = self;

        // Handle warehouse config
        warehouse_config.default_to(&parent.__warehouse_specific_config__);

        // Handle omissible database and schema fields separately
        handle_omissible_override(database, &parent.database);
        handle_omissible_override(schema, &parent.schema);

        // Handle grants with custom merge logic
        default_to_grants(grants, &parent.grants);
        #[allow(unused, clippy::let_unit_value)]
        let packages = default_packages(packages, &parent.packages);

        default_to!(
            parent,
            [
                access,
                enabled,
                alias,
                tags,
                meta,
                group,
                docs,
                quoting,
                on_configuration_change,
                static_analysis,
                function_kind,
                volatility,
                runtime_version,
                entry_point,
            ]
        );
    }
}

impl From<ProjectFunctionConfig> for FunctionConfig {
    fn from(config: ProjectFunctionConfig) -> Self {
        Self {
            access: config.access,
            enabled: config.enabled,
            alias: config.alias,
            database: config.database,
            schema: config.schema,
            tags: config.tags,
            meta: config.meta,
            group: config.group,
            docs: config.docs,
            grants: config.grants,
            quoting: config.quoting,
            on_configuration_change: config.on_configuration_change,
            static_analysis: config.static_analysis,
            function_kind: config.function_kind,
            volatility: config.volatility,
            runtime_version: config.runtime_version,
            entry_point: config.entry_point,
            packages: config.packages,
            __warehouse_specific_config__: WarehouseSpecificNodeConfig::default(),
        }
    }
}

impl FunctionConfig {
    /// Custom comparison that treats Omitted and Present(None) as equivalent for schema/database fields
    pub fn same_config(&self, other: &FunctionConfig) -> bool {
        // Compare all fields individually
        let enabled_eq = self.enabled == other.enabled;
        let alias_eq = self.alias == other.alias;
        let schema_eq = omissible_option_eq(&self.schema, &other.schema); // Custom comparison for Omissible
        let tags_eq = self.tags == other.tags;
        let meta_eq_result = meta_eq(&self.meta, &other.meta); // Custom comparison for meta
        let group_eq = self.group == other.group;
        let docs_eq_result = docs_eq(&self.docs, &other.docs); // Custom comparison for docs
        let grants_eq_result = grants_eq(&self.grants, &other.grants); // Custom comparison for grants
        let quoting_eq = self.quoting == other.quoting;
        let on_configuration_change_eq =
            self.on_configuration_change == other.on_configuration_change;
        let static_analysis_eq = self.static_analysis == other.static_analysis;
        let function_kind_eq = self.function_kind == other.function_kind;
        let volatility_eq = self.volatility == other.volatility;
        let access_eq_result = access_eq(&self.access, &other.access); // Custom comparison for access
        let packages_eq = array_of_strings_eq(&self.packages, &other.packages);
        let warehouse_config_eq = same_warehouse_config(
            &self.__warehouse_specific_config__,
            &other.__warehouse_specific_config__,
        );

        let result = enabled_eq
            && alias_eq
            && schema_eq
            && tags_eq
            && meta_eq_result
            && group_eq
            && docs_eq_result
            && grants_eq_result
            && quoting_eq
            && on_configuration_change_eq
            && static_analysis_eq
            && function_kind_eq
            && volatility_eq
            && access_eq_result
            && packages_eq
            && warehouse_config_eq;

        if !result {
            log_state_mod_diff(
                "unique_id in next function_config log",
                "function_config",
                [
                    (
                        "enabled",
                        enabled_eq,
                        Some((
                            format!("{:?}", &self.enabled),
                            format!("{:?}", &other.enabled),
                        )),
                    ),
                    (
                        "alias",
                        alias_eq,
                        Some((format!("{:?}", &self.alias), format!("{:?}", &other.alias))),
                    ),
                    (
                        "schema",
                        schema_eq,
                        Some((
                            format!("{:?}", &self.schema),
                            format!("{:?}", &other.schema),
                        )),
                    ),
                    (
                        "tags",
                        tags_eq,
                        Some((format!("{:?}", &self.tags), format!("{:?}", &other.tags))),
                    ),
                    (
                        "meta",
                        meta_eq_result,
                        Some((format!("{:?}", &self.meta), format!("{:?}", &other.meta))),
                    ),
                    (
                        "group",
                        group_eq,
                        Some((format!("{:?}", &self.group), format!("{:?}", &other.group))),
                    ),
                    ("docs", docs_eq_result, None),
                    (
                        "grants",
                        grants_eq_result,
                        Some((
                            format!("{:?}", &self.grants),
                            format!("{:?}", &other.grants),
                        )),
                    ),
                    (
                        "quoting",
                        quoting_eq,
                        Some((
                            format!("{:?}", &self.quoting),
                            format!("{:?}", &other.quoting),
                        )),
                    ),
                    (
                        "on_configuration_change",
                        on_configuration_change_eq,
                        Some((
                            format!("{:?}", &self.on_configuration_change),
                            format!("{:?}", &other.on_configuration_change),
                        )),
                    ),
                    (
                        "static_analysis",
                        static_analysis_eq,
                        Some((
                            format!("{:?}", &self.static_analysis),
                            format!("{:?}", &other.static_analysis),
                        )),
                    ),
                    (
                        "function_kind",
                        function_kind_eq,
                        Some((
                            format!("{:?}", &self.function_kind),
                            format!("{:?}", &other.function_kind),
                        )),
                    ),
                    (
                        "volatility",
                        volatility_eq,
                        Some((
                            format!("{:?}", &self.volatility),
                            format!("{:?}", &other.volatility),
                        )),
                    ),
                    (
                        "access",
                        access_eq_result,
                        Some((
                            format!("{:?}", &self.access),
                            format!("{:?}", &other.access),
                        )),
                    ),
                    (
                        "packages",
                        packages_eq,
                        Some((
                            format!("{:?}", &self.packages),
                            format!("{:?}", &other.packages),
                        )),
                    ),
                    ("warehouse_config", warehouse_config_eq, None),
                ],
            );
        }

        result
    }
}

impl ConfigKeys for FunctionConfig {
    // The default implementation from the trait will handle
    // extracting field names via serialization automatically
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::project::dbt_project::ResolvableConfig;
    use crate::schemas::serde::StringOrArrayOfStrings;

    #[test]
    fn test_function_config_packages_append() {
        let parent = FunctionConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "pandas".to_string(),
            ])),
            ..Default::default()
        };

        let mut child = FunctionConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "matplotlib".to_string(),
            ])),
            ..Default::default()
        };

        child.default_to(&parent);

        assert_eq!(
            child.packages,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "pandas".to_string(),
                "matplotlib".to_string(),
            ]))
        );
    }

    #[test]
    fn test_function_config_packages_none_child_inherits_parent() {
        let parent = FunctionConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
            ])),
            ..Default::default()
        };

        let mut child = FunctionConfig {
            packages: None,
            ..Default::default()
        };

        child.default_to(&parent);

        assert_eq!(
            child.packages,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
            ]))
        );
    }

    #[test]
    fn test_function_config_packages_same_config() {
        let a = FunctionConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
            ])),
            ..Default::default()
        };

        let b = FunctionConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
            ])),
            ..Default::default()
        };

        assert!(a.same_config(&b));

        let c = FunctionConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "pandas".to_string(),
            ])),
            ..Default::default()
        };

        assert!(!a.same_config(&c));
    }
}
