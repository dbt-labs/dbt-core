use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use crate::default_to;
use crate::schemas::common::{DbtQuoting, StoreFailuresAs};
use crate::schemas::project::configs::common::{default_meta_and_tags, default_quoting};
use crate::schemas::project::{DefaultTo, IterChildren};
use crate::schemas::serde::{bool_or_string_bool, StringOrArrayOfStrings};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectDataTestConfig {
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+database")]
    pub database: Option<String>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+error_if")]
    pub error_if: Option<String>,
    #[serde(rename = "+fail_calc")]
    pub fail_calc: Option<String>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+limit")]
    pub limit: Option<i32>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+severity")]
    pub severity: Option<String>,
    #[serde(
        default,
        rename = "+store_failures",
        deserialize_with = "bool_or_string_bool"
    )]
    pub store_failures: Option<bool>,
    #[serde(rename = "+store_failures_as")]
    pub store_failures_as: Option<StoreFailuresAs>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+warn_if")]
    pub warn_if: Option<String>,
    #[serde(rename = "+where")]
    pub where_: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<StaticAnalysisKind>,
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectDataTestConfig>>,
}

impl IterChildren<ProjectDataTestConfig> for ProjectDataTestConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema)]
pub struct DataTestConfig {
    pub alias: Option<String>,
    pub database: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub error_if: Option<String>,
    pub fail_calc: Option<String>,
    pub group: Option<String>,
    pub limit: Option<i32>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub schema: Option<String>,
    pub severity: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub store_failures: Option<bool>,
    pub store_failures_as: Option<StoreFailuresAs>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub warn_if: Option<String>,
    pub quoting: Option<DbtQuoting>,
    pub static_analysis: Option<StaticAnalysisKind>,
    #[serde(rename = "where")]
    pub where_: Option<String>,
}

impl From<ProjectDataTestConfig> for DataTestConfig {
    fn from(config: ProjectDataTestConfig) -> Self {
        Self {
            alias: config.alias,
            database: config.database,
            enabled: config.enabled,
            error_if: config.error_if,
            fail_calc: config.fail_calc,
            group: config.group,
            limit: config.limit,
            meta: config.meta,
            schema: config.schema,
            severity: config.severity,
            store_failures: config.store_failures,
            store_failures_as: config.store_failures_as,
            tags: config.tags,
            warn_if: config.warn_if,
            quoting: config.quoting,
            where_: config.where_,
            static_analysis: config.static_analysis,
        }
    }
}

impl From<DataTestConfig> for ProjectDataTestConfig {
    fn from(config: DataTestConfig) -> Self {
        Self {
            alias: config.alias,
            database: config.database,
            enabled: config.enabled,
            error_if: config.error_if,
            fail_calc: config.fail_calc,
            group: config.group,
            limit: config.limit,
            meta: config.meta,
            schema: config.schema,
            severity: config.severity,
            store_failures: config.store_failures,
            store_failures_as: config.store_failures_as,
            tags: config.tags,
            warn_if: config.warn_if,
            quoting: config.quoting,
            where_: config.where_,
            static_analysis: config.static_analysis,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<DataTestConfig> for DataTestConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &DataTestConfig) {
        let DataTestConfig {
            ref mut alias,
            ref mut database,
            ref mut enabled,
            ref mut error_if,
            ref mut fail_calc,
            ref mut group,
            ref mut limit,
            ref mut meta,
            ref mut schema,
            ref mut severity,
            ref mut store_failures,
            ref mut store_failures_as,
            ref mut tags,
            ref mut warn_if,
            ref mut quoting,
            ref mut where_,
            ref mut static_analysis,
        } = self;

        // Protect the mutable refs from being used in the default_to macro
        #[allow(unused, clippy::let_unit_value)]
        let quoting = default_quoting(quoting, &parent.quoting);
        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused, clippy::let_unit_value)]
        let tags = ();

        default_to!(
            parent,
            [
                enabled,
                store_failures,
                store_failures_as,
                limit,
                severity,
                error_if,
                warn_if,
                fail_calc,
                alias,
                database,
                schema,
                group,
                where_,
                static_analysis,
            ]
        );
    }

    fn database(&self) -> Option<String> {
        self.database.clone()
    }

    fn schema(&self) -> Option<String> {
        self.schema.clone()
    }

    fn alias(&self) -> Option<String> {
        self.alias.clone()
    }
}
