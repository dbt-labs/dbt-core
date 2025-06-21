use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::ShouldBe;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use crate::default_to;
use crate::schemas::common::DbtMaterialization;
use crate::schemas::common::DbtQuoting;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::HardDeletes;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::project::configs::common::default_hooks;
use crate::schemas::project::configs::common::default_meta_and_tags;
use crate::schemas::project::configs::common::default_quoting;
use crate::schemas::project::DefaultTo;
use crate::schemas::project::IterChildren;
use crate::schemas::serde::bool_or_string_bool;
use crate::schemas::serde::StringOrArrayOfStrings;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSnapshotConfig {
    // Snapshot-specific Configuration
    #[serde(rename = "+database")]
    pub database: Option<String>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+materialized")]
    pub materialized: Option<DbtMaterialization>,
    #[serde(rename = "+strategy")]
    pub strategy: Option<String>,
    #[serde(rename = "+unique_key")]
    pub unique_key: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+check_cols")]
    pub check_cols: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+updated_at")]
    pub updated_at: Option<String>,
    #[serde(rename = "+dbt_valid_to_current")]
    pub dbt_valid_to_current: Option<String>,
    #[serde(rename = "+snapshot_meta_column_names")]
    pub snapshot_meta_column_names: Option<SnapshotMetaColumnNames>,
    #[serde(rename = "+hard_deletes")]
    pub hard_deletes: Option<HardDeletes>,
    // General Configuration
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+pre-hook")]
    pub pre_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+post-hook")]
    pub post_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+persist_docs")]
    pub persist_docs: Option<PersistDocsConfig>,
    #[serde(rename = "+grants")]
    pub grants: Option<serde_json::Value>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(
        default,
        rename = "+quote_columns",
        deserialize_with = "bool_or_string_bool"
    )]
    pub quote_columns: Option<bool>,
    #[serde(rename = "+invalidate_hard_deletes")]
    pub invalidate_hard_deletes: Option<bool>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    // Flattened field:
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSnapshotConfig>>,
}

impl IterChildren<ProjectSnapshotConfig> for ProjectSnapshotConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Default, PartialEq, Eq)]
pub struct SnapshotConfig {
    // Snapshot-specific Configuration
    pub database: Option<String>,
    pub schema: Option<String>,
    pub alias: Option<String>,
    pub materialized: Option<DbtMaterialization>,
    pub strategy: Option<String>,
    pub unique_key: Option<StringOrArrayOfStrings>,
    pub check_cols: Option<StringOrArrayOfStrings>,
    pub updated_at: Option<String>,
    pub dbt_valid_to_current: Option<String>,
    pub snapshot_meta_column_names: Option<SnapshotMetaColumnNames>,
    pub hard_deletes: Option<HardDeletes>,
    // General Configuration
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub pre_hook: Verbatim<Option<Hooks>>,
    pub post_hook: Verbatim<Option<Hooks>>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub grants: Option<serde_json::Value>,
    pub event_time: Option<String>,
    pub quoting: Option<DbtQuoting>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub group: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub quote_columns: Option<bool>,
    pub invalidate_hard_deletes: Option<bool>,
    pub docs: Option<DocsConfig>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct SnapshotMetaColumnNames {
    #[serde(default = "default_dbt_scd_id")]
    pub dbt_scd_id: Option<String>,
    #[serde(default = "default_dbt_updated_at")]
    pub dbt_updated_at: Option<String>,
    #[serde(default = "default_dbt_valid_from")]
    pub dbt_valid_from: Option<String>,
    #[serde(default = "default_dbt_valid_to")]
    pub dbt_valid_to: Option<String>,
    #[serde(default = "default_dbt_is_deleted")]
    pub dbt_is_deleted: Option<String>,
}

impl Default for SnapshotMetaColumnNames {
    fn default() -> Self {
        Self {
            dbt_scd_id: default_dbt_scd_id(),
            dbt_updated_at: default_dbt_updated_at(),
            dbt_valid_from: default_dbt_valid_from(),
            dbt_valid_to: default_dbt_valid_to(),
            dbt_is_deleted: default_dbt_is_deleted(),
        }
    }
}

fn default_dbt_scd_id() -> Option<String> {
    Some("DBT_SCD_ID".to_string())
}

fn default_dbt_updated_at() -> Option<String> {
    Some("DBT_UPDATED_AT".to_string())
}

fn default_dbt_valid_from() -> Option<String> {
    Some("DBT_VALID_FROM".to_string())
}

fn default_dbt_valid_to() -> Option<String> {
    Some("DBT_VALID_TO".to_string())
}

fn default_dbt_is_deleted() -> Option<String> {
    Some("DBT_IS_DELETED".to_string())
}

impl From<ProjectSnapshotConfig> for SnapshotConfig {
    fn from(config: ProjectSnapshotConfig) -> Self {
        Self {
            database: config.database,
            schema: config.schema,
            alias: config.alias,
            materialized: config.materialized,
            strategy: config.strategy,
            unique_key: config.unique_key,
            check_cols: config.check_cols,
            updated_at: config.updated_at,
            dbt_valid_to_current: config.dbt_valid_to_current,
            snapshot_meta_column_names: config.snapshot_meta_column_names,
            hard_deletes: config.hard_deletes,
            enabled: config.enabled,
            tags: config.tags,
            pre_hook: config.pre_hook,
            post_hook: config.post_hook,
            persist_docs: config.persist_docs,
            grants: config.grants,
            event_time: config.event_time,
            quoting: config.quoting,
            meta: config.meta,
            group: config.group,
            quote_columns: config.quote_columns,
            invalidate_hard_deletes: config.invalidate_hard_deletes,
            docs: config.docs,
        }
    }
}

impl From<SnapshotConfig> for ProjectSnapshotConfig {
    fn from(config: SnapshotConfig) -> Self {
        Self {
            database: config.database,
            schema: config.schema,
            alias: config.alias,
            materialized: config.materialized,
            strategy: config.strategy,
            unique_key: config.unique_key,
            check_cols: config.check_cols,
            updated_at: config.updated_at,
            dbt_valid_to_current: config.dbt_valid_to_current,
            snapshot_meta_column_names: config.snapshot_meta_column_names,
            hard_deletes: config.hard_deletes,
            enabled: config.enabled,
            tags: config.tags,
            pre_hook: config.pre_hook,
            post_hook: config.post_hook,
            persist_docs: config.persist_docs,
            grants: config.grants,
            event_time: config.event_time,
            quoting: config.quoting,
            meta: config.meta,
            group: config.group,
            quote_columns: config.quote_columns,
            invalidate_hard_deletes: config.invalidate_hard_deletes,
            docs: config.docs,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<SnapshotConfig> for SnapshotConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn get_pre_hook(&self) -> Option<&Hooks> {
        self.pre_hook.as_ref()
    }

    fn get_post_hook(&self) -> Option<&Hooks> {
        self.post_hook.as_ref()
    }

    fn default_to(&mut self, parent: &SnapshotConfig) {
        let SnapshotConfig {
            ref mut database,
            ref mut schema,
            ref mut alias,
            ref mut materialized,
            ref mut strategy,
            ref mut unique_key,
            ref mut check_cols,
            ref mut updated_at,
            ref mut dbt_valid_to_current,
            ref mut snapshot_meta_column_names,
            ref mut hard_deletes,
            ref mut enabled,
            ref mut tags,
            ref mut pre_hook,
            ref mut post_hook,
            ref mut persist_docs,
            ref mut grants,
            ref mut event_time,
            ref mut quoting,
            ref mut meta,
            ref mut group,
            ref mut quote_columns,
            ref mut invalidate_hard_deletes,
            ref mut docs,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let pre_hook = default_hooks(pre_hook, &parent.pre_hook);
        #[allow(unused, clippy::let_unit_value)]
        let post_hook = default_hooks(post_hook, &parent.post_hook);
        #[allow(unused, clippy::let_unit_value)]
        let quoting = default_quoting(quoting, &parent.quoting);
        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused, clippy::let_unit_value)]
        let tags = ();

        // Use the improved default_to macro for simple fields
        default_to!(
            parent,
            [
                enabled,
                alias,
                schema,
                database,
                materialized,
                group,
                persist_docs,
                unique_key,
                grants,
                docs,
                event_time,
                quote_columns,
                invalidate_hard_deletes,
                strategy,
                updated_at,
                dbt_valid_to_current,
                snapshot_meta_column_names,
                hard_deletes,
                check_cols,
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
