//! Common Definitions for use in serializing and deserializing dbt nodes

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use dbt_common::{CodeLocation, ErrorCode, FsError, FsResult, err, fs_err};
use dbt_frontend_common::Dialect;
use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize, Verbatim};
use hex;
use serde::{Deserialize, Deserializer, Serialize};
// Type alias for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use sha2::{Digest, Sha256};
use strum::{Display, EnumIter, EnumString};

use crate::dbt_types::RelationType;

use super::serde::StringOrArrayOfStrings;
#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct FreshnessRules {
    pub count: Option<i64>,
    pub period: Option<FreshnessPeriod>,
}

impl FreshnessRules {
    pub fn validate(rule: Option<&Self>) -> FsResult<()> {
        if rule.is_none() {
            return Ok(());
        }
        let rule = rule.expect("rule should be Some now");
        if rule.count.is_none() || rule.period.is_none() {
            return Err(fs_err!(
                ErrorCode::InvalidArgument,
                "count and period are required when freshness is provided, count: {:?}, period: {:?}",
                rule.count,
                rule.period
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum UpdatesOn {
    #[default]
    Any,
    All,
}

impl std::fmt::Display for UpdatesOn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdatesOn::Any => write!(f, "any"),
            UpdatesOn::All => write!(f, "all"),
        }
    }
}

impl FromStr for UpdatesOn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => Ok(UpdatesOn::Any),
            "all" => Ok(UpdatesOn::All),
            _ => Err(format!("Unknown UpdatesOn value: {s}")),
        }
    }
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct ModelFreshnessRules {
    pub count: Option<i64>,
    pub period: Option<FreshnessPeriod>,
    pub updates_on: Option<UpdatesOn>,
}

impl ModelFreshnessRules {
    pub fn validate(rule: Option<&Self>) -> FsResult<()> {
        if rule.is_none() {
            return Ok(());
        }
        let rule = rule.expect("rule should be Some now");
        if rule.count.is_none() || rule.period.is_none() {
            return Err(fs_err!(
                ErrorCode::InvalidArgument,
                "count and period are required when freshness is provided, count: {:?}, period: {:?}",
                rule.count,
                rule.period
            ));
        }
        Ok(())
    }

    /// Convert the freshness duration to seconds
    pub fn to_seconds(&self) -> i64 {
        let count = self.count.expect("count is required");
        let period = self.period.as_ref().expect("period is required");
        count
            * match period {
                FreshnessPeriod::minute => 60,
                FreshnessPeriod::hour => 60 * 60,
                FreshnessPeriod::day => 60 * 60 * 24,
            }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum FreshnessPeriod {
    minute,
    hour,
    day,
}
impl FromStr for FreshnessPeriod {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "minute" => Ok(FreshnessPeriod::minute),
            "hour" => Ok(FreshnessPeriod::hour),
            "day" => Ok(FreshnessPeriod::day),
            _ => Err(()),
        }
    }
}
impl std::fmt::Display for FreshnessPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let period_str = match self {
            FreshnessPeriod::minute => "minute",
            FreshnessPeriod::hour => "hour",
            FreshnessPeriod::day => "day",
        };
        write!(f, "{period_str}")
    }
}

// We don't skip serializing none here because dbt project evaluator checks for the presence of either error_after or warn_after
// https://github.com/dbt-labs/dbt-project-evaluator/blob/94768b117573705e95a9456273de8e358efadb00/macros/unpack/get_source_values.sql#L27-L28
#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct FreshnessDefinition {
    #[serde(default, serialize_with = "serialize_freshness_rule")]
    pub error_after: Option<FreshnessRules>,
    #[serde(default, serialize_with = "serialize_freshness_rule")]
    pub warn_after: Option<FreshnessRules>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// Custom serializer to ensure FreshnessRules are always objects, never null
fn serialize_freshness_rule<S>(
    rule: &Option<FreshnessRules>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match rule {
        Some(rule) => rule.serialize(serializer),
        None => FreshnessRules::default().serialize(serializer),
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum FreshnessStatus {
    Pass,
    Warn,
    Error,
}

/// Trait for types that can be merged, taking the last non-None value
pub trait Merge<T> {
    /// Merge with another instance, where the other's non-None values overwrite self
    fn merge(&self, other: &T) -> Self;
}

// Generic implementation for Option<T> where T: Clone
impl<T: Clone + Merge<T>> Merge<Option<T>> for Option<T> {
    fn merge(&self, other: &Option<T>) -> Self {
        match (self, other) {
            (Some(s), Some(o)) => Some(s.merge(o)),
            (None, Some(o)) => Some(o.clone()),
            (Some(s), None) => Some(s.clone()),
            (None, None) => None,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, EnumIter, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DbtMaterialization {
    #[default]
    View,
    Table,
    Incremental,
    MaterializedView,
    External,
    Test,
    Ephemeral,
    Unit,
    Analysis,
    /// only for databricks
    StreamingTable,
    /// only for snowflake
    DynamicTable,
    #[serde(untagged)]
    Unknown(String),
}
impl FromStr for DbtMaterialization {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "view" => Ok(DbtMaterialization::View),
            "table" => Ok(DbtMaterialization::Table),
            "incremental" => Ok(DbtMaterialization::Incremental),
            "materialized_view" => Ok(DbtMaterialization::MaterializedView),
            "external" => Ok(DbtMaterialization::External),
            "test" => Ok(DbtMaterialization::Test),
            "ephemeral" => Ok(DbtMaterialization::Ephemeral),
            "unit" => Ok(DbtMaterialization::Unit),
            "analysis" => Ok(DbtMaterialization::Analysis),
            "streaming_table" => Ok(DbtMaterialization::StreamingTable),
            "dynamic_table" => Ok(DbtMaterialization::DynamicTable),
            other => Ok(DbtMaterialization::Unknown(other.to_string())),
        }
    }
}
impl From<DbtMaterialization> for String {
    fn from(materialization: DbtMaterialization) -> Self {
        materialization.to_string()
    }
}

impl std::fmt::Display for DbtMaterialization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let materialized_str = match self {
            DbtMaterialization::View => "view",
            DbtMaterialization::Table => "table",
            DbtMaterialization::Incremental => "incremental",
            DbtMaterialization::MaterializedView => "materialized_view",
            DbtMaterialization::External => "external",
            DbtMaterialization::Test => "test",
            DbtMaterialization::Ephemeral => "ephemeral",
            DbtMaterialization::Unit => "unit",
            DbtMaterialization::StreamingTable => "streaming_table",
            DbtMaterialization::DynamicTable => "dynamic_table",
            DbtMaterialization::Analysis => "analysis",
            DbtMaterialization::Unknown(s) => s.as_str(),
        };
        write!(f, "{materialized_str}")
    }
}

// Question (Ani): does this map correctly?
impl From<DbtMaterialization> for RelationType {
    fn from(materialization: DbtMaterialization) -> Self {
        match materialization {
            DbtMaterialization::Table => RelationType::Table,
            DbtMaterialization::View => RelationType::View,
            DbtMaterialization::MaterializedView => RelationType::MaterializedView,
            DbtMaterialization::Ephemeral => RelationType::Ephemeral,
            DbtMaterialization::External => RelationType::External,
            DbtMaterialization::Test => RelationType::External, // TODO Validate this
            DbtMaterialization::Incremental => RelationType::External, // TODO Validate this
            DbtMaterialization::Unit => RelationType::External, // TODO Validate this
            DbtMaterialization::StreamingTable => RelationType::StreamingTable,
            DbtMaterialization::DynamicTable => RelationType::DynamicTable,
            DbtMaterialization::Analysis => RelationType::External, // TODO Validate this
            DbtMaterialization::Unknown(_) => RelationType::External, // TODO Validate this
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Display, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Access {
    Private,
    #[default]
    Protected,
    Public,
}

impl FromStr for Access {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "private" => Ok(Access::Private),
            "protected" => Ok(Access::Protected),
            "public" => Ok(Access::Public),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct NodeDependsOn {
    #[serde(default)]
    pub macros: Vec<String>,
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub nodes_with_ref_location: Vec<(String, CodeLocation)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ResolvedQuoting {
    pub database: bool,
    pub identifier: bool,
    pub schema: bool,
}

impl Default for ResolvedQuoting {
    fn default() -> Self {
        // dbt rules
        Self::trues()
        // todo: however a much more sensible rule would be
        // Self::falses()
        // ... since SQL is case insensitive -- so let the dialect dictate and not the user...
    }
}

impl ResolvedQuoting {
    pub fn trues() -> Self {
        ResolvedQuoting {
            database: true,
            identifier: true,
            schema: true,
        }
    }
    pub fn falses() -> Self {
        ResolvedQuoting {
            database: false,
            identifier: false,
            schema: false,
        }
    }
}

impl TryFrom<DbtQuoting> for ResolvedQuoting {
    type Error = Box<FsError>;

    fn try_from(value: DbtQuoting) -> FsResult<Self> {
        Ok(ResolvedQuoting {
            database: value.database.ok_or(fs_err!(
                ErrorCode::InvalidArgument,
                "Missing database in dbt quoting config. Failed to convert to ResolvedQuoting."
            ))?,
            identifier: value.identifier.ok_or(fs_err!(
                ErrorCode::InvalidArgument,
                "Missing identifier in dbt quoting config. Failed to convert to ResolvedQuoting."
            ))?,
            schema: value.schema.ok_or(fs_err!(
                ErrorCode::InvalidArgument,
                "Missing schema in dbt quoting config. Failed to convert to ResolvedQuoting."
            ))?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, PartialEq, Eq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DbtQuoting {
    pub database: Option<bool>,
    pub identifier: Option<bool>,
    pub schema: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snowflake_ignore_case: Option<bool>,
}

impl DbtQuoting {
    pub fn is_default(&self) -> bool {
        self.database.is_none() && self.identifier.is_none() && self.schema.is_none()
    }

    pub fn default_to(&mut self, other: &DbtQuoting) {
        self.database = self.database.or(other.database);
        self.identifier = self.identifier.or(other.identifier);
        self.schema = self.schema.or(other.schema);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbtCheckColsSpec {
    /// A list of column names to be used for checking if a snapshot's source data set was updated
    Cols(Vec<String>),
    /// Use all columns to check whether a snapshot's source data set was updated
    All,
}

impl Serialize for DbtCheckColsSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DbtCheckColsSpec::Cols(cols) => cols.serialize(serializer),
            DbtCheckColsSpec::All => "all".serialize(serializer),
        }
    }
}

impl TryFrom<StringOrArrayOfStrings> for DbtCheckColsSpec {
    type Error = Box<FsError>;
    fn try_from(value: StringOrArrayOfStrings) -> Result<Self, Self::Error> {
        match value {
            StringOrArrayOfStrings::String(all) => {
                if all == "all" {
                    Ok(DbtCheckColsSpec::All)
                } else {
                    err!(ErrorCode::Generic, "Invalid check_cols value: {}", all)
                }
            }
            StringOrArrayOfStrings::ArrayOfStrings(cols) => Ok(DbtCheckColsSpec::Cols(cols)),
        }
    }
}
impl<'de> Deserialize<'de> for DbtCheckColsSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = YmlValue::deserialize(deserializer)?;
        match value {
            YmlValue::String(all, _) => {
                // Validate that all is 'all'
                if all != "all" {
                    return Err(serde::de::Error::custom("Expected 'all'"));
                }
                Ok(DbtCheckColsSpec::All)
            }
            YmlValue::Sequence(col_list, _) => {
                let cols: Result<Vec<_>, D::Error> = col_list
                    .into_iter()
                    .map(|v| match v {
                        YmlValue::String(s, _) => Ok(s),
                        _ => Err(serde::de::Error::custom("Expected array of strings")),
                    })
                    .collect();
                match cols {
                    Ok(col_names) => Ok(DbtCheckColsSpec::Cols(col_names.into_iter().collect())),
                    Err(_) => Err(serde::de::Error::custom("Expected array of strings")),
                }
            }
            _ => Err(serde::de::Error::custom(format!(
                "Expected a string or array of strings for check_cols, got {value:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumString, Display, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DbtBatchSize {
    Hour,
    Day,
    Month,
    Year,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct DbtContract {
    #[serde(default = "default_alias_types")]
    pub alias_types: bool,
    #[serde(default)]
    pub enforced: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<YmlValue>,
}

fn default_alias_types() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumString, Display, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DbtIncrementalStrategy {
    Append,
    Merge,
    #[serde(rename = "delete+insert")]
    #[strum(serialize = "delete+insert")]
    DeleteInsert,
    InsertOverwrite,
    Microbatch,
    /// replace_where (Databricks only)
    /// see https://docs.getdbt.com/reference/resource-configs/databricks-configs
    ReplaceWhere,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum DbtUniqueKey {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    Ignore,
    AppendNewColumns,
    Fail,
    SyncAllColumns,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnConfigurationChange {
    Apply,
    Continue,
    Fail,
    #[serde(other)]
    Unknown,
}

impl From<StringOrArrayOfStrings> for DbtUniqueKey {
    fn from(value: StringOrArrayOfStrings) -> Self {
        match value {
            StringOrArrayOfStrings::String(s) => DbtUniqueKey::Single(s),
            StringOrArrayOfStrings::ArrayOfStrings(v) => DbtUniqueKey::Multiple(v),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HardDeletes {
    Ignore,
    Invalidate,
    NewRecord,
}

// Impl try from string
impl TryFrom<String> for HardDeletes {
    type Error = Box<FsError>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(match value.as_str() {
            "ignore" => HardDeletes::Ignore,
            "invalidate" => HardDeletes::Invalidate,
            "new_record" => HardDeletes::NewRecord,
            _ => return err!(ErrorCode::Generic, "Invalid hard_deletes value: {}", value),
        })
    }
}

/// Constraints (model level or column level)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Constraint {
    #[serde(rename = "type")]
    pub type_: ConstraintType,
    pub expression: Option<String>,
    pub name: Option<String>,
    // Only ForeignKey constraints accept: a relation input
    // ref(), source() etc
    pub to: Option<String>,
    /// Only ForeignKey constraints accept: a list columns in that table
    /// containing the corresponding primary or unique key.
    pub to_columns: Option<Vec<String>>,
    pub warn_unsupported: Option<bool>,
    pub warn_unenforced: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintSupport {
    Enforced,
    NotEnforced,
    NotSupported,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintType {
    #[default]
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey,
    Check,
    Custom,
}

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize)]
#[serde(untagged)]
pub enum DbtChecksum {
    String(String),
    Object(DbtChecksumObject),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtChecksumObject {
    pub name: String,
    pub checksum: String,
}

impl Default for DbtChecksum {
    fn default() -> Self {
        Self::String("".to_string())
    }
}

impl PartialEq for DbtChecksum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(s1), Self::String(s2)) => s1 == s2,
            (Self::Object(o1), Self::Object(o2)) => {
                o1.name.to_lowercase() == o2.name.to_lowercase() && o1.checksum == o2.checksum
            }
            (Self::String(c1), Self::Object(o2)) => *c1 == o2.checksum,
            (Self::Object(o1), Self::String(c2)) => o1.checksum == *c2,
        }
    }
}

impl DbtChecksum {
    pub fn hash(s: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(s);
        let checksum = hasher.finalize();
        Self::Object(DbtChecksumObject {
            name: "SHA256".to_string(),
            checksum: hex::encode(checksum),
        })
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct IncludeExclude {
    pub exclude: Option<StringOrArrayOfStrings>,
    pub include: Option<StringOrArrayOfStrings>,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, Default)]
pub struct Expect {
    pub rows: Option<Rows>,
    #[serde(default)]
    pub format: Formats,
    pub fixture: Option<String>,
}

#[derive(
    Debug, Serialize, Default, Deserialize, Clone, EnumString, Display, JsonSchema, PartialEq,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Formats {
    #[default]
    Dict,
    Csv,
    Sql,
}

#[derive(Debug, Serialize, Default, Deserialize, Clone, JsonSchema)]
pub struct Given {
    pub input: Spanned<String>,
    pub rows: Option<Rows>,
    #[serde(default)]
    pub format: Formats,
    pub fixture: Option<String>,
}

#[derive(Debug, Serialize, UntaggedEnumDeserialize, Clone, JsonSchema)]
#[serde(untagged)]
pub enum Rows {
    String(String),
    List(Vec<BTreeMap<String, YmlValue>>),
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Default, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct DocsConfig {
    #[serde(default = "default_show")]
    pub show: bool,
    pub node_color: Option<String>,
}

fn default_show() -> bool {
    true
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct PersistDocsConfig {
    pub columns: Option<bool>,
    pub relation: Option<bool>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct ScheduleConfig {
    pub cron: Option<String>,
    pub time_zone_value: Option<String>,
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum Hooks {
    String(String),
    ArrayOfStrings(Vec<String>),
    HookConfig(HookConfig),
    HookConfigArray(Vec<HookConfig>),
}

impl Hooks {
    pub fn extend(&mut self, other: &Self) {
        let mut new_hooks = vec![];
        match other {
            Hooks::String(s) => {
                new_hooks.push(HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                });
            }
            Hooks::ArrayOfStrings(v) => {
                new_hooks.extend(v.iter().map(|s| HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                }));
            }
            Hooks::HookConfig(hook_config) => {
                new_hooks.push(hook_config.clone());
            }
            Hooks::HookConfigArray(hook_configs) => {
                new_hooks.extend(hook_configs.clone());
            }
        }
        match self {
            Hooks::String(s) => {
                new_hooks.push(HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                });
            }
            Hooks::ArrayOfStrings(v) => {
                new_hooks.extend(v.iter().map(|s| HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                }));
            }
            Hooks::HookConfig(hook_config) => {
                new_hooks.push(hook_config.clone());
            }
            Hooks::HookConfigArray(hook_configs) => {
                new_hooks.extend(hook_configs.clone());
            }
        }
        *self = Hooks::HookConfigArray(new_hooks);
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct HookConfig {
    pub sql: Option<String>,
    pub transaction: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Dimension {
    pub name: String,
    #[serde(rename = "type")]
    pub dimension_type: DimensionType,
    pub description: Option<String>,
    pub label: Option<String>,
    #[serde(default = "default_false")]
    pub is_partition: bool,
    pub type_params: Option<DimensionTypeParams>,
    pub expr: Option<String>,
    pub metadata: Option<YmlValue>,
    pub config: Option<DimensionConfig>,
}
fn default_false() -> bool {
    false
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DimensionType {
    Categorical,
    Time,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DimensionTypeParams {
    pub time_granularity: Option<TimeGranularity>,
    pub validity_params: Option<DimensionValidityParams>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct DimensionConfig {
    pub meta: BTreeMap<String, YmlValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct DimensionValidityParams {
    #[serde(default = "default_false")]
    pub is_start: bool,
    #[serde(default = "default_false")]
    pub is_end: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct SemanticModelDependsOn {
    pub macros: Vec<String>,
    pub nodes: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TimeGranularity {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoreFailuresAs {
    #[default]
    Ephemeral,
    Table,
    View,
}

#[derive(Debug, Serialize, Default, Deserialize, Clone, EnumString, Display, JsonSchema)]
#[strum(serialize_all = "lowercase")]
pub enum Severity {
    #[default]
    #[serde(alias = "error", alias = "ERROR")]
    Error,
    #[serde(alias = "warn", alias = "WARN")]
    Warn,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Versions {
    pub v: YmlValue,
    pub config: Verbatim<Option<dbt_serde_yaml::Value>>,
    pub __additional_properties__: Verbatim<HashMap<String, YmlValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfoWrapper {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_nodes: Option<i32>,
    pub node_info: NodeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_name: String,
    pub unique_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_finished_at: Option<String>,
    pub node_status: String,
}

/// Get the semantic names for database, schema, and identifier
/// This function will parse the database, schema, and identifier
/// according to the dialect and quoting rules.
pub fn normalize_quoting(
    quoting: &ResolvedQuoting,
    adapter_type: &str,
    database: &str,
    schema: &str,
    identifier: &str,
) -> (String, String, String, ResolvedQuoting) {
    let dialect = Dialect::from_str(adapter_type).unwrap_or_default();
    let (database, database_quoting) = _normalize_quote(quoting.database, &dialect, database);
    let (schema, schema_quoting) = _normalize_quote(quoting.schema, &dialect, schema);
    let (identifier, identifier_quoting) =
        _normalize_quote(quoting.identifier, &dialect, identifier);
    (
        database,
        schema,
        identifier,
        ResolvedQuoting {
            database: database_quoting,
            schema: schema_quoting,
            identifier: identifier_quoting,
        },
    )
}

pub fn normalize_quote(quoting: bool, adapter_type: &str, name: &str) -> (String, bool) {
    let dialect: Dialect = Dialect::from_str(adapter_type).unwrap_or_default();
    _normalize_quote(quoting, &dialect, name)
}

pub fn _normalize_quote(quoting: bool, dialect: &Dialect, name: &str) -> (String, bool) {
    let quoted = name.len() > 1
        && name.starts_with(dialect.quote_char())
        && name.ends_with(dialect.quote_char());

    // If the name is quoted, but the quote config is false, we need to unquote the name
    if (quoted && !quoting) && !name.is_empty() {
        (name[1..name.len() - 1].to_string(), true)
    } else {
        (name.to_string(), quoting)
    }
}

/// Merge two meta maps, with the second map's values taking precedence on key conflicts.
pub fn merge_meta(
    base_meta: Option<BTreeMap<String, YmlValue>>,
    update_meta: Option<BTreeMap<String, YmlValue>>,
) -> Option<BTreeMap<String, YmlValue>> {
    match (base_meta, update_meta) {
        (Some(base_map), Some(update_map)) => {
            let mut merged = base_map;
            merged.extend(update_map);
            Some(merged)
        }
        (None, Some(update_map)) => Some(update_map),
        (Some(base_map), None) => Some(base_map),
        (None, None) => None,
    }
}

/// Merge two tag lists, deduplicating and sorting the result.
pub fn merge_tags(
    base_tags: Option<Vec<String>>,
    update_tags: Option<Vec<String>>,
) -> Option<Vec<String>> {
    match (base_tags, update_tags) {
        // If both are None, result is None
        (None, None) => None,
        // If either has a value (even empty), we preserve that semantic meaning
        (Some(mut base), Some(update)) => {
            base.extend(update);
            base.sort();
            base.dedup();
            Some(base)
        }
        // If only one side has a value, use it
        (Some(base), None) => Some(base),
        (None, Some(update)) => Some(update),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_semantic_name_snowflake_simple() {
        helper("snowflake", false, "xyz", false, "xyz");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoted_identifier() {
        helper("snowflake", false, r#""xyz""#, true, "xyz");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoting() {
        helper("snowflake", true, "xyz", true, "xyz");
    }
    #[test]
    fn test_get_semantic_name_snowflake_quoted_identifier_with_quoting() {
        helper("snowflake", true, r#""xyz""#, true, r#""xyz""#);
    }

    #[test]
    fn test_get_semantic_name_snowflake_non_identifier() {
        // This will fail because `z.3` should be quoted, but this is a user error
        helper("snowflake", false, "z.3", false, "z.3");
    }

    #[test]
    fn test_get_semantic_name_snowflake_reserved() {
        // This will fail because `group` is a reserved keyword, but this is a user error
        helper("snowflake", false, "group", false, "group");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoted_reserved() {
        helper("snowflake", false, r#""GROUP""#, true, "GROUP");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoted_reserved_with_quoting() {
        helper("snowflake", true, r#""GROUP""#, true, r#""GROUP""#);
    }

    #[test]
    fn test_get_semantic_name_postgres_simple() {
        helper("postgres", false, "xyz", false, "xyz");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoted_identifier() {
        helper("postgres", false, r#""xyz""#, true, "xyz");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoting() {
        helper("postgres", true, "xyz", true, "xyz");
    }
    #[test]
    fn test_get_semantic_name_postgres_quoted_identifier_with_quoting() {
        helper("postgres", true, r#""xyz""#, true, r#""xyz""#);
    }

    #[test]
    fn test_get_semantic_name_postgres_non_identifier() {
        // This will fail because `z.3` should be quoted, but this is a user error
        helper("postgres", false, "z.3", false, "z.3");
    }

    #[test]
    fn test_get_semantic_name_postgres_reserved() {
        // This will fail because `group` is a reserved keyword, but this is a user error
        helper("postgres", false, "group", false, "group");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoted_reserved() {
        helper("postgres", false, r#""GROUP""#, true, "GROUP");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoted_reserved_with_quoting() {
        helper("postgres", true, r#""GROUP""#, true, r#""GROUP""#);
    }
    fn helper(
        adapter_type: &str,
        quoting: bool,
        identifier: &str,
        expected_quoting: bool,
        expected_identifier: &str,
    ) {
        let result = normalize_quote(quoting, adapter_type, identifier);

        let (actual_identifier, actual_quoting) = result;
        assert_eq!(actual_identifier, expected_identifier);
        assert_eq!(actual_quoting, expected_quoting);
    }
}
