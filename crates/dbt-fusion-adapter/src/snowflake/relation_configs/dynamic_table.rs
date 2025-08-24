//! reference: dbt/adapters/snowflake/relation_configs/dynamic_table.py

use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::result::Result;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray};
use dbt_agate::AgateTable;
use dbt_schemas::schemas::{DbtModel, RelationChangeSet};
use minijinja::{Value, value::Object};

use crate::record_batch_utils::get_column_values;

/// Deserialization target for macro snowflake__describe_dynamic_table
/// https://github.com/dbt-labs/dbt-adapters/blob/61221f455f5960daf80024febfae6d6fb4b46251/dbt-snowflake/src/dbt/include/snowflake/macros/relations/dynamic_table/describe.sql#L3
#[derive(Debug)]
pub struct DescribeDynamicTableResults {
    pub dynamic_table: Arc<RecordBatch>,
    pub catalog: Option<Arc<RecordBatch>>,
}

impl TryFrom<&Value> for DescribeDynamicTableResults {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let dynamic_table = value
            .get_item(&Value::from_safe_string("dynamic_table".into()))
            .map_err(|e| format!("Expected key `dynamic_table`: {e}"))?
            .downcast_object::<AgateTable>()
            .ok_or("Failed to convert dynamic_table to AgateTable".to_string())?
            .to_record_batch();

        let catalog_value = value
            .get_item(&Value::from_safe_string("catalog".into()))
            .map_err(|e| format!("Expected key `dynamic_table`: {e}"))?;
        let catalog = if catalog_value == Value::UNDEFINED {
            None
        } else {
            Some(
                catalog_value
                    .downcast_object::<AgateTable>()
                    .ok_or("Failed to convert catalog to AgateTable".to_string())?
                    .to_record_batch(),
            )
        };

        Ok(Self {
            dynamic_table,
            catalog,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetLagInterval {
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl Display for TargetLagInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Seconds => write!(f, "seconds"),
            Self::Minutes => write!(f, "minutes"),
            Self::Hours => write!(f, "hours"),
            Self::Days => write!(f, "days"),
        }
    }
}

impl TryFrom<&str> for TargetLagInterval {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let unit = if value.eq_ignore_ascii_case("second") || value.eq_ignore_ascii_case("seconds")
        {
            Self::Seconds
        } else if value.eq_ignore_ascii_case("minute") | value.eq_ignore_ascii_case("minutes") {
            Self::Minutes
        } else if value.eq_ignore_ascii_case("hour") | value.eq_ignore_ascii_case("hours") {
            Self::Hours
        } else if value.eq_ignore_ascii_case("day") | value.eq_ignore_ascii_case("days") {
            Self::Days
        } else {
            return Err(format!("Unsupported interval type: {value}"));
        };
        Ok(unit)
    }
}

/// Snowflake dynamic table target lag
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetLag {
    Downstream,
    TimeBased(u32, TargetLagInterval),
}

impl Display for TargetLag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Downstream => write!(f, "downstream"),
            Self::TimeBased(num, interval) => write!(f, "{num} {interval}"),
        }
    }
}

impl TryFrom<&str> for TargetLag {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.eq_ignore_ascii_case("downstream") {
            return Ok(Self::Downstream);
        }
        // Parse for strings in the form "[0-9]+ {seconds | minutes | hours | days}"
        let mut parts = value.split_ascii_whitespace();

        let opt = parts
            .next()
            .and_then(|num_str| num_str.parse::<u32>().ok())
            .and_then(|num| {
                parts
                    .next()
                    .and_then(|interval_str| TargetLagInterval::try_from(interval_str).ok())
                    .map(|interval| Self::TimeBased(num, interval))
            });
        if let Some(target_lag) = opt {
            if parts.next().is_none() {
                return Ok(target_lag);
            }
        }
        Err(format!("Unsupported target lag: {value}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetLagConfig {
    /// The target lag to use on the dynamic table.
    pub target_lag: TargetLag,
}

impl Object for TargetLagConfig {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("target_lag") => Some(Value::from(self.target_lag.to_string())),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        write!(f, "{}", self.target_lag)
    }
}

impl Display for TargetLagConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.target_lag.fmt(f)
    }
}

impl TryFrom<&str> for TargetLagConfig {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            target_lag: TargetLag::try_from(value)?,
        })
    }
}

/// Snowflake dynamic table refresh modes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshMode {
    Auto,
    Full,
    Incremental,
}

impl Display for RefreshMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "AUTO"),
            Self::Full => write!(f, "FULL"),
            Self::Incremental => write!(f, "INCREMENTAL"),
        }
    }
}

impl Default for RefreshMode {
    fn default() -> Self {
        Self::Auto
    }
}

impl TryFrom<&str> for RefreshMode {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mode = if value.eq_ignore_ascii_case("auto") {
            Self::Auto
        } else if value.eq_ignore_ascii_case("full") {
            Self::Full
        } else if value.eq_ignore_ascii_case("incremental") {
            Self::Incremental
        } else {
            return Err(format!("Unsupported refresh mode: {value}"));
        };
        Ok(mode)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RefreshModeConfig {
    /// The refresh mode to use on the dynamic table.
    pub refresh_mode: RefreshMode,
}

impl Object for RefreshModeConfig {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("refresh_mode") => Some(Value::from(self.refresh_mode.to_string())),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        write!(f, "{}", self.refresh_mode)
    }
}

impl Display for RefreshModeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.refresh_mode.fmt(f)
    }
}

impl TryFrom<&str> for RefreshModeConfig {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            refresh_mode: RefreshMode::try_from(value)?,
        })
    }
}

/// Snowflake initial data population behavior
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Initialize {
    OnCreate,
    OnSchedule,
}

impl Default for Initialize {
    fn default() -> Self {
        Self::OnCreate
    }
}

impl Display for Initialize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OnCreate => write!(f, "ON_CREATE"),
            Self::OnSchedule => write!(f, "ON_SCHEDULE"),
        }
    }
}

impl TryFrom<&str> for Initialize {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mode = if value.eq_ignore_ascii_case("on_create") {
            Self::OnCreate
        } else if value.eq_ignore_ascii_case("on_schedule") {
            Self::OnSchedule
        } else {
            return Err(format!("Unsupported initialize type: {value}"));
        };
        Ok(mode)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InitializeConfig {
    /// The initial data behavior to use on the dynamic table.
    pub initialize: Initialize,
}

impl Object for InitializeConfig {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("initialize") => Some(Value::from(self.initialize.to_string())),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        write!(f, "{}", self.initialize)
    }
}

impl TryFrom<&str> for InitializeConfig {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            initialize: Initialize::try_from(value)?,
        })
    }
}

/// This config follows the specs found here:
/// https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table
/// Reference:
/// https://github.com/dbt-labs/dbt-adapters/blob/816d190c9e31391a48cee979bd049aeb34c89ad3/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L36
// XXX: Deviation from core: raw SQL is not used here - that is already available within the macro as {{ sql }}
#[derive(Debug, Clone)]
pub struct SnowflakeDynamicTableConfig {
    /// Name of the dynamic table.
    pub table_name: String,
    /// Name of the schema containing the dynamic table.
    pub schema_name: String,
    /// Name of the database containing the dynamic table.
    pub database_name: String,
    /// The maximum amount of time that the dynamic table's content should lag behind updates to the source tables.
    /// This configuration option is required.
    pub target_lag: TargetLagConfig,
    /// The name of the warehouse that provides the compute resources for refreshing the dynamic table.
    pub snowflake_warehouse: String,
    /// Specifies the refresh mode for the dynamic table.
    pub refresh_mode: RefreshModeConfig,
    /// Specifies the behavior of the initial refresh of the dynamic table.
    pub initialize: InitializeConfig,
    /// Specifies the row access policy to set on a dynamic table.
    pub row_access_policy: Option<String>,
    /// Specifies the tag name and the tag string value.
    pub table_tag: Option<String>,
}

impl TryFrom<&DbtModel> for SnowflakeDynamicTableConfig {
    type Error = String;

    /// Default behavior follows the table here: https://docs.getdbt.com/reference/resource-configs/snowflake-configs.
    /// Note that `target_lag` and `snowflake_warehouse` are required.
    ///
    /// Reference: https://github.com/dbt-labs/dbt-adapters/blob/816d190c9e31391a48cee979bd049aeb34c89ad3/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L87
    fn try_from(model: &DbtModel) -> Result<Self, Self::Error> {
        let database_name = model.__base_attr__.database.clone();
        let schema_name = model.__base_attr__.schema.clone();
        let table_name = model.__common_attr__.name.clone();

        let snowflake_config = &model
            .__adapter_attr__
            .snowflake_attr
            .as_ref()
            .ok_or("Snowflake attributes not found.".to_string())?;

        // The following two fields are required.
        let target_lag = TargetLagConfig::try_from(
            snowflake_config
                .target_lag
                .clone()
                .ok_or("Failed to get required field target_lag from dynamic_table config.")?
                .as_str(),
        )?;
        let snowflake_warehouse = snowflake_config
            .snowflake_warehouse
            .clone()
            .ok_or("Failed to get required field snowflake_warehouse from dynamic_table config.")?;

        // The remaining fields are not required.
        let refresh_mode = snowflake_config
            .refresh_mode
            .clone()
            .map_or(RefreshModeConfig::default(), |value: String| {
                RefreshModeConfig::try_from(value.as_str()).unwrap_or_default()
            });
        let initialize = snowflake_config
            .initialize
            .clone()
            .map_or(InitializeConfig::default(), |value: String| {
                InitializeConfig::try_from(value.as_str()).unwrap_or_default()
            });
        let row_access_policy = snowflake_config.row_access_policy.clone();
        let table_tag = snowflake_config.table_tag.clone();

        Ok(Self {
            table_name,
            schema_name,
            database_name,
            target_lag,
            snowflake_warehouse,
            refresh_mode,
            initialize,
            row_access_policy,
            table_tag,
        })
    }
}

// Helper function to get a string value from an Record Batch by column name
fn get_string_by_name_from_record_batch(
    batch: &Arc<RecordBatch>,
    col_name: &str,
) -> Result<String, String> {
    if let Ok(column_values) = get_column_values::<StringArray>(batch, col_name) {
        if column_values.len() != 1 {
            return Err(format!(
                "Describe dynamic_table returned an unexpected number of values for {col_name}."
            ));
        }

        Ok(column_values.value(0).to_string())
    } else {
        Err(format!("Describe dynamic_table is missing {col_name}."))
    }
}

// Reference: https://github.com/dbt-labs/dbt-adapters/blob/61221f455f5960daf80024febfae6d6fb4b46251/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L112
impl TryFrom<DescribeDynamicTableResults> for SnowflakeDynamicTableConfig {
    type Error = String;

    fn try_from(value: DescribeDynamicTableResults) -> Result<Self, Self::Error> {
        let batch = value.dynamic_table;
        if batch.num_rows() == 0 {
            return Err("dynamic_table describe table is empty".to_string());
        }

        let _query = get_string_by_name_from_record_batch(&batch, "text")?;

        let table_name = get_string_by_name_from_record_batch(&batch, "name")?;

        let schema_name = get_string_by_name_from_record_batch(&batch, "schema_name")?;

        let database_name = get_string_by_name_from_record_batch(&batch, "database_name")?;

        let snowflake_warehouse = get_string_by_name_from_record_batch(&batch, "warehouse")?;

        let target_lag = TargetLagConfig::try_from(
            get_string_by_name_from_record_batch(&batch, "target_lag")?.as_str(),
        )?;

        let refresh_mode = RefreshModeConfig::try_from(
            get_string_by_name_from_record_batch(&batch, "refresh_mode")?.as_str(),
        )?;

        Ok(Self {
            table_name,
            schema_name,
            database_name,
            target_lag,
            snowflake_warehouse,
            refresh_mode,
            // We don't get initialize since that's a one-time scheduler attribute, not a DT attribute
            initialize: InitializeConfig::default(),
            // These can't be queried from Snowflake
            row_access_policy: None,
            table_tag: None,
        })
    }
}

impl Object for SnowflakeDynamicTableConfig {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("table_name") => Some(Value::from(self.table_name.clone())),
            Some("schema_name") => Some(Value::from(self.schema_name.clone())),
            Some("database_name") => Some(Value::from(self.database_name.clone())),
            Some("target_lag") => Some(Value::from_object(self.target_lag.clone())),
            Some("snowflake_warehouse") => Some(Value::from(self.snowflake_warehouse.clone())),
            Some("refresh_mode") => Some(Value::from_object(self.refresh_mode.clone())),
            Some("initialize") => Some(Value::from_object(self.initialize.clone())),
            Some("row_access_policy") => Some(Value::from(self.row_access_policy.clone())),
            Some("table_tag") => Some(Value::from(self.table_tag.clone())),
            _ => None,
        }
    }
}
// Reference: https://github.com/dbt-labs/dbt-adapters/blob/61221f455f5960daf80024febfae6d6fb4b46251/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L132
#[derive(Debug, Clone)]
pub struct SnowflakeDynamicTableTargetLagConfigChange {
    // Deviation from core: we must include a context to change to, since the presence of this object enforces a config change for this field.
    context: TargetLagConfig,
}

impl RelationChangeSet for SnowflakeDynamicTableTargetLagConfigChange {
    fn requires_full_refresh(&self) -> bool {
        false
    }

    // TODO(anna)
    fn changes(
        &self,
    ) -> &std::collections::BTreeMap<String, Arc<dyn dbt_schemas::schemas::ComponentConfig>> {
        todo!()
    }

    fn get_change(
        &self,
        _component_name: &str,
    ) -> Option<&dyn dbt_schemas::schemas::ComponentConfig> {
        todo!()
    }
}

impl Object for SnowflakeDynamicTableTargetLagConfigChange {
    fn enumerate(self: &Arc<Self>) -> minijinja::value::Enumerator {
        minijinja::value::Enumerator::Str(&["context"])
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("context") => Some(Value::from_object(self.context.clone())),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        write!(f, "{}", self.context)
    }
}

impl Display for SnowflakeDynamicTableTargetLagConfigChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.context.fmt(f)
    }
}

// Reference: https://github.com/dbt-labs/dbt-adapters/blob/61221f455f5960daf80024febfae6d6fb4b46251/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L141
#[derive(Debug, Clone)]
pub struct SnowflakeDynamicTableWarehouseConfigChange {
    // Deviation from core: we must include a context to change to, since the presence of this object enforces a config change for this field.
    context: String,
}

impl RelationChangeSet for SnowflakeDynamicTableWarehouseConfigChange {
    fn requires_full_refresh(&self) -> bool {
        false
    }

    // TODO(anna): come back to this!
    fn changes(
        &self,
    ) -> &std::collections::BTreeMap<String, Arc<dyn dbt_schemas::schemas::ComponentConfig>> {
        todo!()
    }

    fn get_change(
        &self,
        _component_name: &str,
    ) -> Option<&dyn dbt_schemas::schemas::ComponentConfig> {
        todo!()
    }
}

impl Object for SnowflakeDynamicTableWarehouseConfigChange {
    fn enumerate(self: &Arc<Self>) -> minijinja::value::Enumerator {
        minijinja::value::Enumerator::Str(&["context"])
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("context") => Some(Value::from(self.context.clone())),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        write!(f, "{}", self.context)
    }
}

impl Display for SnowflakeDynamicTableWarehouseConfigChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.context.fmt(f)
    }
}

// Reference: https://github.com/dbt-labs/dbt-adapters/blob/61221f455f5960daf80024febfae6d6fb4b46251/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L150
#[derive(Debug, Clone)]
pub struct SnowflakeDynamicTableRefreshModeConfigChange {
    // Deviation from core: we must include a context to change to, since the presence of this object enforces a config change for this field.
    context: RefreshModeConfig,
}

impl RelationChangeSet for SnowflakeDynamicTableRefreshModeConfigChange {
    fn requires_full_refresh(&self) -> bool {
        true
    }

    // TODO(anna)
    fn changes(
        &self,
    ) -> &std::collections::BTreeMap<String, Arc<dyn dbt_schemas::schemas::ComponentConfig>> {
        todo!()
    }

    fn get_change(
        &self,
        _component_name: &str,
    ) -> Option<&dyn dbt_schemas::schemas::ComponentConfig> {
        todo!()
    }
}

impl Display for SnowflakeDynamicTableRefreshModeConfigChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.context.fmt(f)
    }
}

impl Object for SnowflakeDynamicTableRefreshModeConfigChange {
    fn enumerate(self: &Arc<Self>) -> minijinja::value::Enumerator {
        minijinja::value::Enumerator::Str(&["context"])
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("context") => Some(Value::from_object(self.context.clone())),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        write!(f, "{}", self.context)
    }
}

// Reference: https://github.com/dbt-labs/dbt-adapters/blob/acd3177d8a734e508c902a1e933e4dc52b272220/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L159
#[derive(Debug)]
pub struct SnowflakeDynamicTableConfigChangeset {
    target_lag: Option<SnowflakeDynamicTableTargetLagConfigChange>,
    snowflake_warehouse: Option<SnowflakeDynamicTableWarehouseConfigChange>,
    refresh_mode: Option<SnowflakeDynamicTableRefreshModeConfigChange>,
}

impl SnowflakeDynamicTableConfigChangeset {
    pub fn new(old: SnowflakeDynamicTableConfig, new: SnowflakeDynamicTableConfig) -> Self {
        let target_lag = if old.target_lag != new.target_lag {
            Some(SnowflakeDynamicTableTargetLagConfigChange {
                context: new.target_lag,
            })
        } else {
            None
        };

        let snowflake_warehouse = if !old
            .snowflake_warehouse
            .eq_ignore_ascii_case(&new.snowflake_warehouse)
        {
            Some(SnowflakeDynamicTableWarehouseConfigChange {
                context: new.snowflake_warehouse,
            })
        } else {
            None
        };

        let refresh_mode = if new.refresh_mode.refresh_mode != RefreshMode::Auto
            && old.refresh_mode != new.refresh_mode
        {
            Some(SnowflakeDynamicTableRefreshModeConfigChange {
                context: new.refresh_mode,
            })
        } else {
            None
        };

        SnowflakeDynamicTableConfigChangeset {
            target_lag,
            snowflake_warehouse,
            refresh_mode,
        }
    }
}

impl RelationChangeSet for SnowflakeDynamicTableConfigChangeset {
    // TODO(anna): These can't be implemented right now because we're not implementing BaseRelationConfig
    fn changes(
        &self,
    ) -> &std::collections::BTreeMap<String, Arc<dyn dbt_schemas::schemas::ComponentConfig>> {
        todo!()
    }

    fn requires_full_refresh(&self) -> bool {
        let target_lag_requires_refresh = self
            .target_lag
            .as_ref()
            .is_some_and(|target_lag| target_lag.requires_full_refresh());

        let warehouse_requires_refresh = self
            .snowflake_warehouse
            .as_ref()
            .is_some_and(|warehouse| warehouse.requires_full_refresh());

        let refresh_mode_requires_refresh = self
            .refresh_mode
            .as_ref()
            .is_some_and(|refresh_mode| refresh_mode.requires_full_refresh());

        target_lag_requires_refresh || warehouse_requires_refresh || refresh_mode_requires_refresh
    }

    // TODO(anna): These can't be implemented right now because we're not implementing BaseRelationConfig
    fn get_change(
        &self,
        _component_name: &str,
    ) -> Option<&dyn dbt_schemas::schemas::ComponentConfig> {
        todo!()
    }

    fn has_changes(&self) -> bool {
        self.target_lag.is_some()
            || self.snowflake_warehouse.is_some()
            || self.refresh_mode.is_some()
    }
}

impl Object for SnowflakeDynamicTableConfigChangeset {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("target_lag") => {
                // Return None if no change, otherwise return the config as an object
                self.target_lag
                    .as_ref()
                    .map(|target_lag_config| Value::from_object(target_lag_config.clone()))
            }
            Some("snowflake_warehouse") => {
                // Return None if no change, otherwise return the config as an object
                self.snowflake_warehouse
                    .as_ref()
                    .map(|warehouse_config| Value::from_object(warehouse_config.clone()))
            }
            Some("refresh_mode") => {
                // Return None if no change, otherwise return the config as an object
                self.refresh_mode
                    .as_ref()
                    .map(|refresh_mode_config| Value::from_object(refresh_mode_config.clone()))
            }
            Some("requires_full_refresh") => Some(Value::from(self.requires_full_refresh())),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test changeset
    #[test]
    fn test_compute_changeset() {
        let config_1 = SnowflakeDynamicTableConfig {
            table_name: "table".into(),
            schema_name: "schema".into(),
            database_name: "database".into(),
            target_lag: TargetLagConfig {
                target_lag: TargetLag::Downstream,
            },
            snowflake_warehouse: "warehouse".into(),
            refresh_mode: RefreshModeConfig {
                refresh_mode: RefreshMode::Auto,
            },
            initialize: InitializeConfig {
                initialize: Initialize::OnCreate,
            },
            row_access_policy: None,
            table_tag: None,
        };

        let config_2 = SnowflakeDynamicTableConfig {
            table_name: "table".into(),
            schema_name: "schema".into(),
            database_name: "database".into(),
            target_lag: TargetLagConfig {
                target_lag: TargetLag::Downstream,
            },
            snowflake_warehouse: "warehouse".into(),
            refresh_mode: RefreshModeConfig {
                refresh_mode: RefreshMode::Full,
            },
            initialize: InitializeConfig {
                initialize: Initialize::OnCreate,
            },
            row_access_policy: None,
            table_tag: None,
        };

        let config_3 = SnowflakeDynamicTableConfig {
            table_name: "table".into(),
            schema_name: "schema".into(),
            database_name: "database".into(),
            target_lag: TargetLagConfig {
                target_lag: TargetLag::Downstream,
            },
            snowflake_warehouse: "other_warehouse".into(),
            refresh_mode: RefreshModeConfig {
                refresh_mode: RefreshMode::Auto,
            },
            initialize: InitializeConfig {
                initialize: Initialize::OnCreate,
            },
            row_access_policy: None,
            table_tag: None,
        };

        let config_4 = SnowflakeDynamicTableConfig {
            table_name: "table".into(),
            schema_name: "schema".into(),
            database_name: "database".into(),
            target_lag: TargetLagConfig {
                target_lag: TargetLag::TimeBased(1, TargetLagInterval::Hours),
            },
            snowflake_warehouse: "other_warehouse".into(),
            refresh_mode: RefreshModeConfig {
                refresh_mode: RefreshMode::Incremental,
            },
            initialize: InitializeConfig {
                initialize: Initialize::OnCreate,
            },
            row_access_policy: None,
            table_tag: None,
        };

        /* Changeset:
         * target_lag: none
         * snowflake_warehouse: none
         * refresh_mode: auto -> full
         *
         * Since refresh mode changed, we require a full refresh.
         */
        let changeset =
            SnowflakeDynamicTableConfigChangeset::new(config_1.clone(), config_2.clone());

        assert!(changeset.has_changes());
        assert!(changeset.requires_full_refresh());
        assert!(changeset.target_lag.is_none());
        assert!(changeset.snowflake_warehouse.is_none());
        assert!(changeset.refresh_mode.is_some());
        assert_eq!(
            changeset.refresh_mode.unwrap().context,
            RefreshModeConfig {
                refresh_mode: RefreshMode::Full
            }
        );

        /* Changeset:
         * target_lag: none
         * snowflake_warehouse: warehouse -> other_warehouse
         * refresh_mode: none
         *
         * Refresh mode does *not* change since we want to change it to auto.
         * Warehouse has changed, but that doesn't mean we require a full refresh.
         */
        let changeset = SnowflakeDynamicTableConfigChangeset::new(config_2, config_3);

        assert!(changeset.has_changes());
        assert!(!changeset.requires_full_refresh());
        assert!(changeset.target_lag.is_none());
        assert!(changeset.snowflake_warehouse.is_some());
        assert!(changeset.refresh_mode.is_none());
        assert_eq!(
            changeset.snowflake_warehouse.unwrap().context,
            "other_warehouse".to_string()
        );

        /* Changeset:
         * target_lag: downstream -> 1 hour
         * snowflake_warehouse: warehouse -> other_warehouse
         * refresh_mode: full -> incremental
         *
         * Everything changes. Since refresh_mode changed, we require a full refresh.
         */
        let changeset = SnowflakeDynamicTableConfigChangeset::new(config_1, config_4);

        assert!(changeset.has_changes());
        assert!(changeset.requires_full_refresh());
        assert!(changeset.target_lag.is_some());
        assert!(changeset.snowflake_warehouse.is_some());
        assert!(changeset.refresh_mode.is_some());
        assert_eq!(
            changeset.target_lag.unwrap().context,
            TargetLagConfig {
                target_lag: TargetLag::TimeBased(1, TargetLagInterval::Hours)
            }
        );
        assert_eq!(
            changeset.snowflake_warehouse.unwrap().context,
            "other_warehouse".to_string()
        );
        assert_eq!(
            changeset.refresh_mode.unwrap().context,
            RefreshModeConfig {
                refresh_mode: RefreshMode::Incremental
            }
        );
    }

    // Minijinja Object trait tests
    #[test]
    fn test_target_lag_config_object() {
        // Try several possible values for target lags.
        let target_lags = &[
            TargetLag::TimeBased(1, TargetLagInterval::Seconds),
            TargetLag::TimeBased(50, TargetLagInterval::Seconds),
            TargetLag::TimeBased(20, TargetLagInterval::Minutes),
            TargetLag::TimeBased(3, TargetLagInterval::Hours),
            TargetLag::TimeBased(7, TargetLagInterval::Days),
        ];

        for target_lag in target_lags {
            let config = TargetLagConfig {
                target_lag: target_lag.clone(),
            };
            let config_value = Value::from_object(config.clone());

            assert!(config_value.get_item(&Value::from("target_lag")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("target_lag")).unwrap(),
                Value::from(target_lag.clone().to_string())
            );
            assert_eq!(
                config_value
                    .get_item(&Value::from("target_lag"))
                    .unwrap()
                    .to_string(),
                target_lag.to_string()
            );
            assert_eq!(config_value.to_string(), target_lag.to_string());
        }
    }

    #[test]
    fn test_refresh_mode_config_object() {
        // Try all possible values for refresh modes.
        let refresh_modes = &[
            RefreshMode::Auto,
            RefreshMode::Full,
            RefreshMode::Incremental,
        ];

        for refresh_mode in refresh_modes {
            let config = RefreshModeConfig {
                refresh_mode: refresh_mode.clone(),
            };
            let config_value = Value::from_object(config.clone());

            assert!(config_value.get_item(&Value::from("refresh_mode")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("refresh_mode")).unwrap(),
                Value::from(refresh_mode.clone().to_string())
            );
            assert_eq!(
                config_value
                    .get_item(&Value::from("refresh_mode"))
                    .unwrap()
                    .to_string(),
                refresh_mode.to_string()
            );
            assert_eq!(config_value.to_string(), refresh_mode.to_string());
        }
    }

    #[test]
    fn test_initialize_config_object() {
        // Try all possible values for initialize behavior.
        let initialize_options = &[Initialize::OnCreate, Initialize::OnSchedule];

        for initialize in initialize_options {
            let config = InitializeConfig {
                initialize: initialize.clone(),
            };
            let config_value = Value::from_object(config.clone());

            assert!(config_value.get_item(&Value::from("initialize")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("initialize")).unwrap(),
                Value::from(initialize.clone().to_string())
            );
            assert_eq!(
                config_value
                    .get_item(&Value::from("initialize"))
                    .unwrap()
                    .to_string(),
                initialize.to_string()
            );
            assert_eq!(config_value.to_string(), initialize.to_string())
        }
    }

    #[test]
    fn test_snowflake_dynamic_table_config_object() {
        let configs = &[
            (
                TargetLag::Downstream,
                RefreshMode::Auto,
                Initialize::OnCreate,
            ),
            (
                TargetLag::TimeBased(1, TargetLagInterval::Seconds),
                RefreshMode::Auto,
                Initialize::OnCreate,
            ),
            (
                TargetLag::TimeBased(15112, TargetLagInterval::Minutes),
                RefreshMode::Full,
                Initialize::OnSchedule,
            ),
            (
                TargetLag::TimeBased(14, TargetLagInterval::Days),
                RefreshMode::Incremental,
                Initialize::OnSchedule,
            ),
        ];

        for (target_lag, refresh_mode, initialize) in configs {
            let target_lag_config = TargetLagConfig {
                target_lag: target_lag.clone(),
            };
            let refresh_mode_config = RefreshModeConfig {
                refresh_mode: refresh_mode.clone(),
            };
            let initialize_config = InitializeConfig {
                initialize: initialize.clone(),
            };

            let config = SnowflakeDynamicTableConfig {
                table_name: "table".to_string(),
                schema_name: "schema".to_string(),
                database_name: "database".to_string(),
                target_lag: target_lag_config.clone(),
                snowflake_warehouse: "warehouse".to_string(),
                refresh_mode: refresh_mode_config.clone(),
                initialize: initialize_config.clone(),
                row_access_policy: None,
                table_tag: None,
            };
            let config_value = Value::from_object(config);

            assert!(config_value.get_item(&Value::from("table_name")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("table_name")).unwrap(),
                Value::from("table")
            );
            assert!(config_value.get_item(&Value::from("schema_name")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("schema_name")).unwrap(),
                Value::from("schema")
            );
            assert!(config_value.get_item(&Value::from("database_name")).is_ok());
            assert_eq!(
                config_value
                    .get_item(&Value::from("database_name"))
                    .unwrap(),
                Value::from("database")
            );
            assert!(config_value.get_item(&Value::from("target_lag")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("target_lag")).unwrap(),
                Value::from_object(target_lag_config.clone())
            );
            assert!(
                config_value
                    .get_item(&Value::from("snowflake_warehouse"))
                    .is_ok()
            );
            assert_eq!(
                config_value
                    .get_item(&Value::from("snowflake_warehouse"))
                    .unwrap(),
                Value::from("warehouse")
            );
            assert!(config_value.get_item(&Value::from("refresh_mode")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("refresh_mode")).unwrap(),
                Value::from_object(refresh_mode_config.clone())
            );
            assert!(config_value.get_item(&Value::from("initialize")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("initialize")).unwrap(),
                Value::from_object(initialize_config.clone())
            );
            assert!(
                config_value
                    .get_item(&Value::from("row_access_policy"))
                    .is_ok()
            );
            assert_eq!(
                config_value
                    .get_item(&Value::from("row_access_policy"))
                    .unwrap(),
                Value::from(None::<()>)
            );
            assert!(config_value.get_item(&Value::from("table_tag")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("table_tag")).unwrap(),
                Value::from(None::<()>)
            );
        }
    }

    #[test]
    fn test_target_lag_config_change_object() {
        // Try several possible values for target lags.
        let target_lags = &[
            TargetLag::Downstream,
            TargetLag::TimeBased(1, TargetLagInterval::Seconds),
            TargetLag::TimeBased(50, TargetLagInterval::Seconds),
            TargetLag::TimeBased(20, TargetLagInterval::Minutes),
            TargetLag::TimeBased(3, TargetLagInterval::Hours),
            TargetLag::TimeBased(7, TargetLagInterval::Days),
        ];

        for target_lag in target_lags {
            let config = TargetLagConfig {
                target_lag: target_lag.clone(),
            };
            let config_value = Value::from_object(config.clone());

            let config_change = SnowflakeDynamicTableTargetLagConfigChange { context: config };
            let config_change_value = Value::from_object(config_change.clone());

            assert!(
                config_change_value
                    .get_item(&Value::from("context"))
                    .is_ok()
            );
            assert_eq!(
                config_change_value
                    .get_item(&Value::from("context"))
                    .unwrap(),
                config_value.clone()
            );
            assert_eq!(
                config_change_value
                    .get_item(&Value::from("context"))
                    .unwrap()
                    .to_string(),
                target_lag.to_string()
            );
            assert_eq!(config_change_value.to_string(), target_lag.to_string());
        }
    }

    #[test]
    fn test_warehouse_config_change_object() {
        let warehouse = "test_warehouse";

        let config_change = SnowflakeDynamicTableWarehouseConfigChange {
            context: warehouse.to_string(),
        };
        let config_change_value = Value::from_object(config_change);

        assert!(
            config_change_value
                .get_item(&Value::from("context"))
                .is_ok()
        );
        assert_eq!(
            config_change_value
                .get_item(&Value::from("context"))
                .unwrap(),
            Value::from(Some(warehouse.to_string()))
        );
        assert_eq!(
            config_change_value
                .get_item(&Value::from("context"))
                .unwrap()
                .to_string(),
            "test_warehouse".to_string()
        );
        assert_eq!(
            config_change_value.to_string(),
            "test_warehouse".to_string()
        );
    }

    #[test]
    fn test_refresh_mode_config_change_object() {
        // Try all possible values for refresh modes.
        let refresh_modes = &[
            RefreshMode::Auto,
            RefreshMode::Full,
            RefreshMode::Incremental,
        ];

        for refresh_mode in refresh_modes {
            let config = RefreshModeConfig {
                refresh_mode: refresh_mode.clone(),
            };
            let config_value = Value::from_object(config.clone());

            assert!(config_value.get_item(&Value::from("refresh_mode")).is_ok());
            assert_eq!(
                config_value.get_item(&Value::from("refresh_mode")).unwrap(),
                Value::from(refresh_mode.clone().to_string())
            );

            let config_change = SnowflakeDynamicTableRefreshModeConfigChange { context: config };
            let config_change_value = Value::from_object(config_change.clone());

            assert!(
                config_change_value
                    .get_item(&Value::from("context"))
                    .is_ok()
            );
            assert_eq!(
                config_change_value
                    .get_item(&Value::from("context"))
                    .unwrap(),
                config_value.clone()
            );
            assert_eq!(
                config_change_value
                    .get_item(&Value::from("context"))
                    .unwrap()
                    .to_string(),
                refresh_mode.to_string()
            );
            assert_eq!(config_change_value.to_string(), refresh_mode.to_string());
        }
    }

    #[test]
    fn test_snowflake_dynamic_table_config_changeset_object() {
        let target_lag = TargetLag::TimeBased(15210, TargetLagInterval::Hours);
        let target_lag_config = TargetLagConfig {
            target_lag: target_lag.clone(),
        };
        let target_lag_config_change = SnowflakeDynamicTableTargetLagConfigChange {
            context: target_lag_config,
        };

        let warehouse_config_change = SnowflakeDynamicTableWarehouseConfigChange {
            context: "new_warehouse".to_string(),
        };

        let refresh_mode = RefreshMode::Full;
        let refresh_config = RefreshModeConfig {
            refresh_mode: refresh_mode.clone(),
        };
        let refresh_config_change = SnowflakeDynamicTableRefreshModeConfigChange {
            context: refresh_config,
        };

        let config_changeset = SnowflakeDynamicTableConfigChangeset {
            target_lag: Some(target_lag_config_change.clone()),
            snowflake_warehouse: Some(warehouse_config_change.clone()),
            refresh_mode: Some(refresh_config_change.clone()),
        };
        let config_changeset_value = Value::from_object(config_changeset);

        assert!(
            config_changeset_value
                .get_item(&Value::from("target_lag"))
                .is_ok()
        );

        let target_lag_value = config_changeset_value
            .get_item(&Value::from("target_lag"))
            .unwrap();

        assert!(!target_lag_value.is_none());
        assert_eq!(
            target_lag_value,
            Value::from(Some(Value::from_object(target_lag_config_change)))
        );
        assert_eq!(target_lag_value.to_string(), target_lag.to_string());

        assert!(
            config_changeset_value
                .get_item(&Value::from("snowflake_warehouse"))
                .is_ok()
        );

        let warehouse_value = config_changeset_value
            .get_item(&Value::from("snowflake_warehouse"))
            .unwrap();
        assert!(!warehouse_value.is_none());
        assert_eq!(
            warehouse_value,
            Value::from(Some(Value::from_object(warehouse_config_change)))
        );
        assert_eq!(warehouse_value.to_string(), "new_warehouse");

        assert!(
            config_changeset_value
                .get_item(&Value::from("refresh_mode"))
                .is_ok()
        );

        let refresh_value = config_changeset_value
            .get_item(&Value::from("refresh_mode"))
            .unwrap();

        assert!(!refresh_value.is_none());
        assert_eq!(
            refresh_value,
            Value::from(Some(Value::from_object(refresh_config_change)))
        );
        assert_eq!(refresh_value.to_string(), refresh_mode.to_string());
    }
}
