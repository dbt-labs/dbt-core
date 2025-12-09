//! This module is based on the `func getTableSchemaWithFilter` from the following file:
//! https://github.com/dbt-labs/arrow-adbc/blob/6a57518fead4fde556aafdd96d64a259727fa942/go/adbc/driver/bigquery/connection.go#L630

use crate::sql_types::TypeOps;
use arrow_schema::Schema;
use chrono::{DateTime, TimeDelta, Utc};
use dbt_schemas::schemas::{
    DbtModel, InternalDbtNode,
    common::DbtMaterialization,
    manifest::{
        BigqueryPartitionConfig, BigqueryPartitionConfigInner, Range, RangeConfig, TimeConfig,
    },
    nodes::BigQueryAttr,
};
use dbt_xdbc::duration::parse_duration;
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind,
    value::{Object, Value as MinijinjaValue},
};
use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;
use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

// Defaults taken from: https://docs.getdbt.com/reference/resource-configs/bigquery-configs#materialized-views
const DEFAULT_REFRESH_INTERVAL_MINUTES: f64 = 30.0;
const DEFAULT_ENABLE_REFRESH: bool = true;

trait MetadataGetterExt {
    fn get_or_err(&self, key: &str) -> Result<&str, String>;
}

impl MetadataGetterExt for HashMap<String, String> {
    fn get_or_err(&self, key: &str) -> Result<&str, String> {
        self.get(key)
            .map(|s| s.as_str())
            .ok_or_else(|| format!("key not found: {key}"))
    }
}

pub trait BigqueryPartitionConfigExt
where
    Self: Sized,
{
    fn try_from_schema(schema: &Schema, type_ops: &dyn TypeOps) -> Result<Option<Self>, String>;
}

impl BigqueryPartitionConfigExt for BigqueryPartitionConfig {
    fn try_from_schema(
        schema: &Schema,
        type_opts: &dyn TypeOps,
    ) -> Result<Option<BigqueryPartitionConfig>, String> {
        let metadata = &schema.metadata;
        let time_partition = if let Some(partition) = metadata.get("TimePartitioning.Field") {
            let field_name = partition
                .parse::<String>()
                .map_err(|_e| "Could not parse TimePartitioning.Field as string".to_owned())?;

            let (_, field) = schema.fields().find(&field_name).ok_or_else(|| {
                format!("Field '{field_name}' does not exist in arrow schema returned by BigQuery.")
            })?;
            let mut data_type = String::new();
            type_opts
                .format_arrow_type_as_sql(field.data_type(), &mut data_type)
                .map_err(|_e| format!("Could not format arrow type for field '{field_name}'"))?;

            Some(Self {
                field: field_name,
                data_type,
                __inner__: BigqueryPartitionConfigInner::Time(TimeConfig {
                    granularity: metadata
                        .get_or_err("TimePartitioning.Type")
                        .map(|s| s.to_string())?,
                    time_ingestion_partitioning: false,
                }),
                // TODO(serramatutu): how do we determine the value of this?
                copy_partitions: false,
            })
        } else {
            None
        };

        let range_partition = if let Some(partition) = metadata.get("RangePartitioning.Field") {
            let field = partition.parse::<String>().unwrap();

            Some(Self {
                field,
                data_type: "int64".to_string(),
                __inner__: BigqueryPartitionConfigInner::Range(RangeConfig {
                    range: Range {
                        start: metadata
                            .get_or_err("RangePartitioning.Range.Start")?
                            .parse::<u64>()
                            .map_err(|_err| {
                                "Could not parse 'RangePartitioning.Range.Start' as u64".to_string()
                            })?,
                        end: metadata
                            .get_or_err("RangePartitioning.Range.End")?
                            .parse::<u64>()
                            .map_err(|_err| {
                                "Could not parse 'RangePartitioning.Range.End' as u64".to_string()
                            })?,
                        interval: metadata
                            .get_or_err("RangePartitioning.Range.Interval")?
                            .parse::<u64>()
                            .map_err(|_err| {
                                "Could not parse 'RangePartitioning.Range.Interval' as u64"
                                    .to_string()
                            })?,
                    },
                }),
                // TODO(serramatutu): how do we determine the value of this?
                copy_partitions: false,
            })
        } else {
            None
        };

        Ok(time_partition.or(range_partition))
    }
}

/// Access something as a BigQuery MaterializedView
///
/// This exposes data with resolved defaults
pub trait BigqueryMaterializedViewConfig: std::fmt::Debug + Sync + Send {
    fn table_id(&self) -> &str;
    fn dataset_id(&self) -> &str;
    fn project_id(&self) -> &str;

    fn tags(&self) -> &BTreeMap<String, String>;
    fn labels(&self) -> &BTreeMap<String, String>;
    fn kms_key_name(&self) -> &str;
    fn description(&self) -> &str;
    fn partition_by(&self) -> Option<&BigqueryPartitionConfig>;
    fn cluster_by(&self) -> Vec<&str>;

    // https://docs.getdbt.com/reference/resource-configs/bigquery-configs#auto-refresh
    fn enable_refresh(&self) -> bool;
    fn refresh_interval_minutes(&self) -> f64;
    fn max_staleness(&self) -> &str;
    fn expiration_timestamp_ns(&self) -> u64;
    // TODO(serramatutu): possibly add on_configuration_change
}

impl dyn BigqueryMaterializedViewConfig {
    pub fn options_eq<T: Borrow<Self>>(&self, other: &T) -> bool {
        let other = other.borrow();

        self.labels() == other.labels()
            && self.tags() == other.tags()
            && self.kms_key_name() == other.kms_key_name()
            && self.description() == other.description()
            && self.enable_refresh() == other.enable_refresh()
            && self.refresh_interval_minutes() == other.refresh_interval_minutes()
            && self.max_staleness() == other.max_staleness()
            && self.expiration_timestamp_ns() == other.expiration_timestamp_ns()
    }

    pub fn try_from_model(
        model: Arc<DbtModel>,
    ) -> Result<Arc<dyn BigqueryMaterializedViewConfig>, String> {
        if model.__base_attr__.materialized != DbtMaterialization::MaterializedView {
            return Err("Cannot construct BigqueryMaterializedViewConfigFromDbtModel from model which is not a materization.".to_owned());
        }

        if model.base().relation_name.is_none() {
            return Err("DbtModel does not contain relation name".to_owned());
        }

        if model.__adapter_attr__.bigquery_attr.is_none() {
            return Err("DbtModel does not contain Bigquery adapter attributes".to_owned());
        }

        if let Some(partition_by) = &model
            .__adapter_attr__
            .bigquery_attr
            .as_ref()
            .unwrap()
            .partition_by
        {
            partition_by
                .as_bigquery()
                .ok_or_else(|| "PartitionConfig must be BigqueryPartitionConfig".to_string())?;
        }

        Ok(Arc::new(BigqueryMaterializedViewConfigFromDbtModel(model)))
    }

    pub fn try_from_schema(
        schema: &Schema,
        type_ops: &dyn TypeOps,
    ) -> Result<Arc<dyn BigqueryMaterializedViewConfig>, String> {
        let table_type = schema.metadata.get_or_err("Type")?;
        if table_type != "MATERIALIZED_VIEW" {
            return Err(format!(
                "Cannot construct BigqueryPartitionConfig from table_type='{table_type}' (needs to be MATERIALIZED_VIEW)."
            ));
        }

        let full_id = schema.metadata.get_or_err("FullID")?;

        let parts: Vec<_> = full_id.split(":").collect();
        if parts.len() != 2 {
            return Err(
                "FullID must be in the format of project_id:dataset_id.table_id".to_string(),
            );
        }

        let project_id = parts[0].to_string();
        let parts: Vec<_> = parts[1].split(".").collect();
        let dataset_id = parts[0].to_string();
        let table_id = parts[1].to_string();

        Ok(Arc::new(BigqueryMaterializedViewConfigRepr {
            table_id,
            dataset_id,
            project_id,
            enable_refresh: schema
                .metadata
                .get("MaterializedView.EnableRefresh")
                .map(|s| {
                    s.parse::<bool>().map_err(|_err| {
                        "Could not parse 'MaterializedView.EnableRefresh' as boolean".to_string()
                    })
                })
                .unwrap_or(Ok(DEFAULT_ENABLE_REFRESH))?,
            refresh_interval_minutes: schema
                .metadata
                .get("MaterializedView.RefreshInterval")
                .map(|s| {
                    // NOTE: BigQuery driver serializes this with time.Duration.String()
                    // Who thought it was a good idea to use a non-standard format here?.......
                    let duration = parse_duration(s).map_err(|_err| {
                        "Could not parse 'MaterializedView.RefreshInterval' as duration".to_string()
                    })?;
                    let minutes = duration.as_secs() as f64 / 60.0;
                    Ok::<f64, String>(minutes)
                })
                .unwrap_or(Ok(DEFAULT_REFRESH_INTERVAL_MINUTES))?,
            labels: schema
                .metadata
                .get("Labels")
                .map(|labels_json| {
                    if labels_json.is_empty() {
                        Ok(BTreeMap::new())
                    } else {
                        serde_json::from_str(labels_json)
                            .map_err(|_err| "Could not parse 'Labels' as valid JSON".to_string())
                    }
                })
                .unwrap_or_else(|| Ok(BTreeMap::new()))?,
            description: schema
                .metadata
                .get("Description")
                .map(|s| s.to_owned())
                .unwrap_or_else(|| "".to_owned()),
            partition_by: BigqueryPartitionConfig::try_from_schema(schema, type_ops)?,

            // NOTE: dbt set this to None, but the ADBC driver provides it under
            // MaterializedView.MaxStaleness.
            //
            // This is a deviation from Core.
            // https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-bigquery/src/dbt/adapters/bigquery/relation_configs/_options.py#L142
            max_staleness: schema
                .metadata
                .get("MaterializedView.MaxStaleness")
                .map(|s| s.to_owned())
                .unwrap_or_else(|| "".to_owned()),

            expiration_timestamp_ns: schema
                .metadata
                .get("ExpirationTime")
                .map(|s| {
                    DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.timestamp_nanos_opt().map(|v| v as u64).unwrap_or(0))
                        .map_err(|_err| "Could not parse 'ExpirationTime' as DateTime".to_string())
                })
                .unwrap_or(Ok(0))?,
            kms_key_name: schema
                .metadata
                .get("EncryptionConfig.KMSKeyName")
                .map(|s| s.to_owned())
                .unwrap_or_else(|| "".to_owned()),
            cluster_by: cluster_by_from_schema(schema)?,
            tags: schema
                .metadata
                .get("ResourceTags")
                .map(|tags_json| {
                    if tags_json.is_empty() {
                        Ok(BTreeMap::new())
                    } else {
                        serde_json::from_str(tags_json).map_err(|_err| {
                            "Could not parse 'ResourceTags' as valid JSON".to_string()
                        })
                    }
                })
                .unwrap_or_else(|| Ok(BTreeMap::new()))?,
        }))
    }
}

/// A BigqueryMaterializedViewConfig exposed as a Jinja object
#[derive(Debug)]
pub struct BigqueryMaterializedViewConfigObject(Arc<dyn BigqueryMaterializedViewConfig>);

/// The thing that's accessed via `.options` from `BigqueryMaterializedViewConfigObject`
#[derive(Debug)]
pub struct BigqueryMaterializedViewConfigObjectOptions(Arc<BigqueryMaterializedViewConfigObject>);

impl BigqueryMaterializedViewConfigObject {
    pub fn new(repr: Arc<dyn BigqueryMaterializedViewConfig>) -> Self {
        Self(repr)
    }

    pub fn inner(&self) -> &Arc<dyn BigqueryMaterializedViewConfig> {
        &self.0
    }

    // Reference: https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-bigquery/src/dbt/adapters/bigquery/relation_configs/_options.py#L29
    fn as_ddl_dict(self: &Arc<Self>) -> Result<MinijinjaValue, MinijinjaError> {
        let mut hm = BTreeMap::new();

        hm.insert(
            "enable_refresh".to_string(),
            MinijinjaValue::from(self.0.enable_refresh()),
        );
        hm.insert(
            "refresh_interval_minutes".to_string(),
            MinijinjaValue::from(self.0.refresh_interval_minutes()),
        );
        hm.insert(
            "labels".to_string(),
            MinijinjaValue::from_serialize(self.0.labels().iter().collect::<Vec<_>>()),
        );
        hm.insert(
            "tags".to_string(),
            MinijinjaValue::from_serialize(self.0.tags().iter().collect::<Vec<_>>()),
        );

        let expiration_timestamp_ns = self.0.expiration_timestamp_ns();
        if expiration_timestamp_ns > 0 {
            let date = PyDateTime::new_naive(
                DateTime::from_timestamp_nanos(expiration_timestamp_ns as i64).naive_utc(),
            );
            hm.insert(
                "expiration_timestamp".to_string(),
                MinijinjaValue::from_object(date),
            );
        }

        if !self.0.max_staleness().is_empty() {
            hm.insert(
                "max_staleness".to_string(),
                MinijinjaValue::from(self.0.max_staleness()),
            );
        }
        if !self.0.kms_key_name().is_empty() {
            hm.insert(
                "kms_key_name".to_string(),
                MinijinjaValue::from(format!("'{}'", self.0.kms_key_name())),
            );
        }
        if !self.0.description().is_empty() {
            // wtf? (the BigQuery adapter does this, but no idea why)
            let jsonified = serde_json::to_string(self.0.description()).map_err(|_err| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidArgument,
                    format!(
                        "Could not escape materialized view description '{}'",
                        self.0.description()
                    ),
                )
            })?;
            let escaped = format!("\"\"\"{}\"\"\"", &jsonified[1..jsonified.len() - 1]);
            hm.insert("description".to_string(), MinijinjaValue::from(escaped));
        }

        Ok(MinijinjaValue::from_serialize(hm))
    }
}

impl Object for BigqueryMaterializedViewConfigObject {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "options" => Some(MinijinjaValue::from_object(
                BigqueryMaterializedViewConfigObjectOptions(self.clone()),
            )),
            "partition" => self
                .0
                .partition_by()
                .map(|pb| MinijinjaValue::from_object(pb.clone())),
            "cluster" => {
                if self.0.cluster_by().is_empty() {
                    None
                } else {
                    Some(MinijinjaValue::from_serialize(BTreeMap::from([(
                        "fields",
                        self.0
                            .cluster_by()
                            .into_iter()
                            .map(|s| s.to_owned())
                            .collect::<Vec<String>>(),
                    )])))
                }
            }
            _ => None,
        }
    }
}

impl Object for BigqueryMaterializedViewConfigObjectOptions {
    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        method: &str,
        _args: &[MinijinjaValue],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        match method {
            "as_ddl_dict" => self.0.as_ddl_dict(),
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod,
                format!("'{method}' is not a method of BigqueryMaterializedViewConfigObject"),
            )),
        }
    }
}

/// Concrete BigQueryMaterializedViewConfig that holds its fields independently
#[derive(Debug)]
struct BigqueryMaterializedViewConfigRepr {
    table_id: String,
    dataset_id: String,
    project_id: String,

    tags: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    kms_key_name: String,
    enable_refresh: bool,
    expiration_timestamp_ns: u64,
    refresh_interval_minutes: f64,

    max_staleness: String,
    description: String,

    partition_by: Option<BigqueryPartitionConfig>,
    cluster_by: Vec<String>,
}

impl BigqueryMaterializedViewConfig for BigqueryMaterializedViewConfigRepr {
    fn table_id(&self) -> &str {
        self.table_id.as_str()
    }

    fn dataset_id(&self) -> &str {
        self.dataset_id.as_str()
    }

    fn project_id(&self) -> &str {
        self.project_id.as_str()
    }

    fn tags(&self) -> &BTreeMap<String, String> {
        &self.tags
    }

    fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }

    fn kms_key_name(&self) -> &str {
        self.kms_key_name.as_ref()
    }

    fn enable_refresh(&self) -> bool {
        self.enable_refresh
    }

    fn refresh_interval_minutes(&self) -> f64 {
        self.refresh_interval_minutes
    }

    fn max_staleness(&self) -> &str {
        self.max_staleness.as_ref()
    }

    fn description(&self) -> &str {
        self.description.as_ref()
    }

    fn partition_by(&self) -> Option<&BigqueryPartitionConfig> {
        self.partition_by.as_ref()
    }

    fn cluster_by(&self) -> Vec<&str> {
        self.cluster_by.iter().map(|s| s.as_str()).collect()
    }

    fn expiration_timestamp_ns(&self) -> u64 {
        self.expiration_timestamp_ns
    }
}

/// Concrete BigQueryMaterializedViewConfig that wraps a DbtModel
#[derive(Debug)]
struct BigqueryMaterializedViewConfigFromDbtModel(Arc<DbtModel>);

impl BigqueryMaterializedViewConfigFromDbtModel {
    fn attr(&self) -> &BigQueryAttr {
        self.0
            .__adapter_attr__
            .bigquery_attr
            .as_ref()
            .expect("BigqueryMaterializedViewConfigFromDbtModel needs to be BigQuery model")
            .as_ref()
    }
}

static EMPTY_BTREEMAP: BTreeMap<String, String> = BTreeMap::new();

impl BigqueryMaterializedViewConfig for BigqueryMaterializedViewConfigFromDbtModel {
    fn table_id(&self) -> &str {
        self.0
            .base()
            .relation_name
            .as_ref()
            .expect("expected 'relation_name' to exist in model")
            .as_ref()
    }

    fn dataset_id(&self) -> &str {
        self.0.base().schema.as_ref()
    }

    fn project_id(&self) -> &str {
        self.0.base().database.as_ref()
    }

    fn tags(&self) -> &BTreeMap<String, String> {
        self.attr()
            .resource_tags
            .as_ref()
            .unwrap_or(&EMPTY_BTREEMAP)
    }

    fn labels(&self) -> &BTreeMap<String, String> {
        self.attr().labels.as_ref().unwrap_or(&EMPTY_BTREEMAP)
    }

    fn kms_key_name(&self) -> &str {
        self.attr()
            .kms_key_name
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or("")
    }

    fn enable_refresh(&self) -> bool {
        self.attr().enable_refresh.unwrap_or(DEFAULT_ENABLE_REFRESH)
    }

    fn refresh_interval_minutes(&self) -> f64 {
        self.attr()
            .refresh_interval_minutes
            .unwrap_or(DEFAULT_REFRESH_INTERVAL_MINUTES)
    }

    fn max_staleness(&self) -> &str {
        self.attr()
            .max_staleness
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or("")
    }

    fn description(&self) -> &str {
        self.attr()
            .description
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or("")
    }

    fn partition_by(&self) -> Option<&BigqueryPartitionConfig> {
        self.attr()
            .partition_by
            .as_ref()
            .map(|v| v.as_bigquery().expect("violated invariant"))
    }

    fn cluster_by(&self) -> Vec<&str> {
        self.attr()
            .cluster_by
            .as_ref()
            .map(|cb| cb.fields())
            .unwrap_or_default()
    }

    fn expiration_timestamp_ns(&self) -> u64 {
        // Reference: https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-bigquery/src/dbt/adapters/bigquery/relation_configs/_options.py#L129
        match self.attr().hours_to_expiration {
            Some(hours_to_expiration) => {
                (Utc::now() + TimeDelta::hours(hours_to_expiration as i64))
                    .timestamp_nanos_opt()
                    .expect("Your view will expire later than 2262! O.o") as u64
            }
            None => 0,
        }
    }
}

/// Represents a change in a certain configuration by comparing the applied state with the desired state
#[derive(Debug)]
enum ConfigChange<T> {
    /// The config has changed, and the new config should look like `Some.0`
    Some(T),
    /// The config used to exist but has been dropped
    Drop,
    /// There were no detected changes
    None,
}

impl<T> ConfigChange<T> {
    /// Whether the config was changed or not
    #[inline]
    fn is_change(&self) -> bool {
        match self {
            ConfigChange::Some(_) => true,
            ConfigChange::Drop => true,
            ConfigChange::None => false,
        }
    }
}

/// The changeset that needs to be applied to take a materialized view from current existing state
/// to the desired (new) state. This is meant to be used in Jinja.
///
/// Reference: https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-bigquery/src/dbt/adapters/bigquery/relation_configs/_materialized_view.py#L117
#[derive(Debug)]
pub struct BigqueryMaterializedViewConfigChangesetObject {
    options: Option<Arc<dyn BigqueryMaterializedViewConfig>>,
    cluster_by: Option<Vec<String>>,
    // NOTE: in the case of `options` and `cluster_by`, `None` means "there was no config change" and
    // `Some` represents the new desired state. For `partition_by`, since BigQuery uses its special
    // `BigqueryPartitionConfig` struct, there would be no way to distinguish `None` as "there was no
    // config change" or "the config change was dropped", since "no partition config" is represented by
    // `None` all around our codebase. This is why we're using `ConfigChange` instead of `Option` here.
    partition_by: ConfigChange<BigqueryPartitionConfig>,
}

impl BigqueryMaterializedViewConfigChangesetObject {
    pub fn new(
        old: &Arc<dyn BigqueryMaterializedViewConfig>,
        new: Arc<dyn BigqueryMaterializedViewConfig>,
    ) -> Self {
        let partition_by = match (old.partition_by(), new.partition_by()) {
            (Some(old_pb), Some(new_pb)) => {
                if old_pb != new_pb {
                    ConfigChange::Some(new_pb.clone())
                } else {
                    ConfigChange::None
                }
            }
            (None, Some(new_pb)) => ConfigChange::Some(new_pb.clone()),
            (Some(_), None) => ConfigChange::Drop,
            (None, None) => ConfigChange::None,
        };

        let cluster_by = if old.cluster_by() != new.cluster_by() {
            Some(new.cluster_by().into_iter().map(|s| s.to_owned()).collect())
        } else {
            None
        };

        let options = if !old.options_eq(&new) {
            Some(new)
        } else {
            None
        };

        Self {
            options,
            partition_by,
            cluster_by,
        }
    }

    pub fn requires_full_refresh(&self) -> bool {
        self.partition_by.is_change() || self.cluster_by.is_some()
    }

    pub fn has_changes(&self) -> bool {
        self.options.is_some() || self.partition_by.is_change() || self.cluster_by.is_some()
    }
}

impl Object for BigqueryMaterializedViewConfigChangesetObject {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "requires_full_refresh" => Some(MinijinjaValue::from(self.requires_full_refresh())),
            "has_changes" => Some(MinijinjaValue::from(self.has_changes())),
            "options" => self
                .options
                .as_ref()
                .map(|_opts| MinijinjaValue::from_dyn_object(self.clone())),
            "context" => self.options.as_ref().map(|opts| {
                MinijinjaValue::from_object(BigqueryMaterializedViewConfigObjectOptions(Arc::new(
                    BigqueryMaterializedViewConfigObject(opts.clone()),
                )))
            }),
            _ => None,
        }
    }
}

/// Check if the actual and configured partitions for a table are a match.
/// BigQuery tables can be replaced if:
/// - Both tables are not partitioned, OR
/// - Both tables are partitioned using the exact same configs
///
/// If there is a mismatch, then the table cannot be replaced directly.
pub fn partitions_match(
    remote: Option<BigqueryPartitionConfig>,
    local: Option<BigqueryPartitionConfig>,
) -> bool {
    match (&remote, &local) {
        (Some(remote), Some(local)) => remote == local,
        (None, None) => true,
        _ => false,
    }
}

/// Parse a vec of cluster_by fields from the returned schema of the BQ driver.
pub fn cluster_by_from_schema(schema: &Schema) -> Result<Vec<String>, String> {
    schema
        .metadata
        .get("Clustering.Fields")
        .map(|value_json| {
            if value_json.is_empty() {
                Ok(Vec::new())
            } else {
                serde_json::from_str(value_json)
                    .map_err(|_err| "Could not parse 'Clustering.Fields' as valid JSON".to_string())
            }
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_types::NaiveTypeOpsImpl;
    use arrow::datatypes::DataType;
    use arrow_schema::{Field, Fields, TimeUnit};
    use dbt_common::adapter::AdapterType;

    /// Test that the arrow schema returned from the BigQuery driver
    /// can be parsed into a `BigqueryPartitionConfigRepr`
    #[test]
    fn test_materialized_view_from_schema() {
        let tf = Box::new(NaiveTypeOpsImpl::new(AdapterType::Bigquery)) as Box<dyn TypeOps>;
        // Metadata reference: https://github.com/apache/arrow-adbc/blob/f5c0354ac80eaa829c1228e1e0dba7ddc7fd1787/go/adbc/driver/bigquery/connection.go#L703
        let fields = Fields::from(vec![Field::new(
            "my_field",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let metadata = HashMap::from_iter(
            [
                ("Name", "my_view"),
                ("Location", "US"),
                ("Description", "description"),
                (
                    "Labels",
                    "{\"label_1\": \"value_1\", \"label_2\":\"value_2\"}",
                ),
                ("FullID", "my_project:my_dataset.my_view"),
                ("Type", "MATERIALIZED_VIEW"),
                ("CreationTime", "2025-01-01T00:00:00+00:00"),
                ("LastModifiedTime", "2025-01-01T00:00:00+00:00"),
                ("NumBytes", "1234"),
                ("NumLongTermBytes", "12345"),
                ("NumRows", "123"),
                ("ETag", "sometag"),
                ("DefaultCollation", ""),
                ("ResourceTags", "{\"my_tag\":\"my_val\"}"),
                ("EncryptionConfig.KMSKeyName", "my_key"),
                ("ExpirationTime", "2025-01-01T00:00:00+00:00"),
                (
                    "SnapshotDefinition.BaseTableReference",
                    "my_project:my-dataset.my_snapshot_base_table",
                ),
                (
                    "SnapshotDefinition.SnapshotTime",
                    "2025-01-01T00:00:00+00:00",
                ),
                (
                    "CloneDefinition.BaseTableReference",
                    "my_project:my-dataset.my_clone_base_table",
                ),
                ("CloneDefinition.CloneTime", "2025-01-01T00:00:00+00:00"),
                ("MaterializedView.EnableRefresh", "true"),
                (
                    "MaterializedView.LastRefreshTime",
                    "2025-01-01T00:00:00+00:00",
                ),
                ("MaterializedView.Query", "SELECT 1"),
                ("MaterializedView.RefreshInterval", "24h0m0s"),
                ("MaterializedView.AllowNonIncrementalDefinition", "true"),
                ("MaterializedView.MaxStaleness", "2025-01-01T00:00:00+00:00"),
                ("TimePartitioning.Type", "DAY"),
                ("TimePartitioning.Expiration", "24h0m0s"),
                ("TimePartitioning.Field", "my_field"),
                ("Clustering.Fields", "[\"field_1\",\"field_2\"]"),
                // tables can't be both time-partitioned and range-partitioned at the same time,
                // so in this case RangePartitioning is unset
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        let schema = Schema::new(fields).with_metadata(metadata);

        let mv =
            <dyn BigqueryMaterializedViewConfig>::try_from_schema(&schema, tf.as_ref()).unwrap();
        assert_eq!(mv.table_id(), "my_view");
        assert_eq!(mv.dataset_id(), "my_dataset");
        assert_eq!(mv.project_id(), "my_project");

        assert_eq!(mv.description(), "description");
        assert_eq!(mv.labels().len(), 2);
        assert_eq!(mv.labels().get("label_1").unwrap(), "value_1");
        assert_eq!(mv.labels().get("label_2").unwrap(), "value_2");
        assert_eq!(mv.kms_key_name(), "my_key");

        assert!(mv.enable_refresh());
        assert_eq!(mv.refresh_interval_minutes(), 24.0 * 60.0);

        assert_eq!(
            mv.tags(),
            &BTreeMap::from([("my_tag".to_owned(), "my_val".to_owned())])
        );
        assert_eq!(
            mv.expiration_timestamp_ns(),
            DateTime::parse_from_rfc3339("2025-01-01T00:00:00+00:00")
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap() as u64
        );

        assert_eq!(mv.cluster_by(), &["field_1", "field_2"]);

        let partition_by = mv.partition_by().unwrap();
        assert_eq!(partition_by.field, "my_field");
        let BigqueryPartitionConfigInner::Time(partition_time) = &partition_by.__inner__ else {
            panic!("partition_by should be time");
        };
        assert_eq!(partition_time.granularity, "DAY");
    }

    /// Test that the arrow schema returned from the BigQuery driver
    /// can be parsed into a `PartitionConfig`
    #[test]
    fn test_empty_partition_config_from_schema() {
        let tf = Box::new(NaiveTypeOpsImpl::new(AdapterType::Bigquery)) as Box<dyn TypeOps>;

        let schema = Schema::new(Fields::empty());
        assert!(
            BigqueryPartitionConfig::try_from_schema(&schema, tf.as_ref())
                .unwrap()
                .is_none()
        );
    }

    /// Test that the arrow schema returned from the BigQuery driver
    /// can be parsed into a `PartitionConfig`
    #[test]
    fn test_partition_config_from_schema() {
        let tf = Box::new(NaiveTypeOpsImpl::new(AdapterType::Bigquery)) as Box<dyn TypeOps>;
        // Metadata reference: https://github.com/apache/arrow-adbc/blob/f5c0354ac80eaa829c1228e1e0dba7ddc7fd1787/go/adbc/driver/bigquery/connection.go#L703
        let metadata = HashMap::from_iter(
            [
                ("RangePartitioning.Field", "my_field"),
                ("RangePartitioning.Range.Start", "11111"),
                ("RangePartitioning.Range.End", "22222"),
                ("RangePartitioning.Range.Interval", "33333"),
                ("RangePartitioning.RequirePartitionFilter", "true"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        let schema = Schema::new(Fields::empty()).with_metadata(metadata);

        let partition_by = BigqueryPartitionConfig::try_from_schema(&schema, tf.as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(partition_by.field, "my_field");
        let BigqueryPartitionConfigInner::Range(partition_range) = partition_by.__inner__ else {
            panic!("partition_by should be range");
        };
        assert_eq!(partition_range.range.start, 11111);
        assert_eq!(partition_range.range.end, 22222);
        assert_eq!(partition_range.range.interval, 33333);
        // TODO(serramatutu): RequirePartitionFilter is unused
    }
}
