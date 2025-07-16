use dbt_common::current_function_name;
use dbt_serde_yaml::{JsonSchema, UntaggedEnumDeserialize};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State};
use minijinja::{
    arg_utils::ArgParser,
    listener::RenderingEventListener,
    value::{Object, Value as MinijinjaValue},
};
use serde::{Deserialize, Serialize};
use std::{rc::Rc, sync::Arc};
use strum::{Display, EnumString};

use crate::schemas::columns::base::StdColumn;

/// dbt-core allows either of the variants for the `partition_by` in the model config
/// but the bigquery-adapter throws RunTime error
/// the behaviors are tested from the latest dbt-core + bigquery-adapter as this is written
/// we're conformant to this behavior via here and via the `validate` method
#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum BigqueryPartitionConfigLegacy {
    String(String),
    List(Vec<String>),
    BigqueryPartitionConfig(BigqueryPartitionConfig),
}

/// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/relation_configs/_partition.py#L12-L13
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct BigqueryPartitionConfig {
    pub field: String,
    #[serde(default = "BigqueryPartitionConfig::default_data_type")]
    pub data_type: String,
    #[serde(flatten)]
    pub inner: BigqueryPartitionConfigInner,
    #[serde(default)]
    pub time_ingestion_partitioning: bool,
    #[serde(default)]
    pub copy_partitions: bool,
}

/// Enum representing all field names in BigqueryPartitionConfig
#[derive(Debug, Clone, EnumString, Display)]
#[strum(serialize_all = "snake_case")]
enum PartitionConfigField {
    Field,
    DataType,
    TimeIngestionPartitioning,
    CopyPartitions,
    // Flattened field for TimeConfig
    Granularity,
    // Flattened field for RangeConfig
    Range,
}

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum BigqueryPartitionConfigInner {
    Range(RangeConfig),
    Time(TimeConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct TimeConfig {
    #[serde(default = "BigqueryPartitionConfig::default_granularity")]
    pub granularity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct RangeConfig {
    pub range: Range,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct Range {
    pub start: u64,
    pub end: u64,
    pub interval: u64,
}

/// dbt-core allows either of the variants for the `cluster_by`
/// to allow cluster on a single column or on multiple columns
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum BigqueryClusterConfig {
    String(String),
    List(Vec<String>),
}

impl BigqueryPartitionConfigLegacy {
    pub fn validate(self) -> Result<BigqueryPartitionConfig, MinijinjaError> {
        match self {
            BigqueryPartitionConfigLegacy::BigqueryPartitionConfig(config) => Ok(config),
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                "Expect a BigqueryPartitionConfigStruct",
            )),
        }
    }
}

impl BigqueryPartitionConfig {
    const PARTITION_DATE: &str = "_PARTITIONDATE";
    const PARTITION_TIME: &str = "_PARTITIONTIME";

    pub fn granularity(&self) -> Result<String, MinijinjaError> {
        match &self.inner {
            BigqueryPartitionConfigInner::Time(TimeConfig { granularity }) => {
                Ok(granularity.to_string())
            }
            BigqueryPartitionConfigInner::Range(_) => Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                "RangeConfig does not have a granularity",
            )),
        }
    }

    pub fn range(&self) -> Result<Range, MinijinjaError> {
        match &self.inner {
            BigqueryPartitionConfigInner::Range(RangeConfig { range }) => Ok(range.clone()),
            BigqueryPartitionConfigInner::Time(_) => Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                "TimeConfig does not have a range",
            )),
        }
    }

    pub fn default_data_type() -> String {
        "date".to_string()
    }

    pub fn default_granularity() -> String {
        "day".to_string()
    }

    /// Return the data type of partitions for replacement.
    /// When time_ingestion_partitioning is enabled, the data type supported are date & timestamp.
    pub fn data_type_for_partition(&self) -> Result<MinijinjaValue, MinijinjaError> {
        let data_type = if !self.time_ingestion_partitioning || self.data_type == "date" {
            self.data_type.as_str()
        } else {
            "timestamp"
        };
        Ok(MinijinjaValue::from(data_type))
    }

    pub fn reject_partition_field_column(
        &self,
        args: &[MinijinjaValue],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        parser.check_num_args(current_function_name!(), 0, 1)?;

        let columns = parser.get::<MinijinjaValue>("columns")?;
        if let Some(columns) = columns.downcast_object::<Vec<StdColumn>>() {
            let columns = columns
                .iter()
                .filter(|c| c.name.to_uppercase() != self.field.to_uppercase())
                .collect::<Vec<_>>();
            Ok(MinijinjaValue::from_serialize(columns))
        } else {
            Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                "columns must be a list of StdColumn",
            ))
        }
    }

    /// Return true if the data type should be truncated instead of cast to the data type
    pub fn data_type_should_be_truncated(&self) -> bool {
        !(self.data_type == "int64"
            || (self.data_type == "date"
                && match &self.inner {
                    BigqueryPartitionConfigInner::Time(TimeConfig { granularity }) => {
                        granularity == "day"
                    }
                    BigqueryPartitionConfigInner::Range(_) => {
                        unreachable!("when data_type is date, inner must be a TimeConfig")
                    }
                }))
    }

    /// Return the time partitioning field name based on the data type.
    /// The default is _PARTITIONTIME, but for date it is _PARTITIONDATE
    pub fn time_partitioning_field(&self) -> Result<MinijinjaValue, MinijinjaError> {
        let field = if self.data_type == "date" {
            Self::PARTITION_DATE
        } else {
            Self::PARTITION_TIME
        };
        Ok(MinijinjaValue::from(field))
    }

    /// Return the insertable time partitioning field name based on the data type.
    /// Practically, only _PARTITIONTIME works so far.
    pub fn insertable_time_partitioning_field(&self) -> Result<MinijinjaValue, MinijinjaError> {
        Ok(MinijinjaValue::from(Self::PARTITION_TIME))
    }

    /// Render the partition expression
    pub fn render(&self, alias: Option<String>) -> Result<MinijinjaValue, MinijinjaError> {
        let column = if !self.time_ingestion_partitioning {
            self.field.to_owned()
        } else {
            self.time_partitioning_field()?
                .as_str()
                .expect("time_partitioning_field must be a string")
                .to_owned()
        };

        let column = if let Some(alias) = &alias {
            format!("{alias}.{column}")
        } else {
            column
        };

        let result = if self.data_type_should_be_truncated() {
            format!(
                "{}_trunc({}, {})",
                self.data_type,
                column,
                self.granularity()?
            )
        } else {
            column
        };

        Ok(MinijinjaValue::from(result))
    }

    /// Wrap the partitioning column when time involved to ensure it is properly cast to matching time
    pub fn render_wrapped(
        &self,
        args: &[MinijinjaValue],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        parser.check_num_args(current_function_name!(), 0, 1)?;

        let alias = parser
            .get_optional::<MinijinjaValue>("alias")
            .map(|a| {
                a.as_str().map(String::from).ok_or_else(|| {
                    MinijinjaError::new(
                        MinijinjaErrorKind::InvalidArgument,
                        "alias must be a string",
                    )
                })
            })
            .transpose()?;

        if (self.data_type == "date"
            || self.data_type == "timestamp"
            || self.data_type == "datetime")
            && !self.data_type_should_be_truncated()
            && !(self.time_ingestion_partitioning && self.data_type == "date")
        {
            Ok(MinijinjaValue::from(format!(
                "{}({})",
                self.data_type,
                self.render(alias)?.as_str().unwrap()
            )))
        } else {
            self.render(alias)
        }
    }
}

impl Object for BigqueryPartitionConfig {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        let key_str = key.as_str()?;
        let field = PartitionConfigField::try_from(key_str).ok()?;

        match field {
            PartitionConfigField::Field => Some(MinijinjaValue::from(self.field.clone())),
            PartitionConfigField::DataType => Some(MinijinjaValue::from(self.data_type.clone())),
            PartitionConfigField::Granularity => self.granularity().map(MinijinjaValue::from).ok(),
            PartitionConfigField::TimeIngestionPartitioning => {
                Some(MinijinjaValue::from(self.time_ingestion_partitioning))
            }
            PartitionConfigField::CopyPartitions => {
                Some(MinijinjaValue::from(self.copy_partitions))
            }
            PartitionConfigField::Range => self.range().map(MinijinjaValue::from_serialize).ok(),
        }
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        match name {
            "data_type_for_partition" => self.data_type_for_partition(),
            "reject_partition_field_column" => self.reject_partition_field_column(args),
            "time_partitioning_field" => self.time_partitioning_field(),
            "render_wrapped" => self.render_wrapped(args),
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!("Unknown method on PartitionConfig object: '{name}'"),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct GrantAccessToTarget {
    pub dataset: Option<String>,
    pub project: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_deserialize_time_partition_config() {
        let json = json!({
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "hour",
        });

        let config: BigqueryPartitionConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(
            config.inner,
            BigqueryPartitionConfigInner::Time(_)
        ));
    }

    #[test]
    fn test_deserialize_range_partition_config() {
        let json = json!({
            "field": "user_id",
            "data_type": "int64",
            "range": {
                "start": 0,
                "end": 100,
                "interval": 10
            },
        });

        let config: BigqueryPartitionConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(
            config.inner,
            BigqueryPartitionConfigInner::Range(_)
        ));
        assert!(!config.time_ingestion_partitioning);
        assert!(!config.copy_partitions);
    }

    #[test]
    fn test_deserialize_with_defaults() {
        let json = json!({
            "field": "created_at"
        });

        let config: BigqueryPartitionConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.field, "created_at");
        assert_eq!(config.data_type, "date"); // default
        assert!(
            matches!(config.inner, BigqueryPartitionConfigInner::Time(TimeConfig { granularity }) if granularity == "day")
        ); // default
        assert!(!config.time_ingestion_partitioning); // default
        assert!(!config.copy_partitions); // default
    }

    #[test]
    fn test_partition_config_field_enum_covers_all_fields_time_config() {
        // Create a sample config with time partitioning
        let config = BigqueryPartitionConfig {
            field: "test_field".to_string(),
            data_type: "date".to_string(),
            inner: BigqueryPartitionConfigInner::Time(TimeConfig {
                granularity: "day".to_string(),
            }),
            time_ingestion_partitioning: false,
            copy_partitions: false,
        };

        // Serialize to JSON to get all field names
        let json_value = serde_json::to_value(&config).unwrap();
        let json_object = json_value.as_object().unwrap();

        // Test that all JSON fields can be parsed by our enum (except flattened fields)
        for field_name in json_object.keys() {
            assert!(
                PartitionConfigField::try_from(field_name.as_str()).is_ok(),
                "Field '{field_name}' should be parseable by PartitionConfigField enum"
            );
        }

        // Test that a bogus field is rejected
        assert!(PartitionConfigField::try_from("invalid_field").is_err());
    }

    #[test]
    fn test_partition_config_field_enum_covers_all_fields_range_config() {
        // Create a sample config with range partitioning
        let config = BigqueryPartitionConfig {
            field: "user_id".to_string(),
            data_type: "int64".to_string(),
            inner: BigqueryPartitionConfigInner::Range(RangeConfig {
                range: Range {
                    start: 0,
                    end: 100,
                    interval: 10,
                },
            }),
            time_ingestion_partitioning: true,
            copy_partitions: true,
        };

        // Serialize to JSON to get all field names
        let json_value = serde_json::to_value(&config).unwrap();
        let json_object = json_value.as_object().unwrap();

        // Test that all JSON fields can be parsed by our enum (except nested range fields)
        for field_name in json_object.keys() {
            assert!(
                PartitionConfigField::try_from(field_name.as_str()).is_ok(),
                "Field '{field_name}' should be parseable by PartitionConfigField enum"
            );
        }
    }

    #[test]
    fn test_get_value_returns_correct_values() {
        // Test with time config
        let time_config = Arc::new(BigqueryPartitionConfig {
            field: "created_at".to_string(),
            data_type: "timestamp".to_string(),
            inner: BigqueryPartitionConfigInner::Time(TimeConfig {
                granularity: "hour".to_string(),
            }),
            time_ingestion_partitioning: true,
            copy_partitions: false,
        });

        // Test field values
        assert_eq!(
            time_config
                .get_value(&MinijinjaValue::from("field"))
                .unwrap(),
            MinijinjaValue::from("created_at")
        );
        assert_eq!(
            time_config
                .get_value(&MinijinjaValue::from("data_type"))
                .unwrap(),
            MinijinjaValue::from("timestamp")
        );
        assert_eq!(
            time_config
                .get_value(&MinijinjaValue::from("granularity"))
                .unwrap(),
            MinijinjaValue::from("hour")
        );
        assert_eq!(
            time_config
                .get_value(&MinijinjaValue::from("time_ingestion_partitioning"))
                .unwrap(),
            MinijinjaValue::from(true)
        );
        assert_eq!(
            time_config
                .get_value(&MinijinjaValue::from("copy_partitions"))
                .unwrap(),
            MinijinjaValue::from(false)
        );

        // Test with range config (granularity should return None)
        let range_config = Arc::new(BigqueryPartitionConfig {
            field: "user_id".to_string(),
            data_type: "int64".to_string(),
            inner: BigqueryPartitionConfigInner::Range(RangeConfig {
                range: Range {
                    start: 0,
                    end: 100,
                    interval: 10,
                },
            }),
            time_ingestion_partitioning: false,
            copy_partitions: true,
        });

        assert_eq!(
            range_config
                .get_value(&MinijinjaValue::from("field"))
                .unwrap(),
            MinijinjaValue::from("user_id")
        );
        assert_eq!(
            range_config
                .get_value(&MinijinjaValue::from("data_type"))
                .unwrap(),
            MinijinjaValue::from("int64")
        );
        // granularity should return None for range config
        assert!(
            range_config
                .get_value(&MinijinjaValue::from("granularity"))
                .is_none()
        );
        assert_eq!(
            range_config
                .get_value(&MinijinjaValue::from("copy_partitions"))
                .unwrap(),
            MinijinjaValue::from(true)
        );

        // Test that invalid fields return None
        assert!(
            time_config
                .get_value(&MinijinjaValue::from("invalid_field"))
                .is_none()
        );
        assert!(
            range_config
                .get_value(&MinijinjaValue::from("invalid_field"))
                .is_none()
        );
    }
}
