//! Relation and RelationConfig implementations for different data warehouses.

pub(crate) mod config;
pub use config::{BaseRelationChangeSet, BaseRelationConfig, ComponentConfig, RelationChangeSet};

// Relation and RelationConfig for different data warehouses
pub mod bigquery;
pub mod databricks;
pub mod fabric;
pub mod parse;
pub mod postgres;
pub mod redshift;
pub mod salesforce;
pub mod snowflake;

mod relation_object;
pub use relation_object::{
    RelationObject, StaticBaseRelation, StaticBaseRelationObject, create_relation,
    create_relation_from_node, do_create_relation,
};

pub(crate) mod config_v2;

#[cfg(test)]
pub(crate) mod test_helpers;

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, Utc};
    use dbt_schemas::{
        filter::{RunFilter, Sample},
        schemas::{
            common::ResolvedQuoting,
            relations::base::{BaseRelation as _, TableFormat},
        },
    };

    use crate::AdapterType;

    use super::*;

    #[test]
    fn test_render_with_run_filter_snowflake_adapter() {
        let relation = snowflake::SnowflakeRelation::new(
            None,
            None,
            Some("my_table".to_owned()),
            None,
            TableFormat::Default,
            ResolvedQuoting::disabled(),
        );
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 7, 8)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: Some(DateTime::<Utc>::from_naive_utc_and_offset(end, Utc)),
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let event_time = Some("created_at".to_string());

        let result = relation.render_with_run_filter_as_str(&run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from my_table where created_at >= to_timestamp_tz('2024-07-01T00:00:00') and created_at < to_timestamp_tz('2024-07-08T18:00:00'))"
        );
    }

    #[test]
    fn test_render_with_run_filter_bigquery_adapter() {
        let relation = bigquery::BigqueryRelation::new(
            None,
            None,
            Some("my_table".to_owned()),
            None,
            None,
            ResolvedQuoting::disabled(),
        );
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 7, 8)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: Some(DateTime::<Utc>::from_naive_utc_and_offset(end, Utc)),
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let event_time = Some("created_at".to_string());

        let result = relation.render_with_run_filter_as_str(&run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from my_table where cast(created_at as timestamp) >= '2024-07-01T00:00:00' and cast(created_at as timestamp) < '2024-07-08T18:00:00')"
        );
    }

    #[test]
    fn test_render_with_run_filter_redshift_adapter() {
        // relation impl in core doesn't seem to override this
        let relation = redshift::RedshiftRelation::new(
            None,
            None,
            Some("my_table".to_owned()),
            None,
            None,
            ResolvedQuoting::disabled(),
        );
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 7, 8)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: Some(DateTime::<Utc>::from_naive_utc_and_offset(end, Utc)),
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let event_time = Some("created_at".to_string());

        let result = relation.render_with_run_filter_as_str(&run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from my_table where created_at >= '2024-07-01T00:00:00' and created_at < '2024-07-08T18:00:00')"
        );
    }

    #[test]
    fn test_render_with_run_filter_databricks_adapter() {
        // relation impl in dbt-databricks doesn't seem to override this
        let relation = databricks::DatabricksRelation::new(
            AdapterType::Databricks, // ?
            None,
            None,
            Some("my_table".to_owned()),
            None,
            None,
            ResolvedQuoting::disabled(),
            None,
            false,
            false,
        );
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 7, 8)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: Some(DateTime::<Utc>::from_naive_utc_and_offset(end, Utc)),
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let event_time = Some("created_at".to_string());

        let result = relation.render_with_run_filter_as_str(&run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from my_table where created_at >= '2024-07-01T00:00:00' and created_at < '2024-07-08T18:00:00')"
        );
    }
}
