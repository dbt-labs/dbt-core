//! Native PostgreSQL declarative partitioning support (dbt-postgres issue #679).
//!
//! Port of `dbt.adapters.postgres.partitioning` from dbt-adapters. The Jinja macros
//! call `adapter.parse_partition_by` (validation + rendering) and
//! `adapter.get_partition_bounds` (auto range-partition computation); the logic behind
//! both lives here so it can be unit tested independently of the adapter dispatch.
//!
//! <https://www.postgresql.org/docs/current/ddl-partitioning.html>

use chrono::{Datelike, Duration, Months, NaiveDateTime, Timelike};

/// Supported partition methods.
pub const PARTITION_METHODS: [&str; 3] = ["range", "list", "hash"];

/// Supported `range` granularities for auto-managed partitions.
pub const RANGE_GRANULARITIES: [&str; 5] = ["hour", "day", "week", "month", "year"];

/// Upper bound on auto-generated range partitions per build. Guards against, e.g.,
/// `granularity='hour'` over a multi-year range emitting tens of thousands of
/// `CREATE TABLE` statements in a single request.
pub const MAX_AUTO_PARTITIONS: usize = 10000;

/// One auto-computed range partition: a deterministic name plus quoted SQL literals
/// for a `FOR VALUES FROM (..) TO (..)` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBound {
    pub name: String,
    pub from: String,
    pub to: String,
}

/// `strftime`-style pattern used to name a partition at the given granularity.
fn name_format(granularity: &str) -> &'static str {
    match granularity {
        "year" => "%Y",
        "month" => "%Y%m",
        "week" => "%Y%m%d",
        "day" => "%Y%m%d",
        // hour
        _ => "%Y%m%d%H",
    }
}

/// Floor `dt` to the start of its granularity bucket.
fn floor_to_granularity(dt: NaiveDateTime, granularity: &str) -> NaiveDateTime {
    let midnight = dt
        .with_hour(0)
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_nanosecond(0))
        .unwrap_or(dt);
    match granularity {
        "year" => midnight
            .with_month(1)
            .and_then(|d| d.with_day(1))
            .unwrap_or(midnight),
        "month" => midnight.with_day(1).unwrap_or(midnight),
        "week" => {
            let weekday = midnight.weekday().num_days_from_monday() as i64;
            midnight - Duration::days(weekday)
        }
        "day" => midnight,
        // hour: keep the date + hour, zero the rest
        _ => dt
            .with_minute(0)
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_nanosecond(0))
            .unwrap_or(dt),
    }
}

/// Advance `dt` by exactly one bucket of the given granularity.
fn advance(dt: NaiveDateTime, granularity: &str) -> Option<NaiveDateTime> {
    match granularity {
        "year" => dt.checked_add_months(Months::new(12)),
        "month" => dt.checked_add_months(Months::new(1)),
        "week" => Some(dt + Duration::weeks(1)),
        "day" => Some(dt + Duration::days(1)),
        // hour
        _ => Some(dt + Duration::hours(1)),
    }
}

/// Compute the range partitions needed to cover `[minimum, maximum]` at `granularity`.
///
/// Mirrors `compute_partition_bounds` in the Python adapter: returns `PartitionBound`s
/// whose literals are quoted SQL timestamps for a `FOR VALUES FROM (..) TO (..)` clause.
/// Returns an empty list when either bound is `None` (e.g. an empty staged batch).
pub fn compute_partition_bounds(
    minimum: Option<NaiveDateTime>,
    maximum: Option<NaiveDateTime>,
    granularity: &str,
) -> Result<Vec<PartitionBound>, String> {
    let (minimum, maximum) = match (minimum, maximum) {
        (Some(lo), Some(hi)) => (lo, hi),
        _ => return Ok(Vec::new()),
    };

    let fmt = name_format(granularity);
    let mut current = floor_to_granularity(minimum, granularity);
    let mut bounds: Vec<PartitionBound> = Vec::new();

    while current <= maximum {
        if bounds.len() >= MAX_AUTO_PARTITIONS {
            return Err(format!(
                "partition_by would auto-create more than {MAX_AUTO_PARTITIONS} '{granularity}' \
                 partitions for the range [{minimum}, {maximum}]. Use a coarser granularity \
                 or declare explicit `partitions`."
            ));
        }
        let next = advance(current, granularity)
            .ok_or_else(|| format!("partition_by range overflowed at '{current}'"))?;
        bounds.push(PartitionBound {
            name: format!("p{}", current.format(fmt)),
            from: format!("'{}'", current.format("%Y-%m-%d %H:%M:%S")),
            to: format!("'{}'", next.format("%Y-%m-%d %H:%M:%S")),
        });
        current = next;
    }
    Ok(bounds)
}

/// Validate a parsed partition config, mirroring `PostgresPartitionConfig._validate`.
///
/// `method` is the resolved method (already defaulted to `range`). Returns a
/// human-readable error message on the first violation.
pub fn validate_partition_config(
    fields: &[String],
    method: &str,
    granularity: Option<&str>,
) -> Result<(), String> {
    if fields.is_empty() {
        return Err(
            "partition_by requires at least one column in `fields`, but none were provided"
                .to_string(),
        );
    }
    if !PARTITION_METHODS.contains(&method) {
        return Err(format!(
            "Invalid partition_by method '{method}'. Supported methods are: {}",
            PARTITION_METHODS.join(", ")
        ));
    }
    if let Some(granularity) = granularity {
        if !RANGE_GRANULARITIES.contains(&granularity) {
            return Err(format!(
                "Invalid partition_by granularity '{granularity}'. Supported granularities are: {}",
                RANGE_GRANULARITIES.join(", ")
            ));
        }
        if method != "range" {
            return Err(
                "partition_by `granularity` is only supported for the `range` method".to_string(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime};

    fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    #[test]
    fn none_bounds_return_empty() {
        assert_eq!(compute_partition_bounds(None, None, "month").unwrap(), vec![]);
    }

    #[test]
    fn monthly_bounds() {
        let bounds = compute_partition_bounds(
            Some(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap()),
            Some(NaiveDate::from_ymd_opt(2024, 3, 5).unwrap().and_hms_opt(0, 0, 0).unwrap()),
            "month",
        )
        .unwrap();
        let names: Vec<&str> = bounds.iter().map(|b| b.name.as_str()).collect();
        assert_eq!(names, vec!["p202401", "p202402", "p202403"]);
        assert_eq!(bounds[0].from, "'2024-01-01 00:00:00'");
        assert_eq!(bounds[0].to, "'2024-02-01 00:00:00'");
    }

    #[test]
    fn daily_single_and_multi() {
        assert_eq!(compute_partition_bounds(Some(dt(2024, 1, 1)), Some(dt(2024, 1, 1)), "day").unwrap().len(), 1);
        assert_eq!(compute_partition_bounds(Some(dt(2024, 1, 1)), Some(dt(2024, 1, 2)), "day").unwrap().len(), 2);
    }

    #[test]
    fn cap_raises() {
        let err = compute_partition_bounds(Some(dt(2000, 1, 1)), Some(dt(2020, 1, 1)), "hour")
            .unwrap_err();
        assert!(err.contains(&MAX_AUTO_PARTITIONS.to_string()));
    }

    #[test]
    fn validate_rejects_bad_method() {
        let err = validate_partition_config(&["id".to_string()], "nonsense", None).unwrap_err();
        assert!(err.contains("method"));
    }

    #[test]
    fn validate_rejects_empty_fields() {
        let err = validate_partition_config(&[], "range", None).unwrap_err();
        assert!(err.contains("at least one column"));
    }

    #[test]
    fn validate_rejects_granularity_without_range() {
        let err =
            validate_partition_config(&["id".to_string()], "hash", Some("day")).unwrap_err();
        assert!(err.contains("granularity"));
    }

    #[test]
    fn validate_rejects_bad_granularity() {
        let err = validate_partition_config(&["created_at".to_string()], "range", Some("fortnight"))
            .unwrap_err();
        assert!(err.contains("granularity"));
    }
}
