//! SQL diffing utilities for evaluating replay of recorded events.
//!
//! This module provides a validation engine for comparing incoming events against
//! recorded events, with support for:
//! - Known deviations that should skip validation
//! - SQL extraction and sanitization for meaningful comparison
//! - Pluggable sanitizers to clean dynamic content from SQL

use std::fmt;

use similar::{ChangeTag, TextDiff};

use crate::time_machine::event::AdapterCallEvent;

/// An incoming event being compared against a recorded event.
#[derive(Debug, Clone)]
pub struct IncomingEvent<'a> {
    pub node_id: &'a str,
    pub method: &'a str,
    pub args: &'a serde_json::Value,
}

impl<'a> IncomingEvent<'a> {
    pub fn new(node_id: &'a str, method: &'a str, args: &'a serde_json::Value) -> Self {
        Self {
            node_id,
            method,
            args,
        }
    }
}

impl<'a> From<(&'a str, &'a str, &'a serde_json::Value)> for IncomingEvent<'a> {
    fn from((node_id, method, args): (&'a str, &'a str, &'a serde_json::Value)) -> Self {
        Self::new(node_id, method, args)
    }
}

/// An event with SQL extracted and sanitized for comparison.
#[derive(Debug, Clone)]
pub struct SqlEvent<'a> {
    /// The original node ID
    pub _node_id: &'a str,
    /// The method name
    pub _method: &'a str,
    /// The sanitized SQL string
    pub sanitized_sql: String,
    /// The original raw SQL before sanitization
    pub raw_sql: &'a str,
    /// Non-SQL args
    pub _other_args: serde_json::Value,
}

impl<'a> SqlEvent<'a> {
    /// Create from an incoming event, extracting and sanitizing SQL.
    pub fn from_incoming(
        incoming: &'a IncomingEvent<'a>,
        sanitizers: &[Box<dyn SqlSanitizer>],
    ) -> Option<Self> {
        let raw_sql = extract_sql_from_args(incoming.args)?;
        let sanitized_sql = apply_sanitizers(raw_sql, sanitizers);
        let other_args = extract_non_sql_args(incoming.args);

        Some(Self {
            _node_id: incoming.node_id,
            _method: incoming.method,
            sanitized_sql,
            raw_sql,
            _other_args: other_args,
        })
    }

    /// Create from a recorded event, extracting and sanitizing SQL.
    pub fn from_recorded(
        recorded: &'a AdapterCallEvent,
        sanitizers: &[Box<dyn SqlSanitizer>],
    ) -> Option<Self> {
        let raw_sql = extract_sql_from_args(&recorded.args)?;
        let sanitized_sql = apply_sanitizers(raw_sql, sanitizers);
        let other_args = extract_non_sql_args(&recorded.args);

        Some(Self {
            _node_id: &recorded.node_id,
            _method: &recorded.method,
            sanitized_sql,
            raw_sql,
            _other_args: other_args,
        })
    }
}

// TODO(jason): Hook into telemetry or logging for skips or sanitization so we know what was applied
// would help debug artifacts that should be failing but are passing

/// Trait for known deviations that should skip validation.
///
/// Implement this trait to define patterns that are expected to differ
/// between recording and replay.
pub trait KnownDeviation: Send + Sync {
    /// Name of this deviation rule.
    fn name(&self) -> &str;

    /// Human-readable reason why this is an expected deviation.
    fn reason(&self) -> &str;

    /// Check if the incoming event matches this deviation pattern.
    fn check(&self, incoming: &IncomingEvent, recorded: &AdapterCallEvent) -> DeviationMatch<'_>;
}

/// Result of checking for a known deviation.
#[derive(Debug)]
pub enum DeviationMatch<'a> {
    /// No deviation rule matched - validation should proceed normally
    None,
    /// A known deviation was matched - validation should be skipped
    Matched(MatchedDeviation<'a>),
}

/// Details about a matched deviation.
#[derive(Debug)]
pub struct MatchedDeviation<'a> {
    /// Name of the deviation rule that matched
    #[allow(dead_code)]
    pub rule_name: &'a str,
    /// Human-readable reason why this is an expected deviation
    #[allow(dead_code)]
    pub reason: &'a str,
}

/// Trait for SQL sanitizers that clean dynamic content.
///
/// Implement this trait to define patterns in SQL that should be
/// normalized before comparison (e.g., timestamps, UUIDs, etc.)
pub trait SqlSanitizer: Send + Sync {
    /// Name of this sanitizer.
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Sanitize the SQL string, returning the cleaned version.
    fn sanitize(&self, sql: &str) -> String;
}

/// Result of event validation.
#[derive(Debug)]
pub enum ValidationResult<'a> {
    /// Events match
    Match,
    /// Validation was skipped due to a known deviation
    #[allow(dead_code)]
    Skipped(MatchedDeviation<'a>),
    /// Events differ
    Mismatch(ValidationMismatch),
}

/// Details about a validation mismatch.
#[derive(Debug)]
pub struct ValidationMismatch {
    /// Type of mismatch
    pub kind: MismatchKind,
    /// Expected value
    pub expected: String,
    /// Actual value
    pub actual: String,
}

/// Kind of mismatch found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchKind {
    /// Method names don't match
    Method,
    /// SQL content differs
    Sql,
    /// Recorded event has SQL but incoming does not
    MissingSqlIncoming,
    /// Incoming event has SQL but recorded does not
    MissingSqlRecorded,
    /// Non-SQL arguments differ
    Args,
}

impl fmt::Display for ValidationMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            MismatchKind::Method => {
                write!(
                    f,
                    "method mismatch: expected '{}', got '{}'",
                    self.expected, self.actual
                )
            }
            MismatchKind::Sql => {
                writeln!(f, "SQL mismatch:")?;
                let diff = TextDiff::from_lines(&self.expected, &self.actual);
                for change in diff.iter_all_changes() {
                    let prefix = match change.tag() {
                        ChangeTag::Delete => "-",
                        ChangeTag::Insert => "+",
                        ChangeTag::Equal => " ",
                    };
                    write!(f, "{}{}", prefix, change)?;
                }
                Ok(())
            }
            MismatchKind::MissingSqlIncoming => {
                write!(
                    f,
                    "SQL missing in incoming event, expected:\n{}",
                    self.expected
                )
            }
            MismatchKind::MissingSqlRecorded => {
                write!(f, "SQL missing in recorded event, got:\n{}", self.actual)
            }
            MismatchKind::Args => {
                write!(
                    f,
                    "args mismatch:\n  expected: {}\n  actual: {}",
                    self.expected, self.actual
                )
            }
        }
    }
}

/// The main validation engine for time machine event comparison.
pub struct TimeMachineEventValidationEngine {
    /// Registered deviation rules
    deviations: Vec<Box<dyn KnownDeviation>>,
    /// Registered SQL sanitizers
    sanitizers: Vec<Box<dyn SqlSanitizer>>,
}

impl Default for TimeMachineEventValidationEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeMachineEventValidationEngine {
    /// Create a new validation engine with default rules.
    pub fn new() -> Self {
        let mut engine = Self {
            deviations: Vec::new(),
            sanitizers: Vec::new(),
        };

        // Register deviations
        engine.register_deviation(Box::new(DbtPovModelCostCalculatorDeviation));

        // Register sanitizers
        engine.register_sanitizer(Box::new(TimestampSanitizer));
        engine.register_sanitizer(Box::new(QueryTagSanitizer));
        // Whitespace sanitizer should be last to normalize after other sanitizers
        engine.register_sanitizer(Box::new(WhitespaceSanitizer));

        engine
    }

    /// Register a deviation rule.
    pub fn register_deviation(&mut self, deviation: Box<dyn KnownDeviation>) {
        self.deviations.push(deviation);
    }

    /// Register a SQL sanitizer.
    pub fn register_sanitizer(&mut self, sanitizer: Box<dyn SqlSanitizer>) {
        self.sanitizers.push(sanitizer);
    }

    /// Validate an incoming event against a recorded event.
    pub fn validate(
        &self,
        incoming: &IncomingEvent,
        recorded: &AdapterCallEvent,
    ) -> ValidationResult<'_> {
        // 1. Check for known deviations first
        for deviation in &self.deviations {
            if let DeviationMatch::Matched(matched) = deviation.check(incoming, recorded) {
                return ValidationResult::Skipped(matched);
            }
        }

        // 2. Check method match
        if incoming.method != recorded.method {
            return ValidationResult::Mismatch(ValidationMismatch {
                kind: MismatchKind::Method,
                expected: recorded.method.clone(),
                actual: incoming.method.to_string(),
            });
        }

        // 3. For SQL methods, do SQL comparison
        if is_sql_method(incoming.method) {
            return self.validate_sql_event(incoming, recorded);
        }

        // 4. For non SQL methods, compare args directly
        if !super::serde::values_match(&recorded.args, incoming.args) {
            return ValidationResult::Mismatch(ValidationMismatch {
                kind: MismatchKind::Args,
                expected: recorded.args.to_string(),
                actual: incoming.args.to_string(),
            });
        }

        ValidationResult::Match
    }

    /// Validate SQL events specifically.
    fn validate_sql_event(
        &self,
        incoming: &IncomingEvent,
        recorded: &AdapterCallEvent,
    ) -> ValidationResult<'_> {
        // Extract SQL from both events
        let incoming_sql = SqlEvent::from_incoming(incoming, &self.sanitizers);
        let recorded_sql = SqlEvent::from_recorded(recorded, &self.sanitizers);

        match (incoming_sql, recorded_sql) {
            (Some(inc), Some(rec)) => {
                // Compare using fully sanitized SQL
                if inc.sanitized_sql == rec.sanitized_sql {
                    ValidationResult::Match
                } else {
                    // Format the sanitized SQL for readable diff display
                    ValidationResult::Mismatch(ValidationMismatch {
                        kind: MismatchKind::Sql,
                        expected: format_sql_for_display(&rec.sanitized_sql),
                        actual: format_sql_for_display(&inc.sanitized_sql),
                    })
                }
            }
            (None, None) => ValidationResult::Match,
            (None, Some(inc)) => {
                // Incoming has SQL but recorded does not
                ValidationResult::Mismatch(ValidationMismatch {
                    kind: MismatchKind::MissingSqlRecorded,
                    expected: "(no SQL in recorded event)".to_string(),
                    actual: inc.raw_sql.to_string(),
                })
            }
            (Some(rec), None) => {
                // Recorded has SQL but incoming does not
                ValidationResult::Mismatch(ValidationMismatch {
                    kind: MismatchKind::MissingSqlIncoming,
                    expected: rec.raw_sql.to_string(),
                    actual: "(no SQL in incoming event)".to_string(),
                })
            }
        }
    }
}

/// Check if a method executes SQL.
fn is_sql_method(method: &str) -> bool {
    method == "execute" || method == "run_query"
}

/// Extract SQL string from args.
fn extract_sql_from_args(args: &serde_json::Value) -> Option<&str> {
    match args {
        serde_json::Value::String(s) => Some(s.as_str()),
        serde_json::Value::Array(arr) => arr.first().and_then(|v| v.as_str()),
        _ => None,
    }
}

/// Extract non SQL args
fn extract_non_sql_args(args: &serde_json::Value) -> serde_json::Value {
    match args {
        serde_json::Value::Array(arr) if !arr.is_empty() => {
            serde_json::Value::Array(arr[1..].to_vec())
        }
        _ => serde_json::Value::Array(vec![]),
    }
}

/// Apply all sanitizers to a SQL string.
fn apply_sanitizers(sql: &str, sanitizers: &[Box<dyn SqlSanitizer>]) -> String {
    let mut result = sql.to_string();
    for sanitizer in sanitizers {
        result = sanitizer.sanitize(&result);
    }
    result
}

// TODO(jason): A real SQL formatter...
/// Format normalized SQL for readable diff display by looking at certain keywords
fn format_sql_for_display(normalized_sql: &str) -> String {
    static RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
        regex::Regex::new(
            r"(?i)\b(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|OUTER JOIN|CROSS JOIN|ON|GROUP BY|ORDER BY|HAVING|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|WITH|AS \(|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|COPY GRANTS|VALUES)\b"
        ).expect("valid regex")
    });

    RE.replace_all(normalized_sql, "\n$1").trim().to_string()
}

// ============================================================================
// Deviations
// ============================================================================

/// Deviation for dbt_pov_model_cost_calculator package.
///
/// This package generates dynamic SQL with:
/// - Execution times
/// - Timestamps
/// - Invocation IDs
/// - Run IDs
pub struct DbtPovModelCostCalculatorDeviation;

impl KnownDeviation for DbtPovModelCostCalculatorDeviation {
    fn name(&self) -> &'static str {
        "dbt_pov_model_cost_calculator"
    }

    fn reason(&self) -> &'static str {
        "Package generates dynamic SQL with runtime metrics (execution times, timestamps, invocation IDs) that differ between runs"
    }

    fn check(&self, incoming: &IncomingEvent, _recorded: &AdapterCallEvent) -> DeviationMatch<'_> {
        if incoming.node_id.contains("dbt_pov_model_cost_calculator") {
            DeviationMatch::Matched(MatchedDeviation {
                rule_name: self.name(),
                reason: self.reason(),
            })
        } else {
            DeviationMatch::None
        }
    }
}

/// Sanitizer for ISO timestamp patterns.
pub struct TimestampSanitizer;

impl SqlSanitizer for TimestampSanitizer {
    fn name(&self) -> &'static str {
        "timestamp"
    }

    fn sanitize(&self, sql: &str) -> String {
        // Match ISO 8601 timestamps: 2026-01-23T00:49:47.760170 or with timezone
        static RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
            regex::Regex::new(
                r"'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?'",
            )
            .expect("valid regex")
        });
        RE.replace_all(sql, "'<TIMESTAMP>'").to_string()
    }
}

/// Sanitizer for query_tag session settings.
///
/// Query tags contain dynamic values like thread IDs that differ between runs.
/// Example: `alter session set query_tag = '{"app": "dbt", "thread_id": "Thread-2"}'`
pub struct QueryTagSanitizer;

impl SqlSanitizer for QueryTagSanitizer {
    fn name(&self) -> &'static str {
        "query_tag"
    }

    fn sanitize(&self, sql: &str) -> String {
        // Match: alter session set query_tag = '...' (case insensitive)
        static RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
            regex::Regex::new(r"(?i)(alter\s+session\s+set\s+query_tag\s*=\s*)'[^']*'")
                .expect("valid regex")
        });
        RE.replace_all(sql, "${1}''").to_string()
    }
}

/// Sanitizer that normalizes whitespace by collapsing consecutive whitespace
/// characters into single spaces.
///
/// This handles cases where SQL semantically equivalent but differs only in
/// whitespace formatting (e.g., extra newlines between keywords).
pub struct WhitespaceSanitizer;

impl SqlSanitizer for WhitespaceSanitizer {
    fn name(&self) -> &'static str {
        "whitespace"
    }

    fn sanitize(&self, sql: &str) -> String {
        // Collapse all consecutive whitespace (spaces, tabs, newlines) into single space
        static RE: once_cell::sync::Lazy<regex::Regex> =
            once_cell::sync::Lazy::new(|| regex::Regex::new(r"\s+").expect("valid regex"));
        RE.replace_all(sql, " ").trim().to_string()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time_machine::semantic::SemanticCategory;

    fn make_recorded_event(
        node_id: &str,
        method: &str,
        args: serde_json::Value,
    ) -> AdapterCallEvent {
        AdapterCallEvent {
            node_id: node_id.to_string(),
            seq: 0,
            method: method.to_string(),
            semantic_category: SemanticCategory::Write,
            args,
            result: serde_json::json!(null),
            success: true,
            error: None,
            timestamp_ns: 0,
        }
    }

    #[test]
    fn test_dbt_pov_model_cost_calculator_deviation() {
        let engine = TimeMachineEventValidationEngine::new();
        let args = serde_json::json!([]);

        let incoming = IncomingEvent::new(
            "operation.dbt_pov_model_cost_calculator.dbt_pov_model_cost_calculator-on_run_end-0",
            "execute",
            &args,
        );
        let recorded = make_recorded_event(incoming.node_id, "execute", args.clone());

        let result = engine.validate(&incoming, &recorded);
        assert!(matches!(result, ValidationResult::Skipped(_)));
    }

    #[test]
    fn test_normal_model_validates() {
        let engine = TimeMachineEventValidationEngine::new();
        let args = serde_json::json!(["SELECT 1"]);

        let incoming = IncomingEvent::new("model.my_project.my_model", "execute", &args);
        let recorded = make_recorded_event(incoming.node_id, "execute", args.clone());

        let result = engine.validate(&incoming, &recorded);
        assert!(matches!(result, ValidationResult::Match));
    }

    #[test]
    fn test_sql_mismatch_detected() {
        let engine = TimeMachineEventValidationEngine::new();
        let incoming_args = serde_json::json!(["SELECT 1"]);
        let recorded_args = serde_json::json!(["SELECT 2"]);

        let incoming = IncomingEvent::new("model.my_project.my_model", "execute", &incoming_args);
        let recorded = make_recorded_event(incoming.node_id, "execute", recorded_args);

        let result = engine.validate(&incoming, &recorded);
        assert!(matches!(
            result,
            ValidationResult::Mismatch(ValidationMismatch {
                kind: MismatchKind::Sql,
                ..
            })
        ));
    }

    #[test]
    fn test_timestamp_sanitizer() {
        let sanitizer = TimestampSanitizer;
        let sql = "INSERT INTO t (ts) VALUES ('2026-01-21T01:04:12.066273')";
        let sanitized = sanitizer.sanitize(sql);
        assert_eq!(sanitized, "INSERT INTO t (ts) VALUES ('<TIMESTAMP>')");
    }

    #[test]
    fn test_query_tag_sanitizer() {
        let sanitizer = QueryTagSanitizer;
        let sql = r#"alter session set query_tag = '{"app": "dbt", "dbt_snowflake_query_tags_version": "2.5.0", "is_incremental": false, "thread_id": "Thread-2"}'"#;
        let sanitized = sanitizer.sanitize(sql);
        assert_eq!(sanitized, "alter session set query_tag = ''");
    }

    #[test]
    fn test_query_tag_sanitizer_case_insensitive() {
        let sanitizer = QueryTagSanitizer;
        let sql = r#"ALTER SESSION SET QUERY_TAG = '{"thread_id": "Thread-5"}'"#;
        let sanitized = sanitizer.sanitize(sql);
        assert_eq!(sanitized, "ALTER SESSION SET QUERY_TAG = ''");
    }

    #[test]
    fn test_whitespace_sanitizer() {
        let sanitizer = WhitespaceSanitizer;
        // Simulates the actual diff: "copy grants as (" vs "copy grants\n\n\n  as ("
        let sql1 = "  copy grants as (";
        let sql2 = "  copy grants\n\n\n  as (";
        assert_eq!(sanitizer.sanitize(sql1), sanitizer.sanitize(sql2));
        assert_eq!(sanitizer.sanitize(sql2), "copy grants as (");
    }

    #[test]
    fn test_whitespace_sanitizer_multiline() {
        let sanitizer = WhitespaceSanitizer;
        let sql = "SELECT\n    a,\n    b\nFROM\n    t";
        let sanitized = sanitizer.sanitize(sql);
        assert_eq!(sanitized, "SELECT a, b FROM t");
    }

    #[test]
    fn test_format_sql_for_display() {
        // Normalized SQL should be reformatted with newlines at keywords
        let normalized = "SELECT a, b FROM t WHERE x = 1 AND y = 2 ORDER BY a";
        let formatted = format_sql_for_display(normalized);
        assert!(formatted.contains('\n'), "Should have newlines");
        assert!(formatted.contains("\nFROM"), "Should break at FROM");
        assert!(formatted.contains("\nWHERE"), "Should break at WHERE");
        assert!(formatted.contains("\nAND"), "Should break at AND");
        assert!(formatted.contains("\nORDER BY"), "Should break at ORDER BY");
    }

    #[test]
    fn test_sanitization_makes_sql_match() {
        let engine = TimeMachineEventValidationEngine::new();

        // SQL with different timestamps
        let incoming_args = serde_json::json!([
            "INSERT INTO t (id, ts) VALUES ('019be853-9daf-7c50-b02f-369e65f08b69', '2026-01-23T00:49:47.760170')"
        ]);
        let recorded_args = serde_json::json!([
            "INSERT INTO t (id, ts) VALUES ('019be853-9daf-7c50-b02f-369e65f08b69', '2026-01-21T01:04:12.066273')"
        ]);

        let incoming = IncomingEvent::new("model.my_project.my_model", "execute", &incoming_args);
        let recorded = make_recorded_event(incoming.node_id, "execute", recorded_args);

        let result = engine.validate(&incoming, &recorded);
        assert!(
            matches!(result, ValidationResult::Match),
            "Expected match after sanitization, got {:?}",
            result
        );
    }

    #[test]
    fn test_whitespace_diff_matches_but_preserves_formatting_in_display() {
        let engine = TimeMachineEventValidationEngine::new();

        // SQL that differs only in whitespace (like the "copy grants as (" case)
        let incoming_args = serde_json::json!(["  copy grants as ("]);
        let recorded_args = serde_json::json!(["  copy grants\n\n\n  as ("]);

        let incoming = IncomingEvent::new("model.my_project.my_model", "execute", &incoming_args);
        let recorded = make_recorded_event(incoming.node_id, "execute", recorded_args);

        // Should match because whitespace is normalized for comparison
        let result = engine.validate(&incoming, &recorded);
        assert!(
            matches!(result, ValidationResult::Match),
            "Expected match after whitespace normalization, got {:?}",
            result
        );
    }

    #[test]
    fn test_mismatch_display_preserves_formatting() {
        let engine = TimeMachineEventValidationEngine::new();

        // SQL that has actual differences (not just whitespace)
        let incoming_args = serde_json::json!(["SELECT\n    a,\n    b\nFROM t"]);
        let recorded_args = serde_json::json!(["SELECT\n    x,\n    y\nFROM t"]);

        let incoming = IncomingEvent::new("model.my_project.my_model", "execute", &incoming_args);
        let recorded = make_recorded_event(incoming.node_id, "execute", recorded_args);

        let result = engine.validate(&incoming, &recorded);
        match result {
            ValidationResult::Mismatch(mismatch) => {
                // Display should preserve newlines for readable diff
                assert!(
                    mismatch.expected.contains('\n'),
                    "Expected display SQL to preserve newlines, got: {}",
                    mismatch.expected
                );
                assert!(
                    mismatch.actual.contains('\n'),
                    "Actual display SQL to preserve newlines, got: {}",
                    mismatch.actual
                );
            }
            _ => panic!("Expected mismatch, got {:?}", result),
        }
    }
}
