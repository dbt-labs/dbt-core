use dbt_common::{ErrorCode, FsResult, fs_err};
use regex::Regex;

/// Validates a metric name according to the rules ported from Python dbt:
/// - Cannot contain spaces
/// - Cannot be longer than 250 characters
/// - Must begin with a letter
/// - Must contain only letters, numbers, and underscores
///
/// # Arguments
/// * `name` - The metric name to validate
///
/// # Returns
/// * `Ok(())` if the name is valid
/// * `Err(FsError)` with SchemaError code if the name is invalid
///
/// # Example
/// ```
/// use validate_metrics::validate_metric_name;
///
/// assert!(validate_metric_name("valid_metric_name").is_ok());
/// assert!(validate_metric_name("invalid name").is_err()); // contains spaces
/// ```
pub fn validate_metric_name(name: &str) -> FsResult<()> {
    let mut errors = Vec::new();

    // Check for spaces
    if name.contains(' ') {
        errors.push("cannot contain spaces");
    }

    // Check length (BigQuery and Snowflake limitation)
    if name.len() > 250 {
        errors.push("cannot contain more than 250 characters");
    }

    // Check if it starts with a letter
    let starts_with_letter = Regex::new(r"^[A-Za-z]").expect("valid regex");
    if !starts_with_letter.is_match(name) {
        errors.push("must begin with a letter");
    }

    // Check if it contains only valid characters (letters, numbers, underscores)
    let valid_chars = Regex::new(r"^[\w]+$").expect("valid regex");
    if !valid_chars.is_match(name) {
        errors.push("must contain only letters, numbers and underscores");
    }

    if !errors.is_empty() {
        return Err(fs_err!(
            ErrorCode::SchemaError,
            "The metric name '{}' is invalid. It {}",
            name,
            errors.join(", ")
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_metric_names() {
        assert!(validate_metric_name("valid_metric").is_ok());
        assert!(validate_metric_name("ValidMetric").is_ok());
        assert!(validate_metric_name("metric123").is_ok());
        assert!(validate_metric_name("metric_with_underscores").is_ok());
        assert!(validate_metric_name("a").is_ok());
        assert!(validate_metric_name("A").is_ok());
    }

    #[test]
    fn test_invalid_metric_names_with_spaces() {
        let result = validate_metric_name("invalid name");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        assert!(error.context.contains("cannot contain spaces"));
    }

    #[test]
    fn test_invalid_metric_names_too_long() {
        let long_name = "a".repeat(251);
        let result = validate_metric_name(&long_name);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        assert!(
            error
                .context
                .contains("cannot contain more than 250 characters")
        );
    }

    #[test]
    fn test_invalid_metric_names_not_starting_with_letter() {
        let result = validate_metric_name("123metric");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        assert!(error.context.contains("must begin with a letter"));
    }

    #[test]
    fn test_invalid_metric_names_invalid_chars() {
        let result = validate_metric_name("metric@name");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        assert!(
            error
                .context
                .contains("must contain only letters, numbers and underscores")
        );
    }

    #[test]
    fn test_invalid_metric_names_with_hyphens() {
        let result = validate_metric_name("metric-with-hyphens");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        assert!(
            error
                .context
                .contains("must contain only letters, numbers and underscores")
        );
    }

    #[test]
    fn test_multiple_validation_errors() {
        let result = validate_metric_name("123 invalid@name");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        let context = &error.context;
        assert!(context.contains("cannot contain spaces"));
        assert!(context.contains("must begin with a letter"));
        assert!(context.contains("must contain only letters, numbers and underscores"));
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::collections::HashSet;

    /// Simulates how duplicate checking works in the resolve functions
    fn check_duplicate_names(names: &[&str]) -> FsResult<()> {
        let mut seen_names = HashSet::new();
        for &name in names {
            // Validate individual name first
            validate_metric_name(name)?;

            // Check for duplicates
            if !seen_names.insert(name.to_string()) {
                return Err(fs_err!(
                    ErrorCode::SchemaError,
                    "Duplicate metric name '{}' found in package",
                    name
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn test_no_duplicate_names() {
        let names = vec!["metric_one", "metric_two", "metric_three"];
        assert!(check_duplicate_names(&names).is_ok());
    }

    #[test]
    fn test_duplicate_names_detected() {
        let names = vec!["metric_one", "metric_two", "metric_one"];
        let result = check_duplicate_names(&names);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        assert!(error.context.contains("Duplicate metric name 'metric_one'"));
    }

    #[test]
    fn test_invalid_name_caught_before_duplicate_check() {
        let names = vec!["valid_metric", "invalid name", "invalid name"];
        let result = check_duplicate_names(&names);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::SchemaError);
        // Should fail on the individual name validation, not duplicate
        assert!(error.context.contains("invalid name"));
        assert!(error.context.contains("cannot contain spaces"));
    }
}
