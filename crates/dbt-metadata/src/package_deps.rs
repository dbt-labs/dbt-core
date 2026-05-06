use crate::parquet_node::ParquetNode;
use std::collections::HashSet;

/// Extracts unique package names from a node's macro dependencies.
///
/// DBT macro unique_ids follow the pattern: "macro.package_name.macro_name"
/// This function parses the depends_on_macros field and extracts the package names.
pub fn extract_macro_packages(node: &ParquetNode) -> HashSet<String> {
    let mut packages = HashSet::new();

    for macro_id in &node.depends_on_macros {
        if let Some(package_name) = parse_package_from_macro_id(macro_id) {
            packages.insert(package_name);
        }
    }

    packages
}

/// Parses a macro unique_id to extract the package name.
///
/// Expected format: "macro.package_name.macro_name"
/// Returns None if the format is invalid.
fn parse_package_from_macro_id(macro_id: &str) -> Option<String> {
    let parts: Vec<&str> = macro_id.split('.').collect();

    // Verify format: should have at least 3 parts and start with "macro"
    if parts.len() >= 3 && parts[0] == "macro" {
        Some(parts[1].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_node::{ParquetNode, ResourceType};
    use dbt_test_primitives::assert_contains;

    #[test]
    fn test_parse_package_from_macro_id() {
        // Valid macro IDs
        assert_eq!(
            parse_package_from_macro_id("macro.dbt_utils.get_column_values"),
            Some("dbt_utils".to_string())
        );
        assert_eq!(
            parse_package_from_macro_id("macro.my_project.custom_macro"),
            Some("my_project".to_string())
        );
        assert_eq!(
            parse_package_from_macro_id("macro.dbt.run_query"),
            Some("dbt".to_string())
        );

        // Invalid formats
        assert_eq!(
            parse_package_from_macro_id("model.my_project.my_model"),
            None
        );
        assert_eq!(parse_package_from_macro_id("macro.only_two"), None);
        assert_eq!(parse_package_from_macro_id("just_one_part"), None);
        assert_eq!(parse_package_from_macro_id(""), None);
    }

    #[test]
    fn test_extract_macro_packages() {
        let node = ParquetNode {
            resource_type: ResourceType::Model,
            unique_id: "model.my_project.my_model".to_string(),
            name: "my_model".to_string(),
            depends_on_macros: vec![
                "macro.dbt_utils.get_column_values".to_string(),
                "macro.dbt_utils.generate_surrogate_key".to_string(),
                "macro.my_project.custom_macro".to_string(),
                "macro.dbt.run_query".to_string(),
                "invalid.format".to_string(), // Should be ignored
            ],
            ..Default::default()
        };

        let packages = extract_macro_packages(&node);

        assert_eq!(packages.len(), 3);
        assert_contains!(packages, "dbt_utils");
        assert_contains!(packages, "my_project");
        assert_contains!(packages, "dbt");
    }

    #[test]
    fn test_extract_macro_packages_empty() {
        let node = ParquetNode {
            resource_type: ResourceType::Model,
            unique_id: "model.my_project.my_model".to_string(),
            name: "my_model".to_string(),
            depends_on_macros: vec![],
            ..Default::default()
        };

        let packages = extract_macro_packages(&node);
        assert!(packages.is_empty());
    }

    #[test]
    fn test_extract_macro_packages_duplicates() {
        let node = ParquetNode {
            resource_type: ResourceType::Model,
            unique_id: "model.my_project.my_model".to_string(),
            name: "my_model".to_string(),
            depends_on_macros: vec![
                "macro.dbt_utils.get_column_values".to_string(),
                "macro.dbt_utils.generate_surrogate_key".to_string(),
                "macro.dbt_utils.test_equal_rowcount".to_string(),
            ],
            ..Default::default()
        };

        let packages = extract_macro_packages(&node);

        // Should only have one "dbt_utils" entry despite multiple macros from same package
        assert_eq!(packages.len(), 1);
        assert_contains!(packages, "dbt_utils");
    }
}
