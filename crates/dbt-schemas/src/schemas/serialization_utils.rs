/// Utilities for handling different serialization modes (Artifact vs Jinja)
use serde::Serialize;

type YmlValue = dbt_serde_yaml::Value;

/// Serialization mode determines how Option fields are handled
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationMode {
    /// Strip None/null fields to reduce size (for manifests, JSON output)
    OmitNone,
    /// Keep None fields as null for accessibility (for list command, Jinja context)
    KeepNone,
}

/// Serialize a value with the specified mode
///
/// Note: This assumes config structs do NOT have #[skip_serializing_none],
/// so None values are serialized as null by default.
pub fn serialize_with_mode<T: Serialize>(value: &T, mode: SerializationMode) -> YmlValue {
    let yml_value = dbt_serde_yaml::to_value(value).expect("Failed to serialize to YAML");

    match mode {
        SerializationMode::KeepNone => yml_value, // Keep everything including nulls
        SerializationMode::OmitNone => strip_null_fields(yml_value), // Remove nulls to save space
    }
}

/// Recursively strips null values from a YmlValue to reduce artifact size
fn strip_null_fields(value: YmlValue) -> YmlValue {
    match value {
        YmlValue::Mapping(map, span) => {
            let filtered_map: dbt_serde_yaml::Mapping = map
                .into_iter()
                .filter_map(|(k, v)| {
                    // Skip null values
                    if matches!(v, YmlValue::Null(_)) {
                        None
                    } else {
                        // Recursively process non-null values
                        Some((k, strip_null_fields(v)))
                    }
                })
                .collect();
            YmlValue::Mapping(filtered_map, span)
        }
        YmlValue::Sequence(seq, span) => {
            let processed_seq = seq.into_iter().map(strip_null_fields).collect();
            YmlValue::Sequence(processed_seq, span)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    // NOTE: No #[skip_serializing_none] - we want None to serialize as null
    #[derive(Serialize)]
    struct TestConfig {
        pub full_refresh: Option<bool>,
        pub enabled: Option<bool>,
        pub required_field: String,
    }

    #[derive(Serialize)]
    struct TestNode {
        pub name: String,
        #[serde(rename = "config")]
        pub deprecated_config: TestConfig,
    }

    #[test]
    fn test_omit_none_mode_strips_null() {
        let node = TestNode {
            name: "test".to_string(),
            deprecated_config: TestConfig {
                full_refresh: None,
                enabled: Some(true),
                required_field: "value".to_string(),
            },
        };

        let result = serialize_with_mode(&node, SerializationMode::OmitNone);

        // In OmitNone mode, null fields should be stripped
        if let YmlValue::Mapping(map, _) = result {
            if let Some(YmlValue::Mapping(config_map, _)) =
                map.get(YmlValue::string("config".to_string()))
            {
                // full_refresh should be missing (stripped)
                assert!(!config_map.contains_key(YmlValue::string("full_refresh".to_string())));
                // enabled should be present
                assert!(config_map.contains_key(YmlValue::string("enabled".to_string())));
            }
        }
    }

    #[test]
    fn test_keep_none_mode_keeps_null() {
        let node = TestNode {
            name: "test".to_string(),
            deprecated_config: TestConfig {
                full_refresh: None,
                enabled: Some(true),
                required_field: "value".to_string(),
            },
        };

        let result = serialize_with_mode(&node, SerializationMode::KeepNone);

        // In KeepNone mode, null fields should be kept
        if let YmlValue::Mapping(map, _) = result {
            if let Some(YmlValue::Mapping(config_map, _)) =
                map.get(YmlValue::string("config".to_string()))
            {
                // full_refresh should be present as null
                assert!(config_map.contains_key(YmlValue::string("full_refresh".to_string())));
                // Check that it's Null
                assert!(matches!(
                    config_map.get(YmlValue::string("full_refresh".to_string())),
                    Some(YmlValue::Null(_))
                ));
                // enabled should be present
                assert!(config_map.contains_key(YmlValue::string("enabled".to_string())));
            }
        }
    }
}
