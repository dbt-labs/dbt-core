use std::borrow::Cow;

pub use dbt_serde_yaml::Value as YmlValue;

// TODO(felipecrv): move this struct for generic use as it now has nothing specific to adapters

#[derive(Debug, Default)]
pub struct AdapterConfig {
    repr: dbt_serde_yaml::Mapping,
}

fn yml_value_to_string<'a>(value: &'a YmlValue) -> Cow<'a, str> {
    // This function exists because `dbt_serde_yaml::to_string` appends
    // a newline to the end of every string. And, less importantly, it
    // also copies values that are strings already.
    match value {
        YmlValue::Null(_) => Cow::Borrowed("null"),
        YmlValue::Bool(b, _) => Cow::Borrowed(if *b { "true" } else { "false" }),
        YmlValue::Number(n, _) => Cow::Owned(n.to_string()),
        YmlValue::String(s, _) => Cow::Borrowed(s),
        YmlValue::Sequence(_, _) | YmlValue::Mapping(_, _) => {
            let res = dbt_serde_yaml::to_string(value);
            debug_assert!(
                res.is_ok(),
                "failed to convert sequence/mapping to string: {res:?}",
            );
            let mut s = res.unwrap();
            if s.ends_with('\n') {
                s.pop();
            }
            Cow::Owned(s)
        }
        YmlValue::Tagged(tagged_value, _) => yml_value_to_string(&tagged_value.value),
    }
}

impl AdapterConfig {
    pub fn new(mapping: dbt_serde_yaml::Mapping) -> Self {
        Self { repr: mapping }
    }

    /// Get the underlying YAML representation of the configuration.
    pub fn repr(&self) -> &dbt_serde_yaml::Mapping {
        &self.repr
    }

    /// Checks if the config contains the given key.
    pub fn contains_key(&self, field: &str) -> bool {
        self.repr.contains_key(field)
    }

    /// Get a value from a map or return None.
    pub fn get(&self, field: &str) -> Option<&YmlValue> {
        self.repr.get(field)
    }

    /// Like `get`, but returns an error if the field is missing.
    pub fn require(&self, field: &str) -> Result<&YmlValue, dbt_serde_yaml::Error> {
        use serde::de::Error as _;
        // Re-implementation of [dbt_serde_yaml::Error::missing_field]
        // that doesn't require a &'static str field name.
        let err = || dbt_serde_yaml::Error::custom(format_args!("missing field `{field}`"));
        self.get(field).ok_or_else(err)
    }

    /// Like `get`, but calls `to_string` on the value.
    pub fn get_string(&self, field: &str) -> Option<Cow<'_, str>> {
        self.get(field).map(yml_value_to_string)
    }

    /// Like `require`, but calls `to_string` on the value.
    pub fn require_string(&self, field: &str) -> Result<Cow<'_, str>, dbt_serde_yaml::Error> {
        self.require(field).map(yml_value_to_string)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_ra3_node_config() {
        let mapping = dbt_serde_yaml::Mapping::from_iter([
            ("ra3_node_bool".into(), YmlValue::bool(true)),
            ("ra3_node_str".into(), YmlValue::string("true".to_string())),
        ]);
        let config = AdapterConfig::new(mapping);

        assert!(config.get_string("ra3_node").is_none());

        let ra3_node = config
            .get_string("ra3_node_MISSING")
            .unwrap_or(Cow::Borrowed("false"));
        let ra3_node: bool = FromStr::from_str(&ra3_node).unwrap();
        assert!(!ra3_node);

        let ra3_node = config
            .get_string("ra3_node_bool")
            .unwrap_or(Cow::Borrowed("false"));
        let ra3_node: bool = FromStr::from_str(&ra3_node).unwrap();
        assert!(ra3_node);
    }

    #[test]
    fn test_yaml_value_conversions() {
        let mapping = dbt_serde_yaml::Mapping::from_iter([
            ("null".into(), YmlValue::null()),
            ("bool".into(), YmlValue::bool(true)),
            ("i64".into(), YmlValue::number(42i64.into())),
            ("u64".into(), YmlValue::number(42u64.into())),
            ("f64".into(), YmlValue::number(42.0f64.into())),
            ("string".into(), YmlValue::string("test".to_string())),
            (
                "sequence".into(),
                YmlValue::sequence(vec!["abra".into(), "cadabra".into()]),
            ),
            (
                "mapping".into(),
                YmlValue::mapping(dbt_serde_yaml::Mapping::from_iter([
                    ("key1".into(), "value1".into()),
                    ("key2".into(), "value2".into()),
                ])),
            ),
        ]);
        let config = AdapterConfig::new(mapping);
        let get_to_string = |key: &str| config.get_string(key).unwrap();
        assert_eq!(get_to_string("null"), "null");
        assert_eq!(get_to_string("bool"), "true");
        assert_eq!(get_to_string("i64"), "42");
        assert_eq!(get_to_string("u64"), "42");
        assert_eq!(get_to_string("f64"), "42.0");
        assert_eq!(get_to_string("string"), "test");
        assert_eq!(
            get_to_string("sequence"),
            r#"- abra
- cadabra"#
        );
        assert_eq!(
            get_to_string("mapping"),
            r#"key1: value1
key2: value2"#
        );

        // check with values that came from a YAML document
        let value: YmlValue = dbt_serde_yaml::from_str(
            r#"
test:
  type: snowflake

  account: 'test_account'

  role: INTEGRATION_TEST

"#,
        )
        .unwrap();
        let test = value.get("test").unwrap();
        let ty = test.get("type").unwrap();
        assert_eq!(yml_value_to_string(ty), "snowflake");
        let account = test.get("account").unwrap();
        assert_eq!(yml_value_to_string(account), "test_account");
        let role = test.get("role").unwrap();
        assert_eq!(yml_value_to_string(role), "INTEGRATION_TEST");

        let s = yml_value_to_string(test);
        assert_eq!(
            s,
            r#"type: snowflake
account: test_account
role: INTEGRATION_TEST"#
        );
    }
}
