use dbt_yaml::Value as YmlValue;
use indexmap::IndexMap;
use serde::Deserialize;

pub(crate) fn deserialize_databricks_tags<'de, D>(
    deserializer: D,
) -> Result<Option<IndexMap<String, YmlValue>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DatabricksTagsInput {
        Mapping(IndexMap<String, YmlValue>),
        Other(YmlValue),
    }

    match Option::<DatabricksTagsInput>::deserialize(deserializer)? {
        None => Ok(None),
        Some(DatabricksTagsInput::Mapping(tags)) => Ok(Some(tags)),
        Some(DatabricksTagsInput::Other(value)) => {
            let _ = value;
            Err(serde::de::Error::custom(
                "databricks_tags must be a dictionary",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::deserialize_databricks_tags;

    #[test]
    fn preserves_tag_order() {
        let mut deserializer =
            serde_json::Deserializer::from_str(r#"{"z_last":"first","a_first":"second"}"#);
        let tags = deserialize_databricks_tags(&mut deserializer)
            .unwrap()
            .unwrap();

        assert_eq!(
            tags.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["z_last", "a_first"]
        );
    }

    #[test]
    fn accepts_empty_and_null() {
        let mut empty_deserializer = serde_json::Deserializer::from_str("{}");
        let empty = deserialize_databricks_tags(&mut empty_deserializer)
            .unwrap()
            .unwrap();
        assert!(empty.is_empty());

        let mut null_deserializer = serde_json::Deserializer::from_str("null");
        assert!(
            deserialize_databricks_tags(&mut null_deserializer)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_non_mapping() {
        let mut deserializer = serde_json::Deserializer::from_str(r#"["not","a","mapping"]"#);
        let error = deserialize_databricks_tags(&mut deserializer).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("databricks_tags must be a dictionary")
        );
    }
}
