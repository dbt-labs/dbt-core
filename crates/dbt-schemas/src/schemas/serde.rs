use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize};
use serde::{
    self, Deserialize, Deserializer, Serialize,
    de::{self, DeserializeOwned},
};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

pub fn default_type<'de, D>(deserializer: D) -> Result<Option<BTreeMap<String, YmlValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Object(map) => Ok(Some(
            map.into_iter()
                .map(|(k, v)| {
                    // Convert serde_json::Value to dbt_serde_yaml::Value
                    let yml_val = serde_json::from_value::<YmlValue>(v).unwrap_or(YmlValue::null());
                    (k, yml_val)
                })
                .collect(),
        )),
        serde_json::Value::Null => Ok(None),
        _ => Err(de::Error::custom("expected an object or null")),
    }
}

/// Deserialize a string or an array of strings into a vector of strings
pub fn string_or_array<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Array(arr) => Ok(Some(
            arr.iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect(),
        )),
        serde_json::Value::String(s) => Ok(Some(vec![s])),
        serde_json::Value::Null => Ok(None),
        _ => Err(de::Error::custom(
            "expected a string, an array of strings, or null",
        )),
    }
}

pub fn bool_or_string_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s.to_lowercase() == "true")))
}

pub fn bool_or_string_bool_default<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s.to_lowercase() == "true"))
        .unwrap_or_default())
}

pub fn u64_or_string_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok())))
}

pub fn default_true() -> Option<bool> {
    Some(true)
}

pub fn try_from_value<T: DeserializeOwned>(
    value: Option<serde_json::Value>,
) -> Result<Option<T>, Box<dyn std::error::Error>> {
    if let Some(value) = value {
        Ok(Some(
            serde_json::from_value(value).map_err(|e| format!("Error parsing value: {e}"))?,
        ))
    } else {
        Ok(None)
    }
}

/// Convert YmlValue to a BTreeMap for minijinja
pub fn yml_value_to_minijinja_map(value: YmlValue) -> BTreeMap<String, minijinja::Value> {
    match value {
        YmlValue::Mapping(map, _) => {
            let mut result = BTreeMap::new();
            for (k, v) in map {
                if let YmlValue::String(key, _) = k {
                    result.insert(key, yml_value_to_minijinja(v));
                }
            }
            result
        }
        _ => BTreeMap::new(),
    }
}

/// Convert YmlValue to serde_json::Value
pub fn yml_to_json_value(value: &YmlValue) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

/// Convert YmlValue to String
pub fn yml_value_to_string(value: &YmlValue) -> Option<String> {
    match value {
        YmlValue::String(s, _) => Some(s.clone()),
        YmlValue::Number(n, _) => Some(n.to_string()),
        YmlValue::Bool(b, _) => Some(b.to_string()),
        YmlValue::Null(_) => Some("null".to_string()),
        _ => None,
    }
}

/// Convert YmlValue to minijinja::Value
pub fn yml_value_to_minijinja(value: YmlValue) -> minijinja::Value {
    match value {
        YmlValue::Null(_) => minijinja::Value::from(()),
        YmlValue::Bool(b, _) => minijinja::Value::from(b),
        YmlValue::String(s, _) => minijinja::Value::from(s),
        YmlValue::Number(n, _) => {
            if let Some(i) = n.as_i64() {
                minijinja::Value::from(i)
            } else if let Some(f) = n.as_f64() {
                minijinja::Value::from(f)
            } else {
                minijinja::Value::from(n.to_string())
            }
        }
        YmlValue::Sequence(seq, _) => {
            let items: Vec<minijinja::Value> =
                seq.into_iter().map(yml_value_to_minijinja).collect();
            minijinja::Value::from(items)
        }
        YmlValue::Mapping(map, _) => {
            let mut result = BTreeMap::new();
            for (k, v) in map {
                if let YmlValue::String(key, _) = k {
                    result.insert(key, yml_value_to_minijinja(v));
                }
            }
            minijinja::Value::from_object(result)
        }
        YmlValue::Tagged(tagged, _) => {
            // For tagged values, convert the inner value
            yml_value_to_minijinja(tagged.value)
        }
    }
}

pub fn try_string_to_type<T: DeserializeOwned>(
    value: &Option<String>,
) -> Result<Option<T>, Box<dyn std::error::Error>> {
    if let Some(value) = value {
        Ok(Some(
            serde_json::from_str(&format!("\"{value}\""))
                .map_err(|e| format!("Error parsing from_str '{value}': {e}"))?,
        ))
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum StringOrInteger {
    String(String),
    Integer(i64),
}

impl Default for StringOrInteger {
    fn default() -> Self {
        StringOrInteger::String("".to_string())
    }
}
impl FromStr for StringOrInteger {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            _ if s.parse::<i64>().is_ok() => Ok(StringOrInteger::Integer(s.parse().unwrap())),
            _ => Ok(StringOrInteger::String(s.to_string())),
        }
    }
}

impl std::fmt::Display for StringOrInteger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StringOrInteger::String(s) => write!(f, "{s}"),
            StringOrInteger::Integer(i) => write!(f, "{i}"),
        }
    }
}

impl From<String> for StringOrInteger {
    fn from(value: String) -> Self {
        if let Ok(i) = value.parse::<i64>() {
            StringOrInteger::Integer(i)
        } else {
            StringOrInteger::String(value)
        }
    }
}

impl StringOrInteger {
    pub fn to_i64(&self) -> i64 {
        match self {
            StringOrInteger::String(value) => {
                if let Ok(i) = value.parse::<i64>() {
                    i
                } else {
                    panic!("")
                }
            }
            StringOrInteger::Integer(i) => *i,
        }
    }
}

#[derive(Debug, Serialize, UntaggedEnumDeserialize, Clone, PartialEq, JsonSchema)]
#[serde(untagged)]
pub enum StringOrMap {
    StringValue(String),
    MapValue(HashMap<String, YmlValue>),
}

#[derive(Serialize, UntaggedEnumDeserialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum StringOrArrayOfStrings {
    String(String),
    ArrayOfStrings(Vec<String>),
}

impl From<StringOrArrayOfStrings> for Vec<String> {
    fn from(value: StringOrArrayOfStrings) -> Self {
        match value {
            StringOrArrayOfStrings::String(s) => vec![s],
            StringOrArrayOfStrings::ArrayOfStrings(a) => a,
        }
    }
}
impl StringOrArrayOfStrings {
    pub fn to_strings(&self) -> Vec<String> {
        match self {
            StringOrArrayOfStrings::String(s) => vec![s.clone()],
            StringOrArrayOfStrings::ArrayOfStrings(a) => a.clone(),
        }
    }
}

impl PartialEq for StringOrArrayOfStrings {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StringOrArrayOfStrings::String(s1), StringOrArrayOfStrings::String(s2)) => s1 == s2,
            (
                StringOrArrayOfStrings::ArrayOfStrings(a1),
                StringOrArrayOfStrings::ArrayOfStrings(a2),
            ) => a1 == a2,
            (StringOrArrayOfStrings::String(s), StringOrArrayOfStrings::ArrayOfStrings(a)) => {
                if a.len() == 1 { a[0] == *s } else { false }
            }
            (StringOrArrayOfStrings::ArrayOfStrings(a), StringOrArrayOfStrings::String(s)) => {
                if a.len() == 1 { a[0] == *s } else { false }
            }
        }
    }
}

impl Eq for StringOrArrayOfStrings {}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum SpannedStringOrArrayOfStrings {
    String(Spanned<String>),
    ArrayOfStrings(Vec<Spanned<String>>),
}

impl From<SpannedStringOrArrayOfStrings> for Vec<Spanned<String>> {
    fn from(value: SpannedStringOrArrayOfStrings) -> Self {
        match value {
            SpannedStringOrArrayOfStrings::String(s) => vec![s],
            SpannedStringOrArrayOfStrings::ArrayOfStrings(a) => a,
        }
    }
}

impl SpannedStringOrArrayOfStrings {
    pub fn to_strings(&self) -> Vec<Spanned<String>> {
        match self {
            SpannedStringOrArrayOfStrings::String(s) => vec![s.clone()],
            SpannedStringOrArrayOfStrings::ArrayOfStrings(a) => a.clone(),
        }
    }
}

impl PartialEq for SpannedStringOrArrayOfStrings {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                SpannedStringOrArrayOfStrings::String(s1),
                SpannedStringOrArrayOfStrings::String(s2),
            ) => s1 == s2,
            (
                SpannedStringOrArrayOfStrings::ArrayOfStrings(a1),
                SpannedStringOrArrayOfStrings::ArrayOfStrings(a2),
            ) => a1 == a2,
            (
                SpannedStringOrArrayOfStrings::String(s),
                SpannedStringOrArrayOfStrings::ArrayOfStrings(a),
            ) => {
                if a.len() == 1 {
                    a[0] == *s
                } else {
                    false
                }
            }
            (
                SpannedStringOrArrayOfStrings::ArrayOfStrings(a),
                SpannedStringOrArrayOfStrings::String(s),
            ) => {
                if a.len() == 1 {
                    a[0] == *s
                } else {
                    false
                }
            }
        }
    }
}

impl Eq for SpannedStringOrArrayOfStrings {}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum FloatOrString {
    Number(f32),
    String(String),
}

impl std::fmt::Display for FloatOrString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FloatOrString::Number(n) => write!(f, "{n}"),
            FloatOrString::String(s) => write!(f, "{s}"),
        }
    }
}
