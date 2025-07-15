use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use dbt_serde_yaml::JsonSchema;
use serde::{
    self,
    de::{self, DeserializeOwned},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;

pub fn default_type<'de, D>(deserializer: D) -> Result<Option<BTreeMap<String, Value>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Object(map) => Ok(Some(map.into_iter().collect())),
        Value::Null => Ok(None),
        _ => Err(de::Error::custom("expected an object or null")),
    }
}

/// Deserialize a string or an array of strings into a vector of strings
pub fn string_or_array<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Array(arr) => Ok(Some(
            arr.iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect(),
        )),
        Value::String(s) => Ok(Some(vec![s])),
        Value::Null => Ok(None),
        _ => Err(de::Error::custom(
            "expected a string, an array of strings, or null",
        )),
    }
}

pub fn bool_or_string_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s.to_lowercase() == "true")))
}

pub fn bool_or_string_bool_default<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s.to_lowercase() == "true"))
        .unwrap_or_default())
}

pub fn u64_or_string_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok())))
}

pub fn default_true() -> Option<bool> {
    Some(true)
}

pub fn try_from_value<T: DeserializeOwned>(
    value: Option<Value>,
) -> Result<Option<T>, Box<dyn std::error::Error>> {
    if let Some(value) = value {
        Ok(Some(
            serde_json::from_value(value).map_err(|e| format!("Error parsing value: {e}"))?,
        ))
    } else {
        Ok(None)
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
#[serde(untagged)]
pub enum StringOrMap {
    StringValue(String),
    MapValue(HashMap<String, Value>),
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
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
                if a.len() == 1 {
                    a[0] == *s
                } else {
                    false
                }
            }
            (StringOrArrayOfStrings::ArrayOfStrings(a), StringOrArrayOfStrings::String(s)) => {
                if a.len() == 1 {
                    a[0] == *s
                } else {
                    false
                }
            }
        }
    }
}

impl Eq for StringOrArrayOfStrings {}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
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
