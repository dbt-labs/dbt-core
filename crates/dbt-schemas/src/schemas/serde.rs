use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::str::FromStr;

use dbt_common::{CodeLocation, ErrorCode, FsError, FsResult, stdfs};
use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize};
use serde::{
    self, Deserialize, Deserializer, Serialize,
    de::{self, DeserializeOwned},
};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
type MinijinjaValue = minijinja::Value;

/// Deserializes a JSON file into a `T`, using the file's absolute path for error reporting.
pub fn typed_struct_from_json_file<T>(path: &Path) -> FsResult<T>
where
    T: DeserializeOwned,
{
    // Note: Do **NOT** open the file and parse as JSON directly using
    // `serde_json::from_reader`! That will be ~30x slower.
    let json_str = stdfs::read_to_string(path)?;

    typed_struct_from_json_str(&json_str, Some(path))
}

pub fn typed_struct_to_pretty_json_file<T>(path: &Path, value: &T) -> FsResult<()>
where
    T: Serialize,
{
    let yml_val = dbt_serde_yaml::to_value(value).map_err(|e| {
        FsError::new(
            ErrorCode::SerializationError,
            format!("Failed to convert to YAML: {e}"),
        )
    })?;
    let file = std::fs::File::create(path).map_err(|e| {
        FsError::new(
            ErrorCode::SerializationError,
            format!("Failed to create file: {e}"),
        )
    })?;
    serde_json::to_writer_pretty(file, &yml_val).map_err(|e| {
        FsError::new(
            ErrorCode::SerializationError,
            format!("Failed to write to file: {e}"),
        )
    })?;
    Ok(())
}

/// Deserializes a JSON string into a `T`.
pub fn typed_struct_from_json_str<T>(json_str: &str, source: Option<&Path>) -> FsResult<T>
where
    T: DeserializeOwned,
{
    let yml_val: YmlValue = serde_json::from_str(json_str).map_err(|e| {
        FsError::new(
            ErrorCode::SerializationError,
            format!("Failed to parse JSON: {e}"),
        )
    })?;

    T::deserialize(yml_val).map_err(|e| yaml_to_fs_error(e, source))
}

/// Converts a `dbt_serde_yaml::Error` into a `FsError`, attaching the error location
pub fn yaml_to_fs_error(err: dbt_serde_yaml::Error, filename: Option<&Path>) -> Box<FsError> {
    let msg = err.display_no_mark().to_string();
    let location = err
        .span()
        .map_or_else(CodeLocation::default, CodeLocation::from);
    let location = if let Some(filename) = filename {
        location.with_file(filename)
    } else {
        location
    };

    if let Some(err) = err.into_external() {
        if let Ok(err) = err.downcast::<FsError>() {
            // These are errors raised from our own callbacks:
            return err;
        }
    }
    FsError::new(ErrorCode::SerializationError, format!("YAML error: {msg}"))
        .with_location(location)
        .into()
}

pub fn default_type<'de, D>(deserializer: D) -> Result<Option<BTreeMap<String, YmlValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = dbt_serde_yaml::Value::deserialize(deserializer)?;
    match value {
        dbt_serde_yaml::Value::Mapping(map, _) => Ok(Some(
            map.into_iter()
                .map(|(k, v)| {
                    let yml_val =
                        dbt_serde_yaml::from_value::<YmlValue>(v).unwrap_or(YmlValue::null());
                    (
                        k.as_str().expect("key is not a string").to_string(),
                        yml_val,
                    )
                })
                .collect(),
        )),
        dbt_serde_yaml::Value::Null(_) => Ok(None),
        _ => Err(de::Error::custom("expected an object or null")),
    }
}

/// Deserialize a string or an array of strings into a vector of strings
pub fn string_or_array<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = dbt_serde_yaml::Value::deserialize(deserializer)?;
    match value {
        dbt_serde_yaml::Value::Sequence(arr, _) => Ok(Some(
            arr.iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect(),
        )),
        dbt_serde_yaml::Value::String(s, _) => Ok(Some(vec![s])),
        dbt_serde_yaml::Value::Null(_) => Ok(None),
        _ => Err(de::Error::custom(
            "expected a string, an array of strings, or null",
        )),
    }
}

pub fn bool_or_string_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = dbt_serde_yaml::Value::deserialize(deserializer)?;
    Ok(value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s.to_lowercase() == "true")))
}

pub fn bool_or_string_bool_default<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = dbt_serde_yaml::Value::deserialize(deserializer)?;
    Ok(value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s.to_lowercase() == "true"))
        .unwrap_or_default())
}

pub fn u64_or_string_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = dbt_serde_yaml::Value::deserialize(deserializer)?;
    Ok(value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok())))
}

pub fn default_true() -> Option<bool> {
    Some(true)
}

pub fn try_from_value<T: DeserializeOwned>(
    value: Option<YmlValue>,
) -> Result<Option<T>, Box<dyn std::error::Error>> {
    if let Some(value) = value {
        Ok(Some(
            dbt_serde_yaml::from_value(value).map_err(|e| format!("Error parsing value: {e}"))?,
        ))
    } else {
        Ok(None)
    }
}

/// Convert YmlValue to a BTreeMap for minijinja
pub fn yml_value_to_minijinja_map(value: YmlValue) -> BTreeMap<String, MinijinjaValue> {
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

pub fn minijinja_value_to_typed_struct<T: DeserializeOwned>(value: MinijinjaValue) -> FsResult<T> {
    let yml_val = dbt_serde_yaml::to_value(value).map_err(|e| {
        FsError::new(
            ErrorCode::SerializationError,
            format!("Failed to convert MinijinjaValue to YmlValue: {e}"),
        )
    })?;

    T::deserialize(yml_val).map_err(|e| yaml_to_fs_error(e, None))
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
pub fn yml_value_to_minijinja(value: YmlValue) -> MinijinjaValue {
    match value {
        YmlValue::Null(_) => MinijinjaValue::from(()),
        YmlValue::Bool(b, _) => MinijinjaValue::from(b),
        YmlValue::String(s, _) => MinijinjaValue::from(s),
        YmlValue::Number(n, _) => {
            if let Some(i) = n.as_i64() {
                MinijinjaValue::from(i)
            } else if let Some(f) = n.as_f64() {
                MinijinjaValue::from(f)
            } else {
                MinijinjaValue::from(n.to_string())
            }
        }
        YmlValue::Sequence(seq, _) => {
            let items: Vec<MinijinjaValue> = seq.into_iter().map(yml_value_to_minijinja).collect();
            MinijinjaValue::from(items)
        }
        YmlValue::Mapping(map, _) => {
            let mut result = BTreeMap::new();
            for (k, v) in map {
                if let YmlValue::String(key, _) = k {
                    result.insert(key, yml_value_to_minijinja(v));
                }
            }
            MinijinjaValue::from_object(result)
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
            dbt_serde_yaml::from_str(&format!("\"{value}\""))
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
