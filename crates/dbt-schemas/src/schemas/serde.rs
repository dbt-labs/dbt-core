use crate::schemas::manifest::postgres::PostgresIndex;
use dbt_common::serde_utils::Omissible;
use dbt_common::{CodeLocationWithFile, ErrorCode, FsError, FsResult, stdfs};
use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize};
use indexmap::IndexMap;
use serde::{
    self, Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned},
};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
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
        .map_or_else(CodeLocationWithFile::default, CodeLocationWithFile::from);
    let location = if let Some(filename) = filename {
        location.with_file(Arc::new(filename.into()))
    } else {
        location
    };

    if let Some(err) = err.into_external()
        && let Ok(err) = err.downcast::<FsError>()
    {
        // These are errors raised from our own callbacks:
        return err;
    }
    FsError::new(ErrorCode::SerializationError, format!("YAML error: {msg}"))
        .with_location(location)
        .into()
}

pub fn default_type<'de, D>(deserializer: D) -> Result<Option<IndexMap<String, YmlValue>>, D::Error>
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

pub fn f64_or_string_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = dbt_serde_yaml::Value::deserialize(deserializer)?;
    Ok(value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok())))
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
        YmlValue::Null(_) => MinijinjaValue::from(None::<()>),
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

/// Wrapper that serializes `StringOrArrayOfStrings` as an array without allocation.
struct AsArray<'a>(&'a StringOrArrayOfStrings);

impl Serialize for AsArray<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;

        match self.0 {
            StringOrArrayOfStrings::ArrayOfStrings(vec) => vec.serialize(serializer),
            StringOrArrayOfStrings::String(s) => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                seq.serialize_element(s)?;
                seq.end()
            }
        }
    }
}

/// Wrapper type for grant configurations that normalizes values to arrays during serialization.
///
/// This type handles grant configurations with the following behavior:
/// - Normalizes string values to arrays during serialization
/// - Preserves insertion order using IndexMap
#[derive(Debug, Clone, PartialEq, Eq, Default, JsonSchema)]
pub struct GrantConfig(pub IndexMap<String, StringOrArrayOfStrings>);

/// Wrapper around `Omissible<GrantConfig>` with Jinja-compatible serialization.
///
/// Serializes `Omitted` as `{}` instead of `null` for dbt-core compat
/// All other behavior delegates to `Omissible<GrantConfig>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OmissibleGrantConfig(pub Omissible<GrantConfig>);

impl Default for OmissibleGrantConfig {
    fn default() -> Self {
        OmissibleGrantConfig(Omissible::Omitted)
    }
}

impl<'de> Deserialize<'de> for OmissibleGrantConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use dbt_common::serde_utils::Omissible;
        Omissible::<GrantConfig>::deserialize(deserializer).map(OmissibleGrantConfig)
    }
}

impl Serialize for OmissibleGrantConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use dbt_common::serde_utils::Omissible;
        match &self.0 {
            Omissible::Present(grant_config) => grant_config.serialize(serializer),
            // Omitted -> {} instead of null (for Jinja compatibility)
            Omissible::Omitted => GrantConfig::default().serialize(serializer),
        }
    }
}

impl OmissibleGrantConfig {
    pub fn is_omitted(&self) -> bool {
        self.0.is_omitted()
    }

    pub fn as_ref(&self) -> Option<&GrantConfig> {
        self.0.as_ref()
    }

    pub fn as_mut(&mut self) -> Option<&mut GrantConfig> {
        self.0.as_mut()
    }
}

impl schemars::JsonSchema for OmissibleGrantConfig {
    fn schema_name() -> String {
        GrantConfig::schema_name()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        GrantConfig::json_schema(generator)
    }

    fn is_referenceable() -> bool {
        GrantConfig::is_referenceable()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        GrantConfig::schema_id()
    }

    #[doc(hidden)]
    fn _schemars_private_non_optional_json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        GrantConfig::_schemars_private_non_optional_json_schema(generator)
    }

    #[doc(hidden)]
    fn _schemars_private_is_option() -> bool {
        true
    }
}

impl<'de> Deserialize<'de> for GrantConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = IndexMap::<String, StringOrArrayOfStrings>::deserialize(deserializer)?;
        Ok(GrantConfig(map))
    }
}

impl Serialize for GrantConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        // Serialize to {} by default for dbt-core Compat. Used in things like iterators.
        let mut map_ser = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map_ser.serialize_entry(k, &AsArray(v))?;
        }
        map_ser.end()
    }
}

impl GrantConfig {
    /// Get a reference to the value associated with a key
    pub fn get(&self, key: &str) -> Option<&StringOrArrayOfStrings> {
        self.0.get(key)
    }

    /// Check if a key exists in the grants
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
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

// =============================================================================
// PrimaryKeyConfig - Wrapper type for primary_key that normalizes to arrays
// =============================================================================
//
// This type implements sadboy's preferred approach:
// - Encodes the serialization/normalization rules in the Rust type itself
// - Accepts both string and array inputs (e.g., "id" or ["id", "tenant_id"])
// - Always serializes values as arrays (matching dbt-core's `listify` behavior)
// - Generates correct JSON schema automatically

/// A wrapper type for the `primary_key` config field that normalizes serialization.
///
/// In dbt-core, primary_key values are "listified" - single strings are converted
/// to single-element arrays. This type accepts either format on input but always
/// serializes as arrays.
///
/// # Example
///
/// ```ignore
/// // Input: "id"
/// // Serializes as: ["id"]
///
/// // Input: ["id", "tenant_id"]
/// // Serializes as: ["id", "tenant_id"]
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
pub struct PrimaryKeyConfig(Option<StringOrArrayOfStrings>);

impl PrimaryKeyConfig {
    /// Creates a new empty PrimaryKeyConfig
    pub fn new() -> Self {
        Self(None)
    }

    /// Creates a PrimaryKeyConfig from a StringOrArrayOfStrings
    pub fn from_value(value: StringOrArrayOfStrings) -> Self {
        Self(Some(value))
    }

    /// Consumes self and returns the inner value
    pub fn into_inner(self) -> Option<StringOrArrayOfStrings> {
        self.0
    }

    /// Returns true if the primary key is empty or unset
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    /// Returns true if the primary key is set
    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    /// Gets the primary key values as a Vec<String>
    pub fn to_strings(&self) -> Option<Vec<String>> {
        self.0.as_ref().map(|v| v.to_strings())
    }
}

impl AsRef<Option<StringOrArrayOfStrings>> for PrimaryKeyConfig {
    fn as_ref(&self) -> &Option<StringOrArrayOfStrings> {
        &self.0
    }
}

impl Serialize for PrimaryKeyConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.0 {
            Some(value) => {
                // Always serialize as array (the "listify" behavior)
                AsArray(value).serialize(serializer)
            }
            None => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for PrimaryKeyConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<StringOrArrayOfStrings>::deserialize(deserializer)?;
        Ok(PrimaryKeyConfig(value))
    }
}

impl From<Option<StringOrArrayOfStrings>> for PrimaryKeyConfig {
    fn from(value: Option<StringOrArrayOfStrings>) -> Self {
        PrimaryKeyConfig(value)
    }
}

impl From<PrimaryKeyConfig> for Option<StringOrArrayOfStrings> {
    fn from(config: PrimaryKeyConfig) -> Self {
        config.0
    }
}

// =============================================================================
// IndexesConfig - Wrapper type for indexes that accepts list or dict formats
// =============================================================================
//
// This type implements sadboy's preferred approach:
// - Encodes the deserialization rules in the Rust type itself
// - Accepts both list format `[{...}]` and dictionary format `{'name': {...}}`
// - Always serializes as a list
// - Generates correct JSON schema automatically

/// A wrapper type for the `indexes` config field that handles flexible deserialization.
///
/// dbt-core accepts both list and dictionary formats for indexes. This type accepts
/// either format on input but always serializes as a list.
///
/// # Example
///
/// ```ignore
/// // Input (list): [{columns: ["id"], unique: true}]
/// // Serializes as: [{columns: ["id"], unique: true}]
///
/// // Input (dict): {"my_index": {columns: ["id"], unique: true}}
/// // Serializes as: [{columns: ["id"], unique: true}]  (keys are discarded)
/// ```
#[derive(Debug, Clone, Default, PartialEq, JsonSchema)]
pub struct IndexesConfig(Option<Vec<PostgresIndex>>);

impl IndexesConfig {
    /// Creates a new empty IndexesConfig
    pub fn new() -> Self {
        Self(None)
    }

    /// Creates an IndexesConfig from a Vec of PostgresIndex
    pub fn from_vec(indexes: Vec<PostgresIndex>) -> Self {
        Self(Some(indexes))
    }

    /// Consumes self and returns the inner value
    pub fn into_inner(self) -> Option<Vec<PostgresIndex>> {
        self.0
    }

    /// Returns true if the indexes are empty or unset
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    /// Returns true if the indexes are set
    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    /// Returns true if the indexes are empty
    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_none_or(|v| v.is_empty())
    }

    /// Returns the number of indexes
    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |v| v.len())
    }
}

impl AsRef<Option<Vec<PostgresIndex>>> for IndexesConfig {
    fn as_ref(&self) -> &Option<Vec<PostgresIndex>> {
        &self.0
    }
}

impl Serialize for IndexesConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Always serialize as Option<Vec<PostgresIndex>>
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for IndexesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{MapAccess, SeqAccess, Visitor};
        use std::fmt;
        use std::marker::PhantomData;

        struct IndexesVisitor(PhantomData<PostgresIndex>);

        impl<'de> Visitor<'de> for IndexesVisitor {
            type Value = Option<Vec<PostgresIndex>>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "a sequence of PostgresIndex, a map of name -> PostgresIndex, or null",
                )
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(None)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(None)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(elem) = seq.next_element()? {
                    vec.push(elem);
                }
                Ok(Some(vec))
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut vec = Vec::new();
                // Discard the keys, collect just the values
                while let Some((_key, value)) = map.next_entry::<String, PostgresIndex>()? {
                    vec.push(value);
                }
                Ok(Some(vec))
            }
        }

        deserializer
            .deserialize_any(IndexesVisitor(PhantomData))
            .map(IndexesConfig)
    }
}

impl From<Option<Vec<PostgresIndex>>> for IndexesConfig {
    fn from(value: Option<Vec<PostgresIndex>>) -> Self {
        IndexesConfig(value)
    }
}

impl From<IndexesConfig> for Option<Vec<PostgresIndex>> {
    fn from(config: IndexesConfig) -> Self {
        config.0
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::serde_utils::Omissible;

    #[derive(Serialize, Deserialize)]
    struct TestConfig {
        #[serde(default)]
        grants: OmissibleGrantConfig,
    }

    #[test]
    fn test_grant_config_normalizes_string_to_array() {
        let mut grants: IndexMap<String, StringOrArrayOfStrings> = IndexMap::new();
        grants.insert(
            "select".to_string(),
            StringOrArrayOfStrings::String("ROLE_A".to_string()),
        );

        let config = TestConfig {
            grants: OmissibleGrantConfig(Omissible::Present(GrantConfig(grants))),
        };
        let json = serde_json::to_string(&config).unwrap();

        // String should be normalized to array
        assert_eq!(json, r#"{"grants":{"select":["ROLE_A"]}}"#);
    }

    #[test]
    fn test_grant_config_preserves_array() {
        let mut grants = IndexMap::new();
        grants.insert(
            "select".to_string(),
            StringOrArrayOfStrings::ArrayOfStrings(vec![
                "ROLE_A".to_string(),
                "ROLE_B".to_string(),
            ]),
        );

        let config = TestConfig {
            grants: OmissibleGrantConfig(Omissible::Present(GrantConfig(grants))),
        };
        let json = serde_json::to_string(&config).unwrap();

        assert_eq!(json, r#"{"grants":{"select":["ROLE_A","ROLE_B"]}}"#);
    }

    #[test]
    fn test_grant_config_omitted_serializes_as_empty_map() {
        let config = TestConfig {
            grants: OmissibleGrantConfig(Omissible::Omitted),
        };
        let json = serde_json::to_string(&config).unwrap();

        // Omitted should serialize as empty map for Jinja compatibility
        assert_eq!(json, r#"{"grants":{}}"#);
    }

    #[test]
    fn test_grant_config_empty_map() {
        let config = TestConfig {
            grants: OmissibleGrantConfig(Omissible::Present(GrantConfig(IndexMap::new()))),
        };
        let json = serde_json::to_string(&config).unwrap();

        assert_eq!(json, r#"{"grants":{}}"#);
    }
}
