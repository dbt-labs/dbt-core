use std::collections::BTreeMap;
use std::ops::Deref;

use dbt_common::node_selector::{IndirectSelection, SelectExpression};
use dbt_yaml::{JsonSchema, UntaggedEnumDeserialize};
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::serde::FloatOrString;

// =============================================================================
// SelectorValue — accepts any YAML scalar (string, boolean, number) and
// normalises it to a `String`.
//
// This matches dbt-core's Python behaviour where the `value` field in a
// selector definition is typed as `Any`, so users can write:
//
//     value: true          # boolean
//     value: 42            # number
//     value: selector_name # string
//
// without quoting non-string values.
// =============================================================================

/// A selector value that accepts any YAML scalar (string, boolean, number)
/// and normalizes it to a string representation for downstream matching.
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct SelectorValue(pub String);

impl SelectorValue {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for SelectorValue {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SelectorValue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SelectorValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for SelectorValue {
    fn from(s: &str) -> Self {
        SelectorValue(s.to_string())
    }
}

impl From<String> for SelectorValue {
    fn from(s: String) -> Self {
        SelectorValue(s)
    }
}

impl From<SelectorValue> for String {
    fn from(v: SelectorValue) -> Self {
        v.0
    }
}

impl Serialize for SelectorValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SelectorValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SelectorValueVisitor;

        impl<'de> Visitor<'de> for SelectorValueVisitor {
            type Value = SelectorValue;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a string, boolean, or number")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<SelectorValue, E> {
                Ok(SelectorValue(v.to_string()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<SelectorValue, E> {
                Ok(SelectorValue(v))
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> Result<SelectorValue, E> {
                Ok(SelectorValue(v.to_string()))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<SelectorValue, E> {
                Ok(SelectorValue(v.to_string()))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<SelectorValue, E> {
                Ok(SelectorValue(v.to_string()))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<SelectorValue, E> {
                Ok(SelectorValue(v.to_string()))
            }
        }

        deserializer.deserialize_any(SelectorValueVisitor)
    }
}

//
// ---- top-level file -------------------------------------------------------------------------
//
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SelectorFile {
    pub version: Option<FloatOrString>,
    /// List of named selectors that may later be referenced with
    /// `dbt run --selector <name>`.
    pub selectors: Vec<SelectorDefinition>,
}

//
// ---- one named selector ---------------------------------------------------------------------
//

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SelectorDefinition {
    /// The key used in `--selector <name>`.
    pub name: String,

    /// Human-readable description (optional).
    #[serde(default)]
    pub description: Option<String>,

    /// Whether this selector should be used when the user does *not*
    /// pass `--select` / `--selector`.
    #[serde(default)]
    pub default: Option<bool>,

    /// Either a bare CLI string or a full YAML expression tree.
    pub definition: SelectorDefinitionValue,
}

//
// ---- definition discriminated union ---------------------------------------------------------
//

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, JsonSchema)]
#[serde(untagged)]
pub enum SelectorDefinitionValue {
    /// CLI-style selector string (e.g. `"snowplow tag:nightly"`).
    String(String),

    /// Full YAML tree (see `SelectorExpr` below).
    Full(SelectorExpr),
}

/// Top‐level expression: either a boolean node or a single atom
#[derive(Serialize, UntaggedEnumDeserialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum SelectorExpr {
    Composite(CompositeExpr),
    Atom(AtomExpr),
}

/// A boolean composition of other selectors
#[derive(Serialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub struct CompositeExpr {
    pub kind: BTreeMap<String, CompositeKind>,
}

impl<'de> Deserialize<'de> for CompositeExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CompositeExprVisitor;

        impl<'de> Visitor<'de> for CompositeExprVisitor {
            type Value = CompositeExpr;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(
                    f,
                    "a map with exactly one of the keys 'union' or 'intersection'"
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut composite_kind: Option<CompositeKind> = None;
                let mut found_key: Option<String> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "union" => {
                            if composite_kind.is_some() {
                                let _: IgnoredAny = map.next_value()?;
                                return Err(de::Error::custom(
                                    "multiple keys provided; expected only one of 'union' or 'intersection'",
                                ));
                            }
                            let values: Vec<SelectorDefinitionValue> = map.next_value()?;
                            composite_kind = Some(CompositeKind::Union(values));
                            found_key = Some("union".to_string());
                        }
                        "intersection" => {
                            if composite_kind.is_some() {
                                let _: IgnoredAny = map.next_value()?;
                                return Err(de::Error::custom(
                                    "multiple keys provided; expected only one of 'union' or 'intersection'",
                                ));
                            }
                            let values: Vec<SelectorDefinitionValue> = map.next_value()?;
                            composite_kind = Some(CompositeKind::Intersection(values));
                            found_key = Some("intersection".to_string());
                        }
                        other => {
                            let _: IgnoredAny = map.next_value()?;
                            return Err(de::Error::unknown_field(
                                other,
                                &["union", "intersection"],
                            ));
                        }
                    }
                }

                match (found_key, composite_kind) {
                    (Some(key), Some(kind)) => {
                        let mut m = BTreeMap::new();
                        m.insert(key, kind);
                        Ok(CompositeExpr { kind: m })
                    }
                    _ => Err(de::Error::custom(
                        "expected a map with a 'union' or 'intersection' key",
                    )),
                }
            }
        }

        deserializer.deserialize_map(CompositeExprVisitor)
    }
}

/// Is this an `OR` or an `AND`?
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum CompositeKind {
    Union(Vec<SelectorDefinitionValue>),
    Intersection(Vec<SelectorDefinitionValue>),
}

//
// ---- full YAML selector AST -----------------------------------------------------------------
//

/// The true leaves: either a method, a shorthand, or an exclude
#[derive(Serialize, UntaggedEnumDeserialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum AtomExpr {
    Method(MethodAtomExpr),
    Exclude(ExcludeAtomExpr),
    /// Direct method name as key with value
    MethodKey(BTreeMap<String, SelectorValue>),
}

/// A *resolved* selector ⇒ the "include" (`select`) expression and the
/// optional "exclude" (`exclude`) expression that will later be handed
/// to the scheduler.
#[derive(Debug, Clone, Default)]
pub struct ResolvedSelector {
    pub include: Option<SelectExpression>,
    pub exclude: Option<SelectExpression>,
}

/// What we really need at runtime for each selector.
#[derive(Debug, Clone)]
pub struct SelectorEntry {
    pub include: SelectExpression, // the include expression (which may contain nested excludes)
    pub is_default: bool,          // original `default: true`
    pub description: Option<String>, // docs string from YAML
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MethodAtomExpr {
    pub method: String,
    pub value: SelectorValue,

    // graph-walk flags (all optional / default = false)
    #[serde(default)]
    pub childrens_parents: bool,
    #[serde(default)]
    pub parents: bool,
    #[serde(default)]
    pub children: bool,

    // depth limits
    #[serde(default)]
    pub parents_depth: Option<u32>,
    #[serde(default)]
    pub children_depth: Option<u32>,

    // indirect selection
    #[serde(default)]
    pub indirect_selection: Option<IndirectSelection>,

    // exclude
    #[serde(default)]
    pub exclude: Option<Vec<SelectorDefinitionValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ExcludeAtomExpr {
    pub exclude: Vec<SelectorDefinitionValue>,
}
