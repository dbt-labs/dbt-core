use std::collections::BTreeMap;

use dbt_common::node_selector::{IndirectSelection, SelectExpression};
use dbt_serde_yaml::{JsonSchema, UntaggedEnumDeserialize};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::serde::FloatOrString;

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
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub struct CompositeExpr {
    #[serde(flatten)]
    pub kind: CompositeKind,
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
    MethodKey(BTreeMap<String, String>),
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
    pub value: String,

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
