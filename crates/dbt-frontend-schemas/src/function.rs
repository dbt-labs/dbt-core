use super::{dialect::Dialect, util::is_false};
use crate::ident::Identifier;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use strum_macros::Display;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Example {
    /// The sql string corresponding to the input of this example
    pub input: String,
    /// The output corresponding to running the input string
    pub output: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
// A reclassify setting, equivalent to a call to the reclassify function
pub struct Reclassify {
    /// Target classifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// Expected source classifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialOrd, Ord, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
/// All file path should either be relative to the workspace, or absolute for an object store like AWS s3://
pub struct FilePath {
    /// A filepath
    pub path: String,
    /// Last modified of the file
    #[serde(skip_serializing)]
    pub time: Option<SystemTime>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
/// A function block defines the signature for user defined
pub struct Function {
    /// The name of the function [syntax: [[catalog.]schema].function]
    pub name: Identifier,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<Identifier>,
    #[serde(skip)]
    pub original_name: Option<Identifier>,
    /// The function category
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub section: String,
    /// The dialect that provides this function
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<Dialect>,
    /// A description of this function
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Arbitrary number of arguments of an common type out of a list of valid types
    #[serde(default, skip_serializing_if = "Variadic::is_default")]
    pub variadic: Variadic,
    /// The function kind
    #[serde(default, skip_serializing_if = "FunctionKind::is_default")]
    pub kind: FunctionKind,
    /// The arguments of this function
    pub parameters: Vec<Parameter>,
    /// The arguments of this function
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub optional_parameters: Vec<OptionalParameter>,
    /// The results of this function (can be a tuple)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub returns: Option<Parameter>,
    /// The constraints on generic type bounds
    #[serde(default, skip_serializing_if = "LinkedHashMap::is_empty")]
    pub constraints: LinkedHashMap<String, String>,
    /// The generic type bounds
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binds: Vec<TypeBound>,
    /// volatility - The volatility of the function.
    #[serde(default, skip_serializing_if = "Volatility::is_default")]
    pub volatility: Volatility,
    /// example - Example use of the function (tuple with input/output)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<Example>,
    /// cross-link - link to existing documentation, for example:
    /// https://trino.io/docs/current/functions/datetime.html#date_trunc
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cross_link: String,
    /// Array of reclassify instructions for changing the attached classifier labels
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reclassify: Vec<Reclassify>,
    /// Function defined by these set of .sdf files
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_locations: Vec<FilePath>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub implemented_by: Option<FunctionImplSpec>,
    /// Function can be called without parentheses, e.g. as if it were a constant, e.g. current_date
    #[serde(default, skip_serializing_if = "is_false")]
    pub special: bool,
    // #[serde(skip)]
    // pub overload: OnceCell<FunctionOverload>,
}

impl Function {
    pub fn with_dialect(self, dialect: Dialect) -> Self {
        Self {
            dialect: Some(dialect),
            ..self
        }
    }

    pub fn is_table(&self) -> bool {
        self.kind == FunctionKind::Table
    }

    pub fn resolve_aliases(mut self) -> Vec<Self> {
        let mut resolved = vec![];
        if self.aliases.is_empty() {
            resolved.push(self)
        } else {
            let aliases = std::mem::take(&mut self.aliases);
            resolved.push(self);
            for alias in aliases {
                let mut copy = resolved[0].clone();
                copy.original_name = Some(copy.name);
                copy.name = alias;
                resolved.push(copy);
            }
        }
        resolved
    }
}

#[derive(
    Serialize, Deserialize, PartialEq, Debug, Default, Clone, Display, Eq, Hash, PartialOrd, Ord,
)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum FunctionKind {
    #[default]
    #[strum(serialize = "scalar")]
    Scalar,
    #[strum(serialize = "aggregate")]
    Aggregate,
    #[strum(serialize = "window")]
    Window,
    #[strum(serialize = "table")]
    Table,
}
impl FunctionKind {
    pub fn is_default(&self) -> bool {
        *self == FunctionKind::Scalar
    }
}

/// Indicates how a function's evaluation is implemented.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum FunctionImplSpec {
    /// By a built-in primitive in Datafusion. (Being phased out in favor of UDFs.)
    Builtin,
    /// By a UDF in the sdf-functions crate.
    Rust(RustFunctionSpec),
    /// By a UDF in the datafusion crate.
    Datafusion(DataFusionSpec),
    /// By a CREATE FUNCTION. (Not yet supported for evaluation.)
    Sql,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RustFunctionSpec {
    /// The name attribute of the implementing UDF.
    /// None indicates the UDF is named the same as the function.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct DataFusionSpec {
    /// The name attribute of the implementing UDF.
    /// None indicates the UDF is named the same as the function.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udf: Option<Identifier>,
}

/// Arbitrary number of arguments of an common type out of a list of valid types
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize, Default, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Variadic {
    // Arguments can have different types
    #[default]
    NonUniform,

    /// All arguments have the same types
    Uniform,

    /// All even arguments have one type, odd arguments have another type
    EvenOdd,

    /// Any length of arguments, arguments can be different types
    Any,
}

impl Variadic {
    pub fn is_default(&self) -> bool {
        *self == Variadic::NonUniform
    }
}

///A function's volatility, which defines the functions eligibility for certain optimizations
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Volatility {
    /// Pure - An pure function will always return the same output when given the same
    /// input.
    #[default]
    Pure,
    /// Stable - A stable function may return different values given the same input across different
    /// queries but must return the same value for a given input within a query.
    Stable,
    /// Volatile - A volatile function may change the return value from evaluation to evaluation.
    /// Multiple invocations of a volatile function may return different results when used in the
    /// same query.
    Volatile,
}
impl Volatility {
    pub fn is_default(&self) -> bool {
        *self == Volatility::Pure
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct TypeBound {
    pub type_variable: Identifier,
    pub datatypes: Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord, Default)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
/// A function parameter
pub struct Parameter {
    /// The name of the parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Identifier>,
    /// A description of this parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The datatype of this parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datatype: Option<String>,
    /// An array of classifier references
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classifier: Option<Vec<String>>,
    /// The required constant values of this parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constants: Option<Vec<String>>,
    /// The parameter may appear as identifier, without quote
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifiers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub constants_as_identifiers: bool,
    /// The name of capture variable to be populated for constant parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_as: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
/// A function parameter
pub struct OptionalParameter {
    /// The name of the parameter
    #[serde()]
    pub name: Identifier,
    /// A description of this parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The datatype of this parameter
    #[serde()]
    pub datatype: String,
    /// An array of classifier references
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classifier: Option<Vec<String>>,
    /// The required constant value of this parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constant: Option<String>,
    /// The parameter may appear as identifier, without quote
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifiers: Option<Vec<String>>,
}

pub fn get_overload_rustname(function: &Function) -> String {
    if let Some(FunctionImplSpec::Rust(RustFunctionSpec { name: Some(name) })) =
        &function.implemented_by
    {
        name.to_owned()
    } else {
        let function_prefix =
            get_function_rustname(function.original_name.as_ref().unwrap_or(&function.name));
        let parameter_suffix = function
            .parameters
            .iter()
            .map(|p| {
                let datatype: String = p.datatype.to_owned().unwrap_or_default();
                format!(
                    "_{}",
                    datatype
                        .replace(['(', ',', '<'], "_")
                        .replace([')', '>', ' ', '$'], "")
                )
            })
            .join("");

        format!("{}{}", function_prefix, parameter_suffix)
    }
}

pub fn get_function_rustname(function_ident: &Identifier) -> Identifier {
    function_ident
        .as_str()
        .replace(['$'], "_")
        .to_ascii_lowercase()
        .into()
}
