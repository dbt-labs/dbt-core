use std::{collections::BTreeMap, path::PathBuf};

use dbt_yaml::Value;
use minijinja::{
    ArgSpec,
    machinery::Span,
    macro_unit::{MacroInfo, MacroUnit},
};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::common::DocsConfig;

/// Macro argument as defined in v12 manifest schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MacroArgument {
    pub name: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(default)]
    pub description: String,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtMacro {
    pub name: String,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    #[serde(skip_serializing)]
    pub span: Option<Span>,
    pub unique_id: String,
    pub macro_sql: String,
    pub depends_on: MacroDependsOn,
    pub description: String,
    pub meta: BTreeMap<String, Value>,
    pub docs: Option<DocsConfig>,
    pub patch_path: Option<PathBuf>,
    pub funcsign: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<ArgSpec>,
    /// Macro arguments from YAML spec (used for manifest serialization via ManifestMacro)
    #[serde(skip)]
    pub arguments: Vec<MacroArgument>,
    #[serde(skip_serializing)]
    pub macro_name_span: Option<Span>,
    pub __other__: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct MacroDependsOn {
    pub macros: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DbtDocsMacro {
    pub name: String,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub unique_id: String,
    pub block_contents: String,
}

pub fn build_macro_units(nodes: &BTreeMap<String, DbtMacro>) -> BTreeMap<String, Vec<MacroUnit>> {
    let mut macros = BTreeMap::new();
    for (_, inner_macro) in nodes.iter() {
        macros
            .entry(inner_macro.package_name.clone())
            .or_insert(vec![])
            .push(MacroUnit {
                info: MacroInfo {
                    name: inner_macro.name.clone(),
                    path: inner_macro.original_file_path.clone(),
                    span: inner_macro.span.expect("span is required"),
                    funcsign: inner_macro.funcsign.clone(),
                    args: inner_macro.args.clone(),
                    unique_id: inner_macro.unique_id.clone(),
                    name_span: inner_macro.macro_name_span.expect("name_span is required"),
                },
                sql: inner_macro.macro_sql.clone(),
            });
    }
    macros
}
