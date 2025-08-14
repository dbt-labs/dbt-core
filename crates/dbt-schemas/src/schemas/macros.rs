use std::{collections::BTreeMap, path::PathBuf};

use dbt_serde_yaml::Value;
use minijinja::{
    ArgSpec,
    machinery::Span,
    macro_unit::{MacroInfo, MacroUnit},
};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

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
    pub patch_path: Option<PathBuf>,
    pub funcsign: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<ArgSpec>,
    #[serde(flatten)]
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
                },
                sql: inner_macro.macro_sql.clone(),
            });
    }
    macros
}
