use std::collections::BTreeMap;

use dbt_schemas::schemas::macros::DbtDocsMacro;
use minijinja::{
    constants::{MACRO_DISPATCH_ORDER, TARGET_PACKAGE_NAME},
    value::Value as MinijinjaValue,
};

use crate::functions::DocMacro;

/// Builds a context for resolving models
pub fn build_resolve_context(
    root_project_name: &str,
    local_project_name: &str,
    docs_macros: &BTreeMap<String, DbtDocsMacro>,
    macro_dispatch_order: BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, MinijinjaValue> {
    let mut ctx = BTreeMap::new();
    let docs_map: BTreeMap<(String, String), String> = docs_macros
        .values()
        .map(|v| {
            (
                (v.package_name.clone(), v.name.clone()),
                v.block_contents.clone(),
            )
        })
        .collect();

    ctx.insert(
        "doc".to_string(),
        MinijinjaValue::from_object(DocMacro::new(root_project_name.to_string(), docs_map)),
    );

    ctx.insert(
        MACRO_DISPATCH_ORDER.to_string(),
        MinijinjaValue::from_object(
            macro_dispatch_order
                .into_iter()
                .map(|(k, v)| (MinijinjaValue::from(k), MinijinjaValue::from(v)))
                .collect::<BTreeMap<_, _>>(),
        ),
    );

    ctx.insert(
        TARGET_PACKAGE_NAME.to_string(),
        MinijinjaValue::from(local_project_name),
    );

    ctx.insert("execute".to_string(), MinijinjaValue::from(false));

    ctx
}
