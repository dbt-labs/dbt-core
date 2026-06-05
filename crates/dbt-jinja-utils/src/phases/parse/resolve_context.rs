use std::collections::BTreeMap;

use dbt_jinja_ctx::{DbtNamespace, JinjaObject, ResolveBaseCtx, to_jinja_btreemap};
use dbt_schemas::schemas::macros::DbtDocsMacro;
use minijinja::value::Value as MinijinjaValue;

use crate::functions::DocMacro;

/// Builds a context for resolving models.
///
/// Internally constructs a typed [`ResolveBaseCtx`]; the
/// `BTreeMap<String, MinijinjaValue>` return type is preserved for now so
/// today's `dbt-parser` callers (which `.extend(...)` per-model overlays onto
/// this base) continue to work. A follow-up PR migrates them to consume the
/// typed struct directly via `render_named_str<S: Serialize>(...)`.
pub fn build_resolve_context(
    root_project_name: &str,
    local_project_name: &str,
    docs_macros: &BTreeMap<String, DbtDocsMacro>,
    macro_dispatch_order: BTreeMap<String, Vec<String>>,
    namespace_keys: Vec<String>,
) -> BTreeMap<String, MinijinjaValue> {
    let docs_map: BTreeMap<(String, String), String> = docs_macros
        .values()
        .map(|v| {
            (
                (v.package_name.clone(), v.name.clone()),
                v.block_contents.clone(),
            )
        })
        .collect();

    let dbt_namespaces: BTreeMap<String, JinjaObject<DbtNamespace>> = namespace_keys
        .into_iter()
        .map(|key| {
            let value = JinjaObject::new(DbtNamespace::new(&key));
            (key, value)
        })
        .collect();

    // Wrap each per-namespace search order as `Value::from(Vec<String>)` —
    // dispatch lookup downcasts to `Vec<String>` so the underlying Object
    // type must be exactly that, not the `MutableVec<Value>` that
    // serde-serializing a `Vec<String>` produces.
    let macro_dispatch_order: BTreeMap<String, MinijinjaValue> = macro_dispatch_order
        .into_iter()
        .map(|(k, v)| (k, MinijinjaValue::from(v)))
        .collect();

    let ctx = ResolveBaseCtx {
        doc: MinijinjaValue::from_object(DocMacro::new(root_project_name.to_string(), docs_map)),
        macro_dispatch_order,
        target_package_name: local_project_name.to_string(),
        execute: false,
        node: MinijinjaValue::NONE,
        connection_name: String::new(),
        dbt_namespaces,
    };

    to_jinja_btreemap(&ctx)
}
