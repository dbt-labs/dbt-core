//! Per-render context structs for the resolve (parse) phase.
//!
//! Unlike [`crate::core::ResolveCore`] which describes the *env globals* set
//! once at parse-env construction, the structs in this module describe the
//! *per-render* context — what gets passed to `render_named_str<S: Serialize>`
//! at every template render call.
//!
//! At PR 3 only [`ResolveBaseCtx`] (the parse-time base, used when rendering
//! sources / docs / project YAML) lives here. [`crate::resolve::ResolveModelCtx`]
//! lands in a follow-up PR.
//!
//! Object-typed slots (`doc`, `dbt_namespaces[…]`, `node`) are typed as
//! [`MinijinjaValue`] for now: the underlying `Object` impls (`DocMacro`,
//! `DbtNamespace`) live in `dbt-jinja-utils`, which `dbt-jinja-ctx` cannot
//! depend on. A later PR moves them into this crate and tightens those slots
//! to `JinjaObject<DocMacro>` / `JinjaObject<DbtNamespace>`. Until then,
//! Object dispatch identity is preserved through the
//! [`crate::JinjaObject`] / `Value::from_serialize` smuggle path because the
//! ctx-internal `MinijinjaValue::from_object(...)` wrapping happens at the
//! caller site.

use std::collections::BTreeMap;

use minijinja::Value as MinijinjaValue;
use schemars::JsonSchema;
use serde::Serialize;

/// Per-render parse-base context. Today's `build_resolve_context` populates
/// this 1:1 — same field names, same key constants
/// (`MACRO_DISPATCH_ORDER`, `TARGET_PACKAGE_NAME`).
///
/// Used at every template render that doesn't have a per-model overlay
/// (e.g. project-yml resolution, source docs, selectors) and as the base for
/// per-model contexts.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ResolveBaseCtx {
    /// `{{ doc(package, name) }}` — `DocMacro` Object lookup.
    ///
    /// Typed `MinijinjaValue` while `DocMacro` lives in `dbt-jinja-utils`;
    /// a future PR moves it here and tightens to `JinjaObject<DocMacro>`.
    #[schemars(with = "serde_json::Value")]
    pub doc: MinijinjaValue,

    /// `{{ MACRO_DISPATCH_ORDER }}` — per-package dispatch order map.
    /// Key string matches `minijinja::constants::MACRO_DISPATCH_ORDER`.
    ///
    /// Each value is `MinijinjaValue::from(Vec<String>)` constructed at the
    /// call site, NOT a serde-serialized `Vec<String>`. The dispatch lookup
    /// in `minijinja/src/dispatch_object.rs` does
    /// `order.downcast_object::<Vec<String>>()` to read the search order, so
    /// the underlying `Object` type must be `Vec<String>` specifically.
    /// Going through serde would produce a `MutableVec<Value>` instead and
    /// silently break dispatch.
    #[serde(rename = "MACRO_DISPATCH_ORDER")]
    #[schemars(with = "BTreeMap<String, Vec<String>>")]
    pub macro_dispatch_order: BTreeMap<String, MinijinjaValue>,

    /// `{{ TARGET_PACKAGE_NAME }}` — local project name.
    /// Key string matches `minijinja::constants::TARGET_PACKAGE_NAME`.
    #[serde(rename = "TARGET_PACKAGE_NAME")]
    pub target_package_name: String,

    /// `{{ execute }}` — `false` at parse, flipped to `true` at compile/run
    /// (PR 5+).
    pub execute: bool,

    /// `{{ node }}` — `Value::NONE` at base scope; populated per-model.
    #[schemars(with = "serde_json::Value")]
    pub node: MinijinjaValue,

    /// `{{ connection_name }}` — empty string at base scope.
    pub connection_name: String,

    /// Per-package namespace objects. Each entry becomes its own top-level
    /// Jinja global via `#[serde(flatten)]` — e.g. `{ "dbt": <DbtNamespace>,
    /// "snowflake": <DbtNamespace>, … }` flattens into individual `{{ dbt }}`,
    /// `{{ snowflake }}` keys.
    ///
    /// Each value is `MinijinjaValue::from_object(DbtNamespace::new(&key))`
    /// at the call site; the underlying `DbtNamespace` Object in
    /// `dbt-jinja-utils` will move into this crate alongside `DocMacro`.
    #[serde(flatten)]
    #[schemars(with = "BTreeMap<String, serde_json::Value>")]
    pub dbt_namespaces: BTreeMap<String, MinijinjaValue>,
}
