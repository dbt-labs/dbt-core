//! Per-render context structs for the compile (render) phase.
//!
//! Compile shares the parse `JinjaEnv` (mutated in place by
//! `configure_compile_and_run_jinja_environment` — only adapter +
//! `undefined_behavior` change, no globals re-registered), so the structs in
//! this module describe *per-render* contexts only.
//!
//! Two flavours:
//! * [`CompileBaseCtx`] — the compile-time base, returned by
//!   `build_compile_base_ctx`. Used as the foundation for every per-node
//!   compile context (today's caller `.clone()`s it onto a per-node overlay).
//! * [`CompileNodeCtx`] — the per-node overlay layered on top of
//!   `CompileBaseCtx` when rendering each node's SQL. Adds `this`, the
//!   per-node `model` map, the validated `ref`/`source`/`function`/`config`
//!   variants, and the underscore-decorated `TARGET_UNIQUE_ID` /
//!   `__minijinja_current_path` / `__minijinja_current_span` /
//!   `__dbt_current_execution_phase__` constants.
//!
//! Object-typed slots are typed as [`MinijinjaValue`] for now: the underlying
//! `Object` impls (`RefFunction`, `SourceFunction`, `FunctionFunction`,
//! `LazyFlatGraph`, `CompileConfig`, `DummyConfig`, `MicrobatchRefContext`,
//! `RelationObject`, …) live in `dbt-jinja-utils` / `dbt-adapter`, which
//! `dbt-jinja-ctx` cannot depend on. A later PR moves the std-only ones here
//! and tightens those slots; the `dbt-adapter`-resident `RelationObject`
//! likely stays opaque or grows a thin handle.

use std::collections::BTreeMap;

use minijinja::Value as MinijinjaValue;
use schemars::JsonSchema;
use serde::Serialize;

use crate::JinjaObject;
use crate::objects::{DbtNamespace, DummyConfig, MacroLookupContext};

/// Per-render compile-base context. Today's `build_compile_base_ctx`
/// populates this 1:1 — same field names, same key constants
/// (`MACRO_DISPATCH_ORDER`, `TARGET_PACKAGE_NAME`).
///
/// Used as the base for per-node compile contexts; the calling renderer
/// `.clone()`s it and overlays per-node validations.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CompileBaseCtx {
    /// `{{ MACRO_DISPATCH_ORDER }}` — per-package dispatch order map. Same
    /// downcast contract as [`crate::ResolveBaseCtx::macro_dispatch_order`]:
    /// each value MUST be `MinijinjaValue::from(Vec<String>)` constructed at
    /// the call site, NOT a serde-serialized `Vec<String>`.
    #[serde(rename = "MACRO_DISPATCH_ORDER")]
    #[schemars(with = "BTreeMap<String, Vec<String>>")]
    pub macro_dispatch_order: BTreeMap<String, MinijinjaValue>,

    /// `{{ ref(...) }}` — unvalidated `RefFunction` Object (validated variant
    /// overlaid per-node).
    #[serde(rename = "ref")]
    #[schemars(with = "serde_json::Value")]
    pub ref_fn: MinijinjaValue,

    /// `{{ source(...) }}` — unvalidated `SourceFunction` Object.
    #[schemars(with = "serde_json::Value")]
    pub source: MinijinjaValue,

    /// `{{ function(...) }}` — unvalidated `FunctionFunction` Object.
    #[schemars(with = "serde_json::Value")]
    pub function: MinijinjaValue,

    /// `{{ execute }}` — `true` at compile/run (flipped from `false` at parse).
    pub execute: bool,

    /// `{{ builtins.* }}` — `BTreeMap<String, MinijinjaValue>` Object holding
    /// `ref` / `source` / `function`. Compile-node-context code downcasts
    /// this back to `BTreeMap<String, MinijinjaValue>` when overlaying
    /// validated variants per-node, so the underlying Object type must be
    /// exactly that.
    #[schemars(with = "serde_json::Value")]
    pub builtins: MinijinjaValue,

    /// `{{ dbt_metadata_envs }}` — `BTreeMap<String, MinijinjaValue>` Object
    /// (string-valued) populated from the OS `DBT_ENV_CUSTOM_ENV_*` vars.
    #[schemars(with = "serde_json::Value")]
    pub dbt_metadata_envs: MinijinjaValue,

    /// `{{ context }}` — `MacroLookupContext` Object for `context['name']`
    /// macro dispatch.
    pub context: JinjaObject<MacroLookupContext>,

    /// `{{ graph }}` — `LazyFlatGraph` Object that builds the flat graph on
    /// first attribute access; lives in `dbt-jinja-utils` for now.
    #[schemars(with = "serde_json::Value")]
    pub graph: MinijinjaValue,

    /// `{{ store_result(...) }}` — `ResultStore`-backed closure.
    #[schemars(with = "serde_json::Value")]
    pub store_result: MinijinjaValue,

    /// `{{ load_result(...) }}` — `ResultStore`-backed closure.
    #[schemars(with = "serde_json::Value")]
    pub load_result: MinijinjaValue,

    /// `{{ TARGET_PACKAGE_NAME }}` — local project name.
    /// Key string matches `minijinja::constants::TARGET_PACKAGE_NAME`.
    #[serde(rename = "TARGET_PACKAGE_NAME")]
    pub target_package_name: String,

    /// `{{ node }}` — `Value::NONE` at base scope; populated per-node.
    #[schemars(with = "serde_json::Value")]
    pub node: MinijinjaValue,

    /// `{{ connection_name }}` — empty string at base scope.
    pub connection_name: String,

    /// Per-package namespace objects. Each entry becomes its own top-level
    /// Jinja global via `#[serde(flatten)]` — same shape as
    /// [`crate::ResolveBaseCtx::dbt_namespaces`].
    #[serde(flatten)]
    #[schemars(with = "BTreeMap<String, serde_json::Value>")]
    pub dbt_namespaces: BTreeMap<String, JinjaObject<DbtNamespace>>,
}

/// Operation-scope render context (REPL, `run-operation`, pre-compile macro
/// evaluation): a [`CompileBaseCtx`] plus a no-op `DummyConfig` to absorb
/// `config(...)` calls where there is no node config. Node contexts carry
/// their own validated `config`, so it lives here rather than on the base.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct OperationCtx {
    #[serde(flatten)]
    pub base: CompileBaseCtx,

    /// `{{ config(...) }}` — no-op for macro renders without a current
    /// node. Calling `config(...)` returns `""`; `config.get(...)` returns
    /// `None`.
    pub config: JinjaObject<DummyConfig>,
}

/// Per-node compile-time overlay layered onto [`CompileBaseCtx`] for each
/// node SQL render. Today's `build_compile_node_context_inner<T>` populates
/// this 1:1 — same field-key strings (including the underscore-decorated
/// `__minijinja_current_path` / `__minijinja_current_span` /
/// `__dbt_current_execution_phase__` constants).
///
/// At construction, the calling renderer first builds `CompileBaseCtx`, then
/// overlays a `CompileNodeCtx` on top. Several keys (`config`, `ref`,
/// `source`, `function`, `builtins`, `context`) are intentionally
/// re-emitted on the overlay — they shadow the base entries with validated
/// per-node variants. Today's code uses `BTreeMap::insert(...)` to overlay;
/// after migration, callers `.extend(...)` the typed overlay onto the typed
/// base before rendering.
///
/// All Object-typed slots are typed [`MinijinjaValue`] for the same reasons
/// as [`crate::ResolveModelCtx`]: their concrete impls live in
/// `dbt-jinja-utils` / `dbt-adapter`, and `model` / `builtins` are
/// constructed via `Value::from_object(BTreeMap<String, MinijinjaValue>)`
/// which downstream code downcasts.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CompileNodeCtx {
    /// `{{ this }}` — `RelationObject` Object (or deferred-relation
    /// `RelationObject` for unsafe nodes with deferred state available).
    #[schemars(with = "serde_json::Value")]
    pub this: MinijinjaValue,

    /// `{{ database }}` — model's adapter database.
    pub database: String,

    /// `{{ schema }}` — model's adapter schema.
    pub schema: String,

    /// `{{ identifier }}` — model's adapter alias (NOT name; alias is the
    /// truncated form that fits OS path limits).
    pub identifier: String,

    /// `{{ config(...) }}` — `CompileConfig` Object backed by the node's
    /// validated, dot-walkable config map. Shadows
    /// [`CompileBaseCtx::config`] (`DummyConfig`).
    #[schemars(with = "serde_json::Value")]
    pub config: MinijinjaValue,

    /// `{{ ref(...) }}` — `RefFunction` Object **with validation**. Shadows
    /// [`CompileBaseCtx::ref_fn`] (unvalidated).
    #[serde(rename = "ref")]
    #[schemars(with = "serde_json::Value")]
    pub ref_fn: MinijinjaValue,

    /// `{{ source(...) }}` — `SourceFunction` Object **with validation**.
    /// Shadows [`CompileBaseCtx::source`].
    #[schemars(with = "serde_json::Value")]
    pub source: MinijinjaValue,

    /// `{{ function(...) }}` — `FunctionFunction` Object **with validation**.
    /// Shadows [`CompileBaseCtx::function`].
    #[schemars(with = "serde_json::Value")]
    pub function: MinijinjaValue,

    /// `{{ builtins.* }}` — the per-node overlay map carrying the validated
    /// `ref` / `source` / `function` / `config` for downstream code that
    /// reads `builtins.config` directly. Shadows [`CompileBaseCtx::builtins`].
    /// Same downcast contract as
    /// [`crate::ResolveModelCtx::builtins`] —
    /// the underlying Object MUST be `BTreeMap<String, MinijinjaValue>` so
    /// `compile_node_context_inner` can `.downcast_ref::<...>()` it on the
    /// next per-node pass.
    #[schemars(with = "serde_json::Value")]
    pub builtins: MinijinjaValue,

    /// `{{ model.* }}` — model dict serialized via
    /// `convert_yml_to_value_map(model.serialize())` plus a `batch` entry
    /// (today's parse-time stub uses London-1970 datetimes; replaced by the
    /// real microbatch context at run time).
    #[schemars(with = "serde_json::Value")]
    pub model: MinijinjaValue,

    /// `{{ store_result(...) }}` — `ResultStore`-backed closure.
    #[schemars(with = "serde_json::Value")]
    pub store_result: MinijinjaValue,

    /// `{{ load_result(...) }}` — `ResultStore`-backed closure.
    #[schemars(with = "serde_json::Value")]
    pub load_result: MinijinjaValue,

    /// `{{ store_raw_result(...) }}` — `ResultStore`-backed closure.
    #[schemars(with = "serde_json::Value")]
    pub store_raw_result: MinijinjaValue,

    /// `{{ TARGET_PACKAGE_NAME }}` — node's package name (NOT root project's;
    /// shadows [`CompileBaseCtx::target_package_name`] which is the local
    /// project at base-build time).
    #[serde(rename = "TARGET_PACKAGE_NAME")]
    pub target_package_name: String,

    /// `{{ TARGET_UNIQUE_ID }}` — node's `<package>.<name>` identifier.
    /// Key string matches `minijinja::constants::TARGET_UNIQUE_ID`.
    #[serde(rename = "TARGET_UNIQUE_ID")]
    pub target_unique_id: String,

    /// `{{ context }}` — `MacroLookupContext` keyed by root project name.
    /// Shadows [`CompileBaseCtx::context`].
    pub context: JinjaObject<MacroLookupContext>,

    /// `{{ __minijinja_current_path }}` — display path of the rendering
    /// `.sql` file. Key string matches `minijinja::constants::CURRENT_PATH`.
    #[serde(rename = "__minijinja_current_path")]
    pub current_path: String,

    /// `{{ __minijinja_current_span }}` — span at the start of rendering
    /// (typically `minijinja::machinery::Span::default()`). Key string
    /// matches `minijinja::constants::CURRENT_SPAN`.
    #[serde(rename = "__minijinja_current_span")]
    #[schemars(with = "serde_json::Value")]
    pub current_span: MinijinjaValue,

    /// `{{ __dbt_current_execution_phase__ }}` — `"render"` at compile.
    /// Key string matches `minijinja::constants::CURRENT_EXECUTION_PHASE`.
    #[serde(rename = "__dbt_current_execution_phase__")]
    pub current_execution_phase: String,
}
