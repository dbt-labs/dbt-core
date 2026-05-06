//! `Object` impls used as values in typed-ctx structs.
//!
//! These move here progressively from `dbt-jinja-utils` so the typed-ctx
//! structs can hold concrete `JinjaObject<T>` slots instead of opaque
//! `MinijinjaValue`. Each move is gated on the type being expressible
//! without depending on `dbt-common` / `dbt-schemas` / `dbt-adapter`
//! (`dbt-jinja-ctx` deliberately depends on none of those).
//!
//! Currently moved (PR 5): `ParseExecute`, `MacroLookupContext`,
//! `DbtNamespace` (dispatch-side).
//!
//! Pending later PRs:
//! * `ParseMetricReference`, `ParseConfigValue` — bundle with `ParseConfig`
//!   in PR 6 (when `ConfigModelHandle` lands and dyn-erases `<T>`).
//! * `DocMacro` — defer until its `dbt-common` dependency chain
//!   (`CodeLocationWithFile`, `StatusReporter`,
//!   `emit_warn_log_from_fs_error`) gets sorted.
//! * `ResolveRefFunction<T>`, `ResolveSourceFunction<T>`,
//!   `ResolveFunctionFunction<T>`, `ParseConfig<T>` — PR 6 (with
//!   `ConfigModelHandle` dyn-erasure).
//! * `RefFunction`, `SourceFunction`, `FunctionFunction`,
//!   `MicrobatchRefContext`, `LazyFlatGraph`, `CompileConfig` — PR 7+
//!   (compile/run phase migrations).

pub mod lookup;
pub mod parse;

pub use lookup::{DbtNamespace, MacroLookupContext};
pub use parse::ParseExecute;
