//! Phase-core ctx structs.
//!
//! [`GlobalCore`] is the typed shape of the three Jinja globals available on
//! every dbt jinja env from the load phase onwards: `run_started_at`,
//! `target`, `flags`. Every later phase's outer ctx flattens this core via
//! `#[serde(flatten)]` and adds phase-specific fields.
//!
//! `flags` is intentionally typed `MinijinjaValue` (not a concrete map or
//! Object) so the same field can carry the load-phase shape (a plain
//! `BTreeMap<String, Value>`, wrapped via `Value::from_serialize`) AND the
//! parse-phase shape (the richer `Flags` Object, wrapped via
//! `Value::from_object`). Both wrappings round-trip through serde with
//! Object identity intact via the [`crate::JinjaObject`] smuggle path.

use minijinja::Value as MinijinjaValue;
use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;
use schemars::JsonSchema;
use serde::Serialize;

use crate::TargetContextMap;
use crate::jinja_object::JinjaObject;

/// Globals available on every dbt jinja env from the load phase onwards.
/// Flattened into [`crate::LoadCtx`] and into every later phase's core.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct GlobalCore {
    /// `{{ run_started_at }}` — UTC datetime of the dbt invocation.
    pub run_started_at: JinjaObject<PyDateTime>,

    /// `{{ target }}` — connection metadata (profile name, database, schema,
    /// adapter type, …).
    #[schemars(with = "serde_json::Value")]
    pub target: TargetContextMap,

    /// `{{ flags }}` — CLI + project flags. At load this wraps a plain
    /// `BTreeMap<String, Value>` (`Value::from_serialize`); at parse it
    /// wraps the `Flags` Object (`Value::from_object`). Either wrapping
    /// round-trips through serde with Object identity preserved via the
    /// minijinja `VALUE_HANDLES` smuggle path.
    #[schemars(with = "serde_json::Value")]
    pub flags: MinijinjaValue,
}
