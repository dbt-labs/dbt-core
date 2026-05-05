//! SLT-style cross-phase Jinja context tests.
//!
//! Each `#[test]` here builds the typed-ctx fixtures available at the current
//! migration step, then runs an `.slt` file against them via the harness in
//! `common/jinja_ctx_slt.rs`. Phases that haven't landed yet are simply not
//! populated in the fixtures map; rows referencing them are silently skipped
//! until a future migration step fills them in.

#[path = "common/jinja_ctx_slt.rs"]
mod jinja_ctx_slt;

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::TimeZone;
use chrono_tz::Tz;
use dbt_jinja_ctx::{GlobalCore, JinjaObject, LoadCtx, ResolveCore};
use dbt_jinja_vars::ConfiguredVar;
use minijinja::Value as MinijinjaValue;
use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;
use minijinja_contrib::modules::pytz::PytzTimezone;

use jinja_ctx_slt::{PhaseFixtures, run_slt};

fn fixture_target() -> Arc<BTreeMap<String, MinijinjaValue>> {
    let mut t: BTreeMap<String, MinijinjaValue> = BTreeMap::new();
    t.insert("name".to_string(), MinijinjaValue::from("dev"));
    t.insert("schema".to_string(), MinijinjaValue::from("public"));
    t.insert("database".to_string(), MinijinjaValue::from("analytics"));
    Arc::new(t)
}

fn fixture_load_flags() -> BTreeMap<String, MinijinjaValue> {
    let mut f = BTreeMap::new();
    f.insert("FULL_REFRESH".to_string(), MinijinjaValue::from(false));
    f.insert("STORE_FAILURES".to_string(), MinijinjaValue::from(false));
    f.insert("INTROSPECT".to_string(), MinijinjaValue::from(true));
    f
}

fn fixture_load_ctx() -> LoadCtx {
    let run_started_at = Tz::UTC.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    LoadCtx::new(run_started_at, fixture_target(), fixture_load_flags())
}

fn fixture_resolve_core() -> ResolveCore {
    let run_started_at = Tz::UTC.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    let target = fixture_target();
    let mut invocation_args_dict: BTreeMap<String, MinijinjaValue> = BTreeMap::new();
    invocation_args_dict.insert("debug".to_string(), MinijinjaValue::from(false));

    ResolveCore {
        global_core: GlobalCore {
            run_started_at: JinjaObject::new(PyDateTime::new_aware(
                run_started_at,
                Some(PytzTimezone::new(Tz::UTC)),
            )),
            target: target.clone(),
            // Fixture stand-in for the parse-phase `Flags` Object — a bare
            // Map is observationally equivalent at the .slt level (key
            // lookups work the same way through `get_attr`). The real
            // `Flags` Object identity is exercised end-to-end via the
            // `dbt-cli clean_project` goldie.
            flags: MinijinjaValue::from_serialize(fixture_load_flags()),
        },
        project_name: "my_project".to_string(),
        env: target,
        invocation_args_dict,
        invocation_id: "11111111-2222-3333-4444-555555555555".to_string(),
        var: JinjaObject::new(ConfiguredVar::new(BTreeMap::new(), BTreeMap::new())),
        database: Some("analytics".to_string()),
        schema: Some("public".to_string()),
        write: MinijinjaValue::NONE,
    }
}

fn fixtures() -> PhaseFixtures {
    PhaseFixtures {
        load: Some(MinijinjaValue::from_serialize(fixture_load_ctx())),
        resolve_globals: Some(MinijinjaValue::from_serialize(fixture_resolve_core())),
        ..PhaseFixtures::default()
    }
}

#[test]
fn load_basics_slt() {
    run_slt("tests/data/jinja_ctx_slt/load_basics.slt", &fixtures())
}

#[test]
fn resolve_globals_basics_slt() {
    run_slt(
        "tests/data/jinja_ctx_slt/resolve_globals_basics.slt",
        &fixtures(),
    )
}

#[test]
fn cross_phase_keyset_slt() {
    run_slt(
        "tests/data/jinja_ctx_slt/cross_phase_keyset.slt",
        &fixtures(),
    )
}

/// Smoke-test: a deliberately-wrong expectation must surface as a clear
/// `expected ... got ...` panic. Guards the harness against silently passing
/// when its comparator is broken.
#[test]
#[should_panic(expected = "expected `\"not-dev\"`, got `\"dev\"`")]
fn slt_harness_reports_value_mismatch() {
    run_slt(
        "tests/data/jinja_ctx_slt/_negative_value_mismatch.slt",
        &fixtures(),
    )
}
