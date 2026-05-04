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
use dbt_jinja_ctx::LoadCtx;
use minijinja::Value as MinijinjaValue;

use jinja_ctx_slt::{PhaseFixtures, run_slt};

fn fixture_load_ctx() -> LoadCtx {
    let run_started_at = Tz::UTC.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();

    let mut target_inner: BTreeMap<String, MinijinjaValue> = BTreeMap::new();
    target_inner.insert("name".to_string(), MinijinjaValue::from("dev"));
    target_inner.insert("schema".to_string(), MinijinjaValue::from("public"));
    target_inner.insert("database".to_string(), MinijinjaValue::from("analytics"));
    let target = Arc::new(target_inner);

    let mut flags = BTreeMap::new();
    flags.insert("FULL_REFRESH".to_string(), MinijinjaValue::from(false));
    flags.insert("STORE_FAILURES".to_string(), MinijinjaValue::from(false));
    flags.insert("INTROSPECT".to_string(), MinijinjaValue::from(true));

    LoadCtx::new(run_started_at, target, flags)
}

fn fixtures() -> PhaseFixtures {
    PhaseFixtures {
        load: Some(MinijinjaValue::from_serialize(fixture_load_ctx())),
        ..PhaseFixtures::default()
    }
}

#[test]
fn load_basics_slt() {
    run_slt("tests/data/jinja_ctx_slt/load_basics.slt", &fixtures())
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
