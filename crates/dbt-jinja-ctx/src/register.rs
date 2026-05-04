//! Register the typed env-globals struct on a minijinja `Environment`.
//!
//! Phase initializers construct their typed ctx struct and feed it into the
//! env via either:
//!
//! * [`to_jinja_globals_btreemap`] — converts a `Serialize` ctx struct into
//!   the `BTreeMap<String, Value>` shape that `JinjaEnvBuilder.with_globals`
//!   accepts. Use this when constructing the env, so `try_with_macros`
//!   (which reads `self.globals` for replay-mode detection) sees the
//!   registered globals before macros are loaded.
//! * [`register_globals_from_serialize`] — registers globals on an
//!   already-built `Environment`. Useful for tests and for cases where the
//!   ctx is computed after the env is built. Order-sensitive: do NOT use
//!   this when builder methods downstream of `with_globals`
//!   (`try_with_macros`, future builder hooks) need to read globals.

use std::collections::BTreeMap;

use minijinja::{Environment, Value};
use serde::Serialize;

/// Walk a typed ctx struct and register each top-level field as a global on
/// `env`. Object-bearing fields wrapped in [`crate::JinjaObject<T>`] retain
/// their Jinja dispatch identity through serde.
///
/// Prefer [`to_jinja_globals_btreemap`] when feeding into a `JinjaEnvBuilder`:
/// any builder method that reads `self.globals` must see them before that
/// method runs, and `with_globals` is the only builder hook for that path.
///
/// # Panics
///
/// Panics if `ctx` does not serialize to a map-shaped value. Every ctx struct
/// in this crate satisfies that — derive `Serialize` on a struct with named
/// fields produces an `ObjectRepr::Map` value via minijinja's
/// `SerializeStruct` impl.
pub fn register_globals_from_serialize<T: Serialize>(env: &mut Environment<'_>, ctx: &T) {
    for (name, val) in to_jinja_globals_btreemap(ctx) {
        env.add_global(name, val);
    }
}

/// Convert a typed ctx struct into the `BTreeMap<String, Value>` shape that
/// `JinjaEnvBuilder.with_globals` accepts. Object identity for callable
/// values wrapped in [`crate::JinjaObject<T>`] is preserved through the
/// `VALUE_HANDLES` smuggle path.
///
/// This is the only env-globals hook that runs before
/// `JinjaEnvBuilder.try_with_macros`, so phase initializers must thread the
/// ctx through here (not [`register_globals_from_serialize`]) for any
/// behaviour that depends on globals being visible at macro-load time —
/// notably the replay-mode Elementary suppression in `try_with_macros`.
///
/// # Panics
///
/// Same contract as [`register_globals_from_serialize`].
pub fn to_jinja_globals_btreemap<T: Serialize>(ctx: &T) -> BTreeMap<String, Value> {
    let value = Value::from_serialize(ctx);
    let object = value
        .as_object()
        .expect("globals ctx must serialize to a map-shaped value");
    object
        .try_iter_pairs()
        .expect("globals ctx must serialize to a map-shaped value")
        .filter_map(|(k, v)| k.as_str().map(|s| (s.to_string(), v)))
        .collect()
}
