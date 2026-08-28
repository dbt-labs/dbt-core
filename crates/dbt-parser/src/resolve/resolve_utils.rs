use std::collections::BTreeMap;
use std::path::Path;

use dbt_adapter_core::AdapterType;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::error::FsError;
use dbt_common::fs_err;
use dbt_common::io_args::ComputeArg;
use dbt_common::tracing::dbt_emit::emit_warn_log_message;
use dbt_schemas::schemas::common::{DbtMaterialization, DbtQuoting};
use dbt_schemas::schemas::project::AdapterProjectConfig;
use dbt_schemas::state::ProfileAdapter;
use indexmap::IndexMap;

/// Validate the root project's `adapters:` block. Called once per run.
///
/// Keying by adapter type removes two checks by construction: a duplicate entry is
/// impossible in a map, and a key that is not an adapter type at all is rejected at
/// deserialization. What is left is an entry for an adapter *this* target does not
/// declare, which is only a warning — one project is commonly run against several
/// targets, so such an entry is not a mistake, but a stray one would otherwise do
/// nothing at all.
pub(crate) fn validate_adapter_project_configs(
    adapters: Option<&IndexMap<AdapterType, AdapterProjectConfig>>,
    target_adapters: &IndexMap<AdapterType, ProfileAdapter>,
) {
    let Some(adapters) = adapters else {
        return;
    };

    for adapter_type in adapters.keys() {
        if !target_adapters.contains_key(adapter_type) {
            emit_warn_log_message(
                ErrorCode::InvalidConfig,
                format!(
                    "dbt_project.yml configures adapter '{adapter_type}' under `adapters:`, but \
                     the active target does not declare it; the entry has no effect. Declared \
                     adapters are: {}",
                    target_adapters
                        .keys()
                        .map(|t| t.as_ref())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            );
        }
    }
}

/// The authored quoting each declared adapter contributes, keyed by adapter type.
///
/// Two layers, left **unresolved** (`None`s preserved) so that a node's own
/// `+quoting:` still wins over both:
///
/// 1. the adapter's entry in the root `dbt_project.yml` `adapters:` block;
/// 2. the top-level `quoting:` block — but **only for the target's default
///    adapter**. A node on a non-default adapter does not inherit the top-level
///    block; it takes its own entry and then falls through to its adapter type's
///    default. Configuring the default adapter is what the top-level block is for,
///    and letting it leak across adapters is what would otherwise force every
///    adapter in a target to agree on one policy.
///
/// Both inputs come from the **root** project, so this is computed once per run and
/// lives on `RootProjectConfigs`. A dependency package's own top-level `quoting:`
/// block does not enter the chain: it was already overridden by the root's via the
/// root-config overlay, and stays overridden.
pub(crate) fn authored_quoting_per_adapter(
    adapters: Option<&IndexMap<AdapterType, AdapterProjectConfig>>,
    target_adapters: &IndexMap<AdapterType, ProfileAdapter>,
    default_adapter: AdapterType,
    top_level_quoting: Option<DbtQuoting>,
) -> IndexMap<AdapterType, DbtQuoting> {
    let top_level = top_level_quoting.unwrap_or_default();

    target_adapters
        .keys()
        .map(|adapter_type| {
            let own = adapters
                .and_then(|configured| configured.get(adapter_type))
                .and_then(|entry| entry.quoting)
                .unwrap_or_default();

            let layered = if *adapter_type == default_adapter {
                own.filled_from(&top_level)
            } else {
                own
            };
            (*adapter_type, layered)
        })
        .collect()
}
/// Normalizes hook key names in an unrendered config map, matching dbt-core's
/// `translate_hook_names` behavior (`context/context_config.py:235`):
/// `post_hook` → `post-hook`, `pre_hook` → `pre-hook`.
/// This applies to both project config and inline SQL config, since users may write
/// either spelling in `{{ config(post_hook=...) }}` calls.
pub(crate) fn normalize_hook_names(
    mut config: BTreeMap<String, dbt_yaml::Value>,
) -> BTreeMap<String, dbt_yaml::Value> {
    if let Some(v) = config.remove("post_hook") {
        config.insert("post-hook".to_string(), v);
    }
    if let Some(v) = config.remove("pre_hook") {
        config.insert("pre-hook".to_string(), v);
    }
    config
}

/// Extracts the `config:` subtree from a `dbt_yaml::Value` node into a flat
/// `BTreeMap`, returning `None` if the key is absent or not a mapping.
pub(crate) fn extract_config_map(
    value: &dbt_yaml::Value,
) -> Option<BTreeMap<String, dbt_yaml::Value>> {
    value
        .get("config")
        .and_then(|v| v.as_mapping())
        .map(|mapping| {
            mapping
                .iter()
                .filter_map(|(k, v)| k.as_str().map(|k| (k.to_string(), v.clone())))
                .collect()
        })
}

/// Coerces a hook config value into its flat list of entries: a sequence's items as-is, or a
/// single non-sequence value (e.g. a lone hook string) as a one-element list.
fn hook_value_entries(value: dbt_yaml::Value) -> Vec<dbt_yaml::Value> {
    match value {
        dbt_yaml::Value::Sequence(seq, _) => seq,
        other => vec![other],
    }
}

/// Combines two hook values into one, concatenating their entries (`existing` first). Hooks
/// accumulate across config sources rather than the later source replacing the earlier one —
/// matching the same "extend, don't replace" semantics [`default_hooks`] already applies on the
/// rendered-config path, so `unrendered_config` reflects every configured hook, not just the
/// most specific source's.
fn merge_hook_values(existing: dbt_yaml::Value, incoming: dbt_yaml::Value) -> dbt_yaml::Value {
    let mut entries = hook_value_entries(existing);
    entries.extend(hook_value_entries(incoming));
    dbt_yaml::Value::Sequence(entries, Default::default())
}

/// Merges `source`'s keys into `unrendered`. Ordinary keys overwrite (most specific source
/// wins); `pre-hook`/`post-hook` accumulate instead, since a resource can configure hooks at
/// more than one level (e.g. a schema.yml `post_hook` plus an inline SQL
/// `{{ config(post_hook=...) }}`) and dbt expects to run all of them, not just the last one
/// configured.
fn merge_config_source(
    unrendered: &mut BTreeMap<String, dbt_yaml::Value>,
    source: BTreeMap<String, dbt_yaml::Value>,
    normalize_hooks: bool,
) {
    for (key, value) in source {
        if normalize_hooks && (key == "pre-hook" || key == "post-hook") {
            if let Some(existing) = unrendered.remove(&key) {
                unrendered.insert(key, merge_hook_values(existing, value));
                continue;
            }
        }
        unrendered.insert(key, value);
    }
}

/// Deep-merges `source` into `destination`, transcribing `deep_merge_item` in dbt_common
/// `utils/dict.py:69-79`: mappings recurse, sequences concatenate with `source` first, everything
/// else `source` replaces. Spans come from whichever side supplied the value.
///
/// Where dbt-core raises or splats a string into characters, `source` wins instead — except a
/// sequence `source` over a scalar `destination`, which keeps the scalar as a one-element tail.
pub(crate) fn deep_merge_yaml(destination: &mut dbt_yaml::Value, source: &dbt_yaml::Value) {
    match source {
        dbt_yaml::Value::Mapping(source_map, _) if destination.is_mapping() => {
            let destination_map = destination
                .as_mapping_mut()
                .expect("destination is a mapping");
            for (key, source_value) in source_map.iter() {
                match destination_map.get_mut(key) {
                    Some(destination_value) => deep_merge_yaml(destination_value, source_value),
                    None => {
                        destination_map.insert(key.clone(), source_value.clone());
                    }
                }
            }
        }
        dbt_yaml::Value::Sequence(source_items, _) => {
            let mut merged = source_items.clone();
            match destination {
                dbt_yaml::Value::Sequence(destination_items, _) => {
                    merged.append(destination_items);
                    *destination_items = merged;
                }
                other => {
                    merged.push(other.clone());
                    *other = dbt_yaml::Value::Sequence(merged, source.span().clone());
                }
            }
        }
        _ => *destination = source.clone(),
    }
}

/// Applies `adapter_type`'s config-key alias map to one config source.
/// (`core/dbt/utils/utils.py:185-192`)
pub(crate) fn canonicalize_source_config_keys(
    adapter_type: AdapterType,
    cfg: BTreeMap<String, dbt_yaml::Value>,
) -> FsResult<BTreeMap<String, dbt_yaml::Value>> {
    dbt_adapter_core::config_aliases::canonicalize_config_keys(adapter_type, cfg).map_err(|dup| {
        let location = dbt_common::CodeLocationWithFile::from(dup.value_b.span().clone());
        fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => location,
            "Config keys `{}` and `{}` both resolve to `{}` for adapter '{}'; a project cannot \
             set the same underlying config key two different ways in the same place.",
            dup.key_a,
            dup.key_b,
            dup.canonical,
            adapter_type,
        )
    })
}

/// Builds `unrendered_config` by merging config sources in hierarchical order:
/// project < root < schema.yml < inline. Each source is canonicalized independently, before merging.
/// Ordinary keys use overwrite semantics (most specific source wins);
/// `pre-hook`/`post-hook` accumulate across sources instead (see [`merge_config_source`]).
///
/// Sources not applicable to a resource type should be passed as `None`.
/// `normalize_hooks` should be `true` only for resource types that support
/// `pre_hook`/`post_hook` (models, seeds, snapshots, tests).
pub(crate) fn build_unrendered_config(
    fqn: &[String],
    local: &crate::utils::RawProjectConfig,
    root: Option<&crate::utils::RawProjectConfig>,
    schema: Option<&BTreeMap<String, dbt_yaml::Value>>,
    inline: Option<&BTreeMap<String, dbt_yaml::Value>>,
    normalize_hooks: bool,
    adapter_type: AdapterType,
) -> FsResult<BTreeMap<String, dbt_yaml::Value>> {
    let canonicalize =
        |cfg: BTreeMap<String, dbt_yaml::Value>| -> FsResult<BTreeMap<String, dbt_yaml::Value>> {
            let cfg = canonicalize_source_config_keys(adapter_type, cfg)?;
            Ok(if normalize_hooks {
                normalize_hook_names(cfg)
            } else {
                cfg
            })
        };

    let mut unrendered = canonicalize(local.get_config_for_fqn(fqn).clone())?;

    if let Some(root_cfg) = root {
        merge_config_source(
            &mut unrendered,
            canonicalize(root_cfg.get_config_for_fqn(fqn).clone())?,
            normalize_hooks,
        );
    }
    if let Some(schema_cfg) = schema {
        merge_config_source(
            &mut unrendered,
            canonicalize(schema_cfg.clone())?,
            normalize_hooks,
        );
    }
    if let Some(inline_cfg) = inline {
        merge_config_source(
            &mut unrendered,
            canonicalize(inline_cfg.clone())?,
            normalize_hooks,
        );
    }

    Ok(unrendered)
}

/// Returns an error for resource names derived from filenames that contain spaces.
/// dbt does not allow spaces in resource names — this mirrors dbt-core's
/// `check_for_spaces_in_resource_names` validation.
pub(crate) fn err_resource_name_has_spaces(name: &str, path: &Path) -> Box<FsError> {
    fs_err!(
        code => ErrorCode::DbtYamlValidationError,
        loc => path.to_path_buf(),
        "Resource name '{}' contains spaces. Resource names cannot contain spaces. \
         Rename '{}' to remove any spaces.",
        name,
        path.display()
    )
}

/// Validates the merged `compute` config on a model / data_test / snapshot node. Currently
/// only `Remote` is supported for those node types; other variants are rejected at parse time
/// rather than mid-build. The set of accepted values will widen as local-compute support
/// for additional node types stabilizes. Unit test nodes use [`validate_unit_test_compute`]
/// instead.
pub(crate) fn validate_compute(compute: Option<ComputeArg>, path: &Path) -> FsResult<()> {
    match compute {
        None | Some(ComputeArg::Remote) => Ok(()),
        Some(other) => Err(fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => path.to_path_buf(),
            "compute config currently only accepts 'remote'; got '{other}'",
        )),
    }
}

/// Resolves and validates a node's `+adapter` selection at parse time.
///
/// Returns the selected [`AdapterType`], so the run layer can pick an execution
/// path without needing the profile. `None` in, `None` out: a node that selects
/// no adapter uses the target's default and is unconstrained.
///
/// Deliberately does **not** check that the target declares the selected adapter.
/// A project may carry `+adapter: bigquery` and be run against a Snowflake-only
/// target, so long as selection excludes those nodes -- and parse cannot know what
/// selection will do. That check lives after scheduling, where the set of nodes
/// that will actually execute is known; see
/// `check_scheduled_adapters_are_declared`.
///
/// What is checked here is only what no selection can rescue: an `alt`-typed
/// selection must satisfy the v1 preconditions:
///
/// 1. catalogs v2 must be enabled and the node must resolve a `catalog_name`
///    (the compute target reads its inputs and writes its output through an
///    attached catalog);
/// 2. the default adapter must be one of the v1-supported warehouses
///    (`snowflake`, or `duckdb`/`alt` for the standalone/dev case);
/// 3. the materialization must be one that runs natively — `table`, `view`, or
///    `incremental` — or a custom (user-authored) materialization; the managed
///    materializations that are out of v1 scope (`snapshot`, `materialized_view`,
///    `dynamic_table`, `streaming_table`) are rejected;
/// 4. Python models are not supported in v1.
///
/// Rule 3 is what keeps `alt` off the node types it cannot materialize: a
/// snapshot arrives with `DbtMaterialization::Snapshot` and a function with
/// `Function`, so an `alt` selection on either is rejected here rather than
/// failing at run time. Data tests inherit their adapter from the node they are
/// attached to instead, and never inherit `alt` (see `resolve_data_tests`).
///
/// The upstream-reachability check (every `ref`/`source` input must be available
/// through a reachable catalog) is enforced later, at DAG build, where the
/// upstream materializations are known.
#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_node_adapter(
    adapter: Option<AdapterType>,
    default_adapter: AdapterType,
    materialized: &DbtMaterialization,
    catalog_name: Option<&str>,
    adapter_type: AdapterType,
    use_catalogs_v2: bool,
    is_python: bool,
    path: &Path,
) -> FsResult<Option<AdapterType>> {
    let err = |msg: String| -> Box<FsError> {
        fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => path.to_path_buf(),
            "{msg}",
        )
    };

    let Some(selected_type) = adapter else {
        return Ok(None);
    };

    // Selecting the default adapter explicitly is a no-op, and always allowed.
    if selected_type == default_adapter {
        return Ok(Some(selected_type));
    }

    // Selecting any declared adapter is allowed -- the primitive is that several
    // adapters are supported. What follows are `alt`'s own preconditions, not an
    // allowlist of selectable adapters, so a non-`alt` selection passes straight
    // through.
    if selected_type != AdapterType::Alt {
        return Ok(Some(selected_type));
    }
    // The external name (`lake_compute`), so diagnostics quote what the author wrote.
    let name = selected_type.as_ref();

    // Rule 4: Python models are not supported.
    if is_python {
        return Err(err(format!(
            "adapter: '{name}' does not support Python models in v1"
        )));
    }

    // Rule 2: v1 warehouse guard.
    if !matches!(
        adapter_type,
        AdapterType::Snowflake | AdapterType::DuckDB | AdapterType::Alt
    ) {
        return Err(err(format!(
            "adapter: '{name}' in v1 supports Snowflake and lake compute only;              the target's default adapter is '{adapter_type}'"
        )));
    }

    // Rule 1: catalogs v2 + a resolvable catalog_name.
    if !use_catalogs_v2 {
        return Err(err(format!(
            "adapter: '{name}' requires catalogs v2              (set the 'use_catalogs_v2' flag)"
        )));
    }
    if catalog_name.is_none() {
        return Err(err(format!(
            "adapter: '{name}' requires a 'catalog_name' that resolves              to an attachable catalog"
        )));
    }

    // Rule 3: materialization must run natively or be a custom materialization.
    match materialized {
        DbtMaterialization::Table
        | DbtMaterialization::View
        | DbtMaterialization::Incremental
        // A custom (user-authored) materialization; enforced against the run path.
        | DbtMaterialization::Unknown(_) => {}
        other => {
            return Err(err(format!(
                "adapter: '{name}' supports table, view, and incremental                  materializations in v1; got '{other}'"
            )));
        }
    }

    Ok(Some(selected_type))
}

/// Unit tests can run on either on the `remote` warehouse or `sidecar`
pub(crate) fn validate_unit_test_compute(compute: Option<ComputeArg>, path: &Path) -> FsResult<()> {
    match compute {
        None | Some(ComputeArg::Remote) | Some(ComputeArg::Sidecar) => Ok(()),
        Some(other) => Err(fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => path.to_path_buf(),
            "unit_test compute config accepts 'remote', 'sidecar', or 'local'; got '{other}'",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::RawProjectConfig;

    /// Validate a selection of the `alt` adapter.
    fn validate_alt(
        materialized: DbtMaterialization,
        catalog_name: Option<&str>,
        adapter_type: AdapterType,
        use_catalogs_v2: bool,
        is_python: bool,
    ) -> FsResult<Option<AdapterType>> {
        validate_node_adapter(
            Some(AdapterType::Alt),
            AdapterType::Snowflake,
            &materialized,
            catalog_name,
            adapter_type,
            use_catalogs_v2,
            is_python,
            Path::new("models/m.sql"),
        )
    }

    fn validate_selection(selected: Option<AdapterType>) -> FsResult<Option<AdapterType>> {
        validate_node_adapter(
            selected,
            AdapterType::Snowflake,
            &DbtMaterialization::Table,
            Some("horizon"),
            AdapterType::Snowflake,
            true,
            false,
            Path::new("models/m.sql"),
        )
    }

    #[test]
    fn no_selection_is_always_accepted_and_resolves_to_none() {
        // An absent selection ignores every other precondition.
        assert_eq!(
            validate_node_adapter(
                None,
                AdapterType::Snowflake,
                &DbtMaterialization::MaterializedView,
                None,
                AdapterType::Bigquery,
                false,
                true,
                Path::new("models/m.sql"),
            )
            .unwrap(),
            None
        );
    }

    /// Naming the default adapter explicitly is a no-op, and skips the alt
    /// preconditions entirely.
    #[test]
    fn selecting_the_default_adapter_is_accepted() {
        let resolved = validate_selection(Some(AdapterType::Snowflake))
            .unwrap()
            .unwrap();
        assert_eq!(resolved, AdapterType::Snowflake);
    }

    // A value that is not an adapter type at all cannot reach here: `+adapter` is
    // typed `AdapterType`, so it is rejected at deserialization against the full
    // set of supported adapters. Covered by `seed_config` and `dbt_project` tests.

    /// An adapter the active target does not declare is **accepted** here. Parse
    /// cannot know whether the node will be selected, and a project spanning
    /// adapters run against a narrower target is legitimate so long as selection
    /// excludes the nodes needing the missing one. The error belongs after
    /// scheduling -- see `check_scheduled_adapters_are_declared`.
    #[test]
    fn an_undeclared_adapter_is_accepted_at_parse() {
        let resolved = validate_selection(Some(AdapterType::Redshift))
            .expect("membership is not parse's question to answer");
        assert_eq!(resolved, Some(AdapterType::Redshift));
    }

    /// Selecting any declared adapter is allowed -- the primitive is that several
    /// adapters are supported, so there is no allowlist of selectable types. The
    /// `alt` preconditions that follow gate on the *selected* adapter being `alt`,
    /// so a DuckDB selection skips them entirely.
    #[test]
    fn selecting_a_declared_non_alt_adapter_is_accepted() {
        let resolved = validate_selection(Some(AdapterType::DuckDB))
            .unwrap()
            .unwrap();
        assert_eq!(resolved, AdapterType::DuckDB);
    }

    #[test]
    fn a_valid_alt_selection_resolves_to_its_name_and_type() {
        let resolved = validate_alt(
            DbtMaterialization::Table,
            Some("horizon"),
            AdapterType::Snowflake,
            true,
            false,
        )
        .unwrap()
        .unwrap();

        assert_eq!(resolved, AdapterType::Alt);
    }

    #[test]
    fn alt_happy_paths() {
        for adapter in [
            AdapterType::Snowflake,
            AdapterType::DuckDB,
            AdapterType::Alt,
        ] {
            assert!(
                validate_alt(
                    DbtMaterialization::Table,
                    Some("horizon"),
                    adapter,
                    true,
                    false
                )
                .is_ok()
            );
        }
        // view + incremental + a custom materialization are all accepted.
        assert!(
            validate_alt(
                DbtMaterialization::View,
                Some("horizon"),
                AdapterType::Snowflake,
                true,
                false
            )
            .is_ok()
        );
        assert!(
            validate_alt(
                DbtMaterialization::Incremental,
                Some("horizon"),
                AdapterType::Snowflake,
                true,
                false
            )
            .is_ok()
        );
        assert!(
            validate_alt(
                DbtMaterialization::Unknown("my_custom_mat".to_string()),
                Some("horizon"),
                AdapterType::Snowflake,
                true,
                false
            )
            .is_ok()
        );
    }

    #[test]
    fn alt_rejects_python_models() {
        assert!(
            validate_alt(
                DbtMaterialization::Table,
                Some("horizon"),
                AdapterType::Snowflake,
                true,
                true
            )
            .is_err()
        );
    }

    #[test]
    fn alt_rejects_unsupported_warehouse() {
        assert!(
            validate_alt(
                DbtMaterialization::Table,
                Some("horizon"),
                AdapterType::Bigquery,
                true,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn alt_requires_catalogs_v2_and_catalog_name() {
        assert!(
            validate_alt(
                DbtMaterialization::Table,
                Some("horizon"),
                AdapterType::Snowflake,
                false,
                false
            )
            .is_err()
        );
        assert!(
            validate_alt(
                DbtMaterialization::Table,
                None,
                AdapterType::Snowflake,
                true,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn alt_rejects_out_of_scope_materializations() {
        for mat in [
            DbtMaterialization::Snapshot,
            DbtMaterialization::MaterializedView,
            DbtMaterialization::DynamicTable,
            DbtMaterialization::StreamingTable,
        ] {
            assert!(
                validate_alt(mat, Some("horizon"), AdapterType::Snowflake, true, false).is_err()
            );
        }
    }

    /// Rule 3 is what keeps `alt` off the node types it cannot materialize, so
    /// extending `+adapter` to them needed no new gate: a snapshot arrives with
    /// `Snapshot` and a function with `Function`, and both land in the reject arm.
    #[test]
    fn alt_is_rejected_for_the_node_types_it_cannot_materialize() {
        for mat in [DbtMaterialization::Snapshot, DbtMaterialization::Function] {
            let err = validate_alt(
                mat.clone(),
                Some("horizon"),
                AdapterType::Snowflake,
                true,
                false,
            )
            .expect_err("alt does not materialize {mat} in v1");
            assert!(
                err.to_string().contains("table, view, and incremental"),
                "expected the materialization diagnostic for {mat}, got: {err}"
            );
        }
    }

    /// The same selection is accepted for every node type when the adapter is a
    /// plain warehouse -- the `alt` preconditions are `alt`'s, not an allowlist of
    /// which node types may select at all.
    #[test]
    fn a_non_alt_selection_is_accepted_for_every_node_type() {
        for mat in [
            DbtMaterialization::Table,
            DbtMaterialization::Snapshot,
            DbtMaterialization::Test,
            DbtMaterialization::Function,
        ] {
            assert_eq!(
                validate_node_adapter(
                    Some(AdapterType::DuckDB),
                    AdapterType::Snowflake,
                    &mat,
                    None,
                    AdapterType::Snowflake,
                    true,
                    false,
                    Path::new("models/m.sql"),
                )
                .unwrap(),
                Some(AdapterType::DuckDB),
                "a duckdb selection should be accepted for {mat}"
            );
        }
    }

    /// Likewise for every node type. What no selection can rescue -- `alt`'s
    /// preconditions -- is still checked at parse, which is the distinction the two
    /// severities turn on.
    #[test]
    fn an_undeclared_adapter_is_accepted_for_every_node_type() {
        for mat in [
            DbtMaterialization::Snapshot,
            DbtMaterialization::Test,
            DbtMaterialization::Function,
        ] {
            let resolved = validate_node_adapter(
                Some(AdapterType::Redshift),
                AdapterType::Snowflake,
                &mat,
                None,
                AdapterType::Snowflake,
                true,
                false,
                Path::new("models/m.sql"),
            )
            .unwrap_or_else(|e| panic!("membership is not parse's question for {mat}: {e}"));
            assert_eq!(resolved, Some(AdapterType::Redshift));
        }
    }

    fn config_map(pairs: &[(&str, &str)]) -> BTreeMap<String, dbt_yaml::Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), dbt_yaml::from_str(v).unwrap()))
            .collect()
    }

    fn hook_strings(value: &dbt_yaml::Value) -> Vec<String> {
        match value {
            dbt_yaml::Value::Sequence(seq, _) => seq
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect(),
            dbt_yaml::Value::String(s, _) => vec![s.clone()],
            other => panic!("expected a hook string or sequence, got {other:?}"),
        }
    }

    #[test]
    fn hooks_accumulate_across_config_sources_instead_of_overwriting() {
        // A resource can configure a `post_hook` at both the schema.yml level and the inline
        // SQL `{{ config(...) }}` level. Both should run (matching the rendered-config path's
        // `default_hooks`, which already extends rather than replaces), so `unrendered_config`
        // must reflect both, not just the more specific (inline) source.
        let local = RawProjectConfig::empty();
        let schema = config_map(&[("post_hook", "\"apply masking\"")]);
        let inline = config_map(&[("post_hook", "\"delete rows\"")]);

        let unrendered = build_unrendered_config(
            &[],
            &local,
            None,
            Some(&schema),
            Some(&inline),
            true,
            AdapterType::Snowflake,
        )
        .unwrap();

        let post_hook = unrendered.get("post-hook").expect("expected post-hook key");
        assert_eq!(
            hook_strings(post_hook),
            vec!["apply masking".to_string(), "delete rows".to_string()]
        );
    }

    #[test]
    fn non_hook_keys_still_use_overwrite_semantics() {
        // Ordinary (non-hook) keys keep "most specific source wins" — only hooks accumulate.
        let local = RawProjectConfig::empty();
        let schema = config_map(&[("materialized", "\"view\"")]);
        let inline = config_map(&[("materialized", "\"table\"")]);

        let unrendered = build_unrendered_config(
            &[],
            &local,
            None,
            Some(&schema),
            Some(&inline),
            true,
            AdapterType::Snowflake,
        )
        .unwrap();

        assert_eq!(
            unrendered.get("materialized").and_then(|v| v.as_str()),
            Some("table")
        );
    }

    #[test]
    fn single_source_hook_is_not_wrapped_unnecessarily() {
        // When only one source configures a hook, no merge occurs -- confirms the accumulation
        // path doesn't kick in (and doesn't panic) when there's nothing to merge with.
        let local = RawProjectConfig::empty();
        let inline = config_map(&[("post_hook", "\"delete rows\"")]);

        let unrendered = build_unrendered_config(
            &[],
            &local,
            None,
            None,
            Some(&inline),
            true,
            AdapterType::Snowflake,
        )
        .unwrap();

        assert_eq!(
            unrendered.get("post-hook").and_then(|v| v.as_str()),
            Some("delete rows")
        );
    }

    #[test]
    fn hook_accumulation_does_not_apply_when_normalize_hooks_is_false() {
        // `normalize_hooks=false` is used for resource types that don't support hooks (e.g.
        // exposures); a `post_hook`-named key there is just an ordinary key, not special-cased.
        let local = RawProjectConfig::empty();
        let schema = config_map(&[("post_hook", "\"a\"")]);
        let inline = config_map(&[("post_hook", "\"b\"")]);

        let unrendered = build_unrendered_config(
            &[],
            &local,
            None,
            Some(&schema),
            Some(&inline),
            false,
            AdapterType::Snowflake,
        )
        .unwrap();

        assert_eq!(
            unrendered.get("post_hook").and_then(|v| v.as_str()),
            Some("b")
        );
        assert!(!unrendered.contains_key("post-hook"));
    }

    fn raw_project_config(pairs: &[(&str, &str)]) -> RawProjectConfig {
        RawProjectConfig {
            config: config_map(pairs),
            children: BTreeMap::new(),
        }
    }

    #[test]
    fn databricks_catalog_alias_is_canonicalized_to_database() {
        let local = raw_project_config(&[("catalog", "\"my_catalog\"")]);

        let unrendered =
            build_unrendered_config(&[], &local, None, None, None, true, AdapterType::Databricks)
                .unwrap();

        assert_eq!(
            unrendered.get("database").and_then(|v| v.as_str()),
            Some("my_catalog")
        );
        assert!(!unrendered.contains_key("catalog"));
    }

    /// The alias map is gated on adapter type, so the same `+catalog:` is an inert extra
    /// config key on an adapter with no such alias.
    #[test]
    fn catalog_key_is_untouched_on_an_adapter_with_no_alias_map() {
        let local = raw_project_config(&[("catalog", "\"my_catalog\"")]);

        let unrendered =
            build_unrendered_config(&[], &local, None, None, None, true, AdapterType::Snowflake)
                .unwrap();

        assert_eq!(
            unrendered.get("catalog").and_then(|v| v.as_str()),
            Some("my_catalog")
        );
        assert!(!unrendered.contains_key("database"));
    }

    /// `catalog_name` is a distinct, real config with its own dbt-core counterpart and must
    /// never be swept up by the `catalog` → `database` alias.
    #[test]
    fn catalog_name_is_never_aliased() {
        for adapter_type in [AdapterType::Databricks, AdapterType::Snowflake] {
            let local = raw_project_config(&[("catalog_name", "\"cat\"")]);

            let unrendered =
                build_unrendered_config(&[], &local, None, None, None, true, adapter_type).unwrap();

            assert_eq!(
                unrendered.get("catalog_name").and_then(|v| v.as_str()),
                Some("cat"),
                "{adapter_type:?}"
            );
        }
    }

    /// Two keys in the same config source resolving to the same canonical key is an error,
    /// mirroring dbt-core's `DuplicateAliasError`.
    #[test]
    fn duplicate_alias_and_canonical_key_in_one_source_errors() {
        let local = raw_project_config(&[("catalog", "\"a\""), ("database", "\"b\"")]);

        let err =
            build_unrendered_config(&[], &local, None, None, None, true, AdapterType::Databricks)
                .expect_err(
                    "catalog and database both resolve to database and must not silently pick one",
                );

        let message = err.to_string();
        assert!(message.contains("catalog"), "{message}");
        assert!(message.contains("database"), "{message}");
    }

    /// Canonicalization runs per source, before layering, so a less specific source's
    /// alias spelling does not shadow ordinary precedence -- a model-level `database:` still
    /// wins over a project-level `+catalog:`.
    #[test]
    fn model_level_canonical_key_wins_over_project_level_alias() {
        let local = raw_project_config(&[("catalog", "\"project_catalog\"")]);
        let inline = config_map(&[("database", "\"model_database\"")]);

        let unrendered = build_unrendered_config(
            &[],
            &local,
            None,
            None,
            Some(&inline),
            true,
            AdapterType::Databricks,
        )
        .unwrap();

        assert_eq!(
            unrendered.get("database").and_then(|v| v.as_str()),
            Some("model_database")
        );
        assert!(!unrendered.contains_key("catalog"));
    }

    /// https://github.com/dbt-labs/fs/pull/13752#discussion_r3872848766 -- a `+catalog:` alias
    /// at a parent `dbt_project.yml` subtree level and a `+database:` canonical spelling at a
    /// nested child level. dbt-mantle (`078260e46`, `context/context_config.py:120-127,222`,
    /// `utils/utils.py:258` `fqn_search`) translates each hierarchy level's own dict through
    /// `translate_aliases` *before* folding it into the accumulating typed result one level at a
    /// time, so this is ordinary override precedence (child's `database` wins) and never a
    /// `DuplicateAliasError` -- that error only fires when the *same* level dict has two
    /// colliding keys.
    ///
    /// `RawProjectConfig` (`crate::utils::recur_raw_project_config`) used to pre-merge every
    /// `dbt_project.yml` subtree level into one raw dict via plain key overwrite
    /// (`merge_raw_config_mappings`), before `build_unrendered_config` ever canonicalized
    /// anything. So the leaf's merged dict carried both `catalog` (from the parent level) and
    /// `database` (from this level) as two distinct keys, and canonicalizing that combined dict
    /// as if it were one config source raised a spurious `DuplicateAliasKey` -- exactly the
    /// "duplicate issue" the review comment predicted, and not one of D3's documented axes
    /// (cross-*source* canonicalize-before-merge, not cross-*level* within one source).
    /// `merge_raw_config_mappings` now canonicalizes each level's own keys before folding them
    /// into the (already-canonical) accumulated parent, closing that gap.
    #[test]
    fn alias_at_parent_level_and_canonical_key_at_child_level_does_not_error() {
        let mapping = yaml(
            r#"
my_project:
  "+catalog": parent_catalog
  staging:
    "+database": staging_database
"#,
        );
        let tree = crate::utils::recur_raw_project_config(
            mapping.as_mapping().unwrap(),
            &BTreeMap::new(),
            AdapterType::Databricks,
        )
        .expect(
            "dbt-core translates per dbt_project.yml level before merging, so a parent-level \
             alias and a child-level canonical spelling never collide",
        );

        let fqn = vec!["my_project".to_string(), "staging".to_string()];

        // Each level is canonicalized before merging, so the leaf's merged dict already carries
        // only the canonical spelling -- the parent's `catalog` was renamed to `database` before
        // the child's own `database` overwrote it, never surviving as a second distinct key.
        let merged = tree.get_config_for_fqn(&fqn);
        assert_eq!(
            merged.get("database").and_then(|v| v.as_str()),
            Some("staging_database")
        );
        assert!(!merged.contains_key("catalog"));

        let unrendered =
            build_unrendered_config(&fqn, &tree, None, None, None, true, AdapterType::Databricks)
                .unwrap();

        assert_eq!(
            unrendered.get("database").and_then(|v| v.as_str()),
            Some("staging_database")
        );
        assert!(!unrendered.contains_key("catalog"));
    }

    /// A genuine same-level duplicate (one `dbt_project.yml` subtree writes both spellings at
    /// once) must still error -- the per-level canonicalization the fix above adds must check at
    /// the same granularity dbt-core does, not disable the check entirely.
    #[test]
    fn alias_and_canonical_key_in_the_same_dbt_project_level_still_errors() {
        let mapping = yaml(
            r#"
my_project:
  "+catalog": a
  "+database": b
"#,
        );

        let err = crate::utils::recur_raw_project_config(
            mapping.as_mapping().unwrap(),
            &BTreeMap::new(),
            AdapterType::Databricks,
        )
        .expect_err("catalog and database both resolve to database at the same level");

        let message = err.to_string();
        assert!(message.contains("catalog"), "{message}");
        assert!(message.contains("database"), "{message}");
    }

    fn yaml(text: &str) -> dbt_yaml::Value {
        dbt_yaml::from_str(text).unwrap()
    }

    /// Deep-merges two YAML documents given as source text. Parsing rather than hand-constructing
    /// the values keeps the spans real.
    fn deep_merged(destination: &str, source: &str) -> dbt_yaml::Value {
        let mut destination = yaml(destination);
        deep_merge_yaml(&mut destination, &yaml(source));
        destination
    }

    #[test]
    fn deep_merge_recurses_into_nested_mappings() {
        // A mapping set at both levels merges key-wise instead of the source replacing it.
        assert_eq!(
            deep_merged(
                "persist_docs:\n  relation: true\n",
                "persist_docs:\n  columns: true\n",
            ),
            yaml("persist_docs:\n  relation: true\n  columns: true\n"),
        );
    }

    #[test]
    fn deep_merge_concatenates_sequences_with_source_first() {
        // `list(source) + list(destination)`: source items first. Not a union, not a replace.
        assert_eq!(
            deep_merged("tags: [model_tag]\n", "tags: [version_tag]\n"),
            yaml("tags: [version_tag, model_tag]\n"),
        );
    }

    #[test]
    fn deep_merge_replaces_other_values_with_source() {
        // Note the asymmetry: a scalar source over a sequence destination replaces, not appends.
        assert_eq!(
            deep_merged("alias: model_alias\n", "alias: version_alias\n"),
            yaml("alias: version_alias\n"),
        );
        assert_eq!(
            deep_merged("tags: [model_tag]\n", "tags: version_tag\n"),
            yaml("tags: version_tag\n"),
        );
    }

    #[test]
    fn deep_merge_handles_keys_present_on_only_one_side() {
        // Source-only keys are added; destination-only keys survive untouched.
        assert_eq!(
            deep_merged("alias: model_alias\n", "materialized: table\n"),
            yaml("alias: model_alias\nmaterialized: table\n"),
        );
        // ... including when the destination has no sub-mapping to recurse into.
        assert_eq!(
            deep_merged("alias: model_alias\n", "persist_docs:\n  columns: true\n"),
            yaml("alias: model_alias\npersist_docs:\n  columns: true\n"),
        );
    }

    #[test]
    fn deep_merge_lets_source_win_over_a_type_mismatch() {
        // dbt-core raises on mapping-over-scalar; we let the source win in both directions.
        assert_eq!(
            deep_merged("persist_docs: true\n", "persist_docs:\n  columns: true\n"),
            yaml("persist_docs:\n  columns: true\n"),
        );
        assert_eq!(
            deep_merged("persist_docs:\n  relation: true\n", "persist_docs: false\n"),
            yaml("persist_docs: false\n"),
        );
    }

    #[test]
    fn deep_merge_coerces_a_scalar_destination_under_a_sequence_source() {
        // A lone hook string plus a hook list: keep both, source first.
        assert_eq!(
            deep_merged("pre-hook: model_hook\n", "pre-hook: [version_hook]\n"),
            yaml("pre-hook: [version_hook, model_hook]\n"),
        );
    }

    #[test]
    fn deep_merge_preserves_spans_from_the_supplying_side() {
        // Parse errors must still point at the line the user authored.
        let mut destination = yaml("alias: model_alias\npersist_docs:\n  relation: true\n");
        let source = yaml("persist_docs:\n  columns: true\ntags: [version_tag]\n");
        deep_merge_yaml(&mut destination, &source);

        let line_of = |path: &[&str]| {
            let mut value = &destination;
            for key in path {
                value = value.get(*key).expect("expected key to be present");
            }
            value.span().start.line
        };

        // Untouched destination values keep their line; source-supplied values carry the source's.
        assert_eq!(line_of(&["persist_docs", "relation"]), 3);
        assert_eq!(line_of(&["persist_docs", "columns"]), 2);
        assert_eq!(line_of(&["tags"]), 3);
        // The merged container keeps the destination's line (a mapping's span starts at its
        // first entry, hence 3 rather than the key's 2).
        assert_eq!(line_of(&["persist_docs"]), 3);
    }
}

#[cfg(test)]
mod adapter_quoting_tests {
    use super::*;

    fn quoting(database: bool, schema: bool, identifier: bool) -> DbtQuoting {
        DbtQuoting {
            database: Some(database),
            schema: Some(schema),
            identifier: Some(identifier),
            snowflake_ignore_case: None,
        }
    }

    fn adapters_fixture() -> IndexMap<AdapterType, ProfileAdapter> {
        use dbt_schemas::schemas::profiles::DbConfig;
        IndexMap::from(
            [
                DbConfig::Snowflake(
                    Box::<dbt_schemas::schemas::profiles::SnowflakeDbConfig>::default(),
                ),
                DbConfig::Alt(Box::<dbt_schemas::schemas::profiles::AltConfig>::default()),
            ]
            .map(|config| (config.adapter_type(), ProfileAdapter::single(config))),
        )
    }

    /// One `adapters:` entry, keyed by type.
    fn entry(
        adapter_type: AdapterType,
        quoting: Option<DbtQuoting>,
    ) -> IndexMap<AdapterType, AdapterProjectConfig> {
        IndexMap::from([(adapter_type, AdapterProjectConfig { quoting })])
    }

    /// The rule the chain exists to express: the top-level `quoting:` block
    /// configures the *default* adapter and nothing else. A node on `alt` gets
    /// only `alt`'s own entry, so it is free to differ without every model
    /// having to say so.
    #[test]
    fn top_level_quoting_reaches_only_the_default_adapter() {
        let top_level = Some(quoting(false, false, false));
        let adapters = entry(AdapterType::Alt, Some(quoting(true, true, true)));

        let per_adapter = authored_quoting_per_adapter(
            Some(&adapters),
            &adapters_fixture(),
            AdapterType::Snowflake,
            top_level,
        );

        assert_eq!(
            per_adapter[&AdapterType::Snowflake],
            quoting(false, false, false),
            "the default adapter takes the top-level block"
        );
        assert_eq!(
            per_adapter[&AdapterType::Alt],
            quoting(true, true, true),
            "a non-default adapter takes only its own entry"
        );
    }

    /// A declared adapter with no `adapters:` entry contributes nothing, so the
    /// node falls straight through to its adapter type's default. Without this
    /// the map would be missing the key and the layer would be skipped silently
    /// either way -- the test pins that they agree.
    #[test]
    fn a_declared_adapter_without_an_entry_contributes_nothing() {
        let per_adapter =
            authored_quoting_per_adapter(None, &adapters_fixture(), AdapterType::Snowflake, None);

        assert_eq!(per_adapter.len(), 2, "every declared adapter gets a key");
        assert_eq!(per_adapter[&AdapterType::Alt], DbtQuoting::default());
        assert_eq!(per_adapter[&AdapterType::Snowflake], DbtQuoting::default());
    }

    /// The default adapter may carry its own entry, which wins over the
    /// top-level block field-wise -- more specific, same file.
    #[test]
    fn the_default_adapters_own_entry_beats_the_top_level_block() {
        let top_level = Some(quoting(false, false, false));
        let adapters = entry(
            AdapterType::Snowflake,
            Some(DbtQuoting {
                identifier: Some(true),
                ..Default::default()
            }),
        );

        let per_adapter = authored_quoting_per_adapter(
            Some(&adapters),
            &adapters_fixture(),
            AdapterType::Snowflake,
            top_level,
        );

        let resolved = per_adapter[&AdapterType::Snowflake];
        assert_eq!(resolved.identifier, Some(true), "the entry wins");
        assert_eq!(
            resolved.database,
            Some(false),
            "fields the entry leaves unset still come from the top-level block"
        );
    }

    /// Only `snowflake_ignore_case` set on a layer must survive. `default_to`
    /// drops that field, which is why the layering uses `filled_from`.
    #[test]
    fn snowflake_ignore_case_survives_layering() {
        let adapters = entry(
            AdapterType::Alt,
            Some(DbtQuoting {
                snowflake_ignore_case: Some(true),
                ..Default::default()
            }),
        );

        let per_adapter = authored_quoting_per_adapter(
            Some(&adapters),
            &adapters_fixture(),
            AdapterType::Snowflake,
            None,
        );

        assert_eq!(
            per_adapter[&AdapterType::Alt].snowflake_ignore_case,
            Some(true)
        );
    }

    /// A target the project was not written for is a normal thing to run against,
    /// so an entry this target cannot use warns rather than failing. Duplicate
    /// entries need no test: the block is a map, so they cannot be expressed.
    #[test]
    fn an_entry_for_an_undeclared_adapter_is_accepted() {
        let adapters = entry(AdapterType::Redshift, Some(quoting(true, true, true)));
        validate_adapter_project_configs(Some(&adapters), &adapters_fixture());
    }
}
