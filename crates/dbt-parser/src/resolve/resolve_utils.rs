use std::collections::BTreeMap;
use std::path::Path;

use dbt_adapter_core::AdapterType;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::error::FsError;
use dbt_common::fs_err;
use dbt_common::io_args::ComputeArg;
use dbt_schemas::schemas::common::{ComputePlatform, DbtMaterialization};
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

/// Builds `unrendered_config` by merging config sources in hierarchical order:
/// project < root < schema.yml < inline. Each source is merged independently so
/// that hook key normalization (pre_hook → pre-hook, etc.) applies per-source
/// before merging. Ordinary keys use overwrite semantics (most specific source wins);
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
) -> BTreeMap<String, dbt_yaml::Value> {
    let apply = |cfg: BTreeMap<String, dbt_yaml::Value>| {
        if normalize_hooks {
            normalize_hook_names(cfg)
        } else {
            cfg
        }
    };

    let mut unrendered = apply(local.get_config_for_fqn(fqn).clone());

    if let Some(root_cfg) = root {
        merge_config_source(
            &mut unrendered,
            apply(root_cfg.get_config_for_fqn(fqn).clone()),
            normalize_hooks,
        );
    }
    if let Some(schema_cfg) = schema {
        merge_config_source(&mut unrendered, apply(schema_cfg.clone()), normalize_hooks);
    }
    if let Some(inline_cfg) = inline {
        merge_config_source(&mut unrendered, apply(inline_cfg.clone()), normalize_hooks);
    }

    unrendered
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

/// Validates a model's `alt_compute` config at parse time.
///
/// Only `alt_compute: alt` is constrained; `default` (or absent) is always
/// accepted. When set to `alt`, the node must satisfy the v1 preconditions:
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
/// The upstream-reachability check (every `ref`/`source` input must be available
/// through a reachable catalog) is enforced later, at DAG build, where the
/// upstream materializations are known.
#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_compute_platform(
    alt_compute: Option<ComputePlatform>,
    materialized: &DbtMaterialization,
    catalog_name: Option<&str>,
    adapter_type: AdapterType,
    use_catalogs_v2: bool,
    is_python: bool,
    path: &Path,
) -> FsResult<()> {
    // Only the `alt` compute target has preconditions; `default` is unconstrained.
    if alt_compute != Some(ComputePlatform::Alt) {
        return Ok(());
    }

    let err = |msg: String| -> Box<FsError> {
        fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => path.to_path_buf(),
            "{msg}",
        )
    };

    // Rule 4: Python models are not supported.
    if is_python {
        return Err(err(
            "alt_compute: 'alt' does not support Python models in v1".to_string(),
        ));
    }

    // Rule 2: v1 warehouse guard.
    if !matches!(
        adapter_type,
        AdapterType::Snowflake | AdapterType::DuckDB | AdapterType::Alt
    ) {
        return Err(err(format!(
            "alt_compute: 'alt' in v1 supports Snowflake and alt only; \
             the configured adapter is '{adapter_type}'"
        )));
    }

    // Rule 1: catalogs v2 + a resolvable catalog_name.
    if !use_catalogs_v2 {
        return Err(err(
            "alt_compute: 'alt' requires catalogs v2 (set the 'use_catalogs_v2' flag)".to_string(),
        ));
    }
    if catalog_name.is_none() {
        return Err(err(
            "alt_compute: 'alt' requires a 'catalog_name' that resolves to an attachable catalog"
                .to_string(),
        ));
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
                "alt_compute: 'alt' supports table, view, and incremental \
                 materializations in v1; got '{other}'"
            )));
        }
    }

    Ok(())
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

    /// Helper: run `validate_compute_platform` with `alt` placement and the
    /// given knobs, defaulting the valid-happy-path inputs.
    fn validate_alt(
        materialized: DbtMaterialization,
        catalog_name: Option<&str>,
        adapter_type: AdapterType,
        use_catalogs_v2: bool,
        is_python: bool,
    ) -> FsResult<()> {
        validate_compute_platform(
            Some(ComputePlatform::Alt),
            &materialized,
            catalog_name,
            adapter_type,
            use_catalogs_v2,
            is_python,
            Path::new("models/m.sql"),
        )
    }

    #[test]
    fn default_placement_is_always_accepted() {
        // `default` / absent placement ignores every other precondition.
        assert!(
            validate_compute_platform(
                None,
                &DbtMaterialization::MaterializedView,
                None,
                AdapterType::Bigquery,
                false,
                true,
                Path::new("models/m.sql"),
            )
            .is_ok()
        );
        assert!(
            validate_compute_platform(
                Some(ComputePlatform::Default),
                &DbtMaterialization::Snapshot,
                None,
                AdapterType::Bigquery,
                false,
                false,
                Path::new("models/m.sql"),
            )
            .is_ok()
        );
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

        let unrendered =
            build_unrendered_config(&[], &local, None, Some(&schema), Some(&inline), true);

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

        let unrendered =
            build_unrendered_config(&[], &local, None, Some(&schema), Some(&inline), true);

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

        let unrendered = build_unrendered_config(&[], &local, None, None, Some(&inline), true);

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

        let unrendered =
            build_unrendered_config(&[], &local, None, Some(&schema), Some(&inline), false);

        assert_eq!(
            unrendered.get("post_hook").and_then(|v| v.as_str()),
            Some("b")
        );
        assert!(!unrendered.contains_key("post-hook"));
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
