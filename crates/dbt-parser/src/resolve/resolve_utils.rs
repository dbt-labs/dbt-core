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

/// Builds the raw schema.yml config map for a model entry, merging the version-level
/// `config:` block (for versioned models) over the model-level one. This mirrors
/// dbt-core's versioned model patch construction, so per-version overrides (e.g.
/// `alias`) are recorded in `unrendered_config` and detected by `state:modified`.
pub(crate) fn extract_schema_yml_config_map(
    schema_value: &dbt_yaml::Value,
    version_config: Option<&dbt_yaml::Value>,
) -> Option<BTreeMap<String, dbt_yaml::Value>> {
    let mut config_map = extract_config_map(schema_value).unwrap_or_default();
    if let Some(mapping) = version_config.and_then(|v| v.as_mapping()) {
        config_map.extend(
            mapping
                .iter()
                .filter_map(|(k, v)| k.as_str().map(|k| (k.to_string(), v.clone()))),
        );
    }
    if config_map.is_empty() {
        None
    } else {
        Some(config_map)
    }
}

/// Builds `unrendered_config` by merging config sources in hierarchical order:
/// project < root < schema.yml < inline. Each source is merged independently so
/// that hook key normalization (pre_hook → pre-hook, etc.) applies per-source
/// before merging, ensuring correct overwrite semantics.
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
        unrendered.extend(apply(root_cfg.get_config_for_fqn(fqn).clone()));
    }
    if let Some(schema_cfg) = schema {
        unrendered.extend(apply(schema_cfg.clone()));
    }
    if let Some(inline_cfg) = inline {
        unrendered.extend(apply(inline_cfg.clone()));
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
            "unit_test compute config accepts 'remote' or 'sidecar'; got '{other}'",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Helper: parse a YAML snippet into a `dbt_yaml::Value` for the
    /// `extract_schema_yml_config_map` tests below.
    fn yaml(s: &str) -> dbt_yaml::Value {
        dbt_yaml::from_str(s).unwrap()
    }

    /// Regression test for dbt-labs/dbt-core#15707: a versioned model's
    /// version-level `config:` block (e.g. `alias`) must be recorded in the raw
    /// schema.yml config map feeding `unrendered_config`, otherwise
    /// `state:modified` cannot detect that the alias was removed or changed.
    #[test]
    fn version_level_alias_is_recorded_in_schema_yml_config() {
        let schema_value = yaml("name: y_v\nlatest_version: 1");
        let version_config = yaml("alias: aliased_y_v");
        let config_map =
            extract_schema_yml_config_map(&schema_value, Some(&version_config)).unwrap();
        assert_eq!(
            config_map.get("alias").and_then(|v| v.as_str()),
            Some("aliased_y_v")
        );
    }

    /// Removing the version-level alias (with no model-level `config:`) must
    /// yield no schema.yml config at all — the before (`alias` present) vs.
    /// after (absent) difference is what `state:modified` keys off.
    #[test]
    fn version_level_alias_removal_yields_no_schema_yml_config() {
        let schema_value = yaml("name: y_v\nlatest_version: 1");
        assert!(extract_schema_yml_config_map(&schema_value, None).is_none());
    }

    /// A version-level `config:` key overrides the model-level one, while
    /// non-conflicting model-level keys are preserved.
    #[test]
    fn version_level_config_overrides_model_level() {
        let schema_value = yaml("name: y_v\nconfig:\n  alias: model_alias\n  tags: [a]");
        let version_config = yaml("alias: version_alias");
        let config_map =
            extract_schema_yml_config_map(&schema_value, Some(&version_config)).unwrap();
        assert_eq!(
            config_map.get("alias").and_then(|v| v.as_str()),
            Some("version_alias")
        );
        assert!(config_map.contains_key("tags"));
    }

    /// Non-versioned models (no version-level config) keep the previous
    /// behavior: model-level `config:` is extracted as-is, and models without
    /// any `config:` block produce no entry.
    #[test]
    fn non_versioned_model_schema_yml_config_unchanged() {
        let schema_value = yaml("name: a\nconfig:\n  alias: aliased_a");
        let config_map = extract_schema_yml_config_map(&schema_value, None).unwrap();
        assert_eq!(
            config_map.get("alias").and_then(|v| v.as_str()),
            Some("aliased_a")
        );
        assert!(extract_schema_yml_config_map(&yaml("name: b"), None).is_none());
    }
}
