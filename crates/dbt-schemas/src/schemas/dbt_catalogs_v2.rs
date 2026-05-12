//! dbt_catalogs.yml v2 schema: three-phase borrowed validation pipeline.
//!
//! Canonical v2 YAML shape (strict keys):
//!
//! ```yaml
//! catalogs:
//!   # type: horizon
//!   # supported platforms: snowflake
//!   # a snowflake config block is required
//!   - name: sf_managed
//!     type: horizon
//!     table_format: iceberg
//!     config:
//!       snowflake:
//!         external_volume: <string>                               # required, non-empty
//!         change_tracking: <boolean>                              # optional
//!         data_retention_time_in_days: <u32 in 0..=90>            # optional
//!         max_data_extension_time_in_days: <u32 in 0..=90>        # optional
//!         storage_serialization_policy: COMPATIBLE|OPTIMIZED      # optional, case-insensitive
//!         iceberg_version: <u32>                                  # optional, e.g. 3 for Iceberg V3
//!         base_location_root: <path string>                       # optional, non-empty if present
//!
//!         # model config only; specifying this in catalogs.yml is invalid
//!         base_location_subpath: <string>                         # optional
//!
//!   # type: glue
//!   # supported platforms: snowflake, duckdb
//!   # at least one supported platform block is required
//!   - name: glue_catalog
//!     type: glue
//!     table_format: iceberg
//!     config:
//!       snowflake:
//!         catalog_database: <string>                              # required, non-empty
//!         auto_refresh: <boolean>                                 # optional
//!         max_data_extension_time_in_days: <u32 in 0..=90>        # optional
//!         target_file_size: AUTO|16MB|32MB|64MB|128MB             # optional
//!         iceberg_version: <u32>                                  # optional, e.g. 3 for Iceberg V3
//!       duckdb:
//!         endpoint|endpoint_type: <string>                        # exactly one required
//!         warehouse: <string>                                     # optional, non-empty if present
//!         secret: <string>                                        # optional DuckDB secret name from profiles.yml
//!         attach_as: <string>                                     # optional, non-empty if present
//!         default_region: <string>                                # optional, non-empty if present
//!         default_schema: <string>                                # optional, non-empty if present
//!         max_table_staleness: <string>                           # optional, non-empty if present
//!         authorization_type: OAUTH2|SIGV4|NONE                   # optional
//!         access_delegation_mode: VENDED_CREDENTIALS|NONE         # optional
//!         support_nested_namespaces: <boolean>                    # optional
//!         support_stage_create: <boolean>                         # optional
//!         purge_requested: <boolean>                              # optional
//!         encode_entire_prefix: <boolean>                         # optional
//!
//!   # type: unity
//!   # supported platforms: snowflake, databricks
//!   # at least one supported platform block is required
//!   - name: unity_catalog
//!     type: unity
//!     table_format: iceberg
//!     config:
//!       snowflake:
//!         catalog_database: <string>                              # required if snowflake block exists; non-empty
//!         auto_refresh: <boolean>                                 # optional
//!         max_data_extension_time_in_days: <u32 in 0..=90>        # optional
//!         target_file_size: AUTO|16MB|32MB|64MB|128MB             # optional
//!         iceberg_version: <u32>                                  # optional, e.g. 3 for Iceberg V3
//!       databricks:
//!         file_format: delta                                      # required if databricks block exists
//!         location_root: <path string>                            # optional, non-empty if present
//!         use_uniform: <boolean>                                  # optional, defaults to false
//!         # Both managed and UniForm paths currently use delta.
//!
//!   # type: hive_metastore
//!   # supported platforms: databricks
//!   # a databricks config block is required
//!   - name: hive_catalog
//!     type: hive_metastore
//!     table_format: default
//!     config:
//!       databricks:
//!         file_format: delta|parquet|hudi                         # required
//!
//!   # type: biglake_metastore
//!   # supported platforms: bigquery
//!   - name: biglake_catalog
//!     type: biglake_metastore
//!     table_format: iceberg
//!     config:
//!       bigquery:
//!         external_volume: gs://<bucket_name>                     # required, non-empty
//!         file_format: parquet                                    # required
//!         base_location_root: <path string>                       # optional, non-empty if present
//!
//!   # type: iceberg_rest
//!   # supported platforms: snowflake, duckdb
//!   # at least one supported platform block is required
//!   - name: rest_catalog
//!     type: iceberg_rest
//!     table_format: iceberg
//!     config:
//!       snowflake:
//!         catalog_database: <string>                              # required if snowflake block exists; non-empty
//!         auto_refresh: <boolean>                                 # optional
//!         max_data_extension_time_in_days: <u32 in 0..=90>        # optional
//!         target_file_size: AUTO|16MB|32MB|64MB|128MB             # optional
//!       duckdb:
//!         endpoint|endpoint_type: <string>                        # exactly one required
//!         warehouse: <string>                                     # optional, non-empty if present
//!         secret: <string>                                        # optional DuckDB secret name from profiles.yml
//!         attach_as: <string>                                     # optional, non-empty if present
//!         default_region: <string>                                # optional, non-empty if present
//!         default_schema: <string>                                # optional, non-empty if present
//!         max_table_staleness: <string>                           # optional, non-empty if present
//!         authorization_type: OAUTH2|SIGV4|NONE                   # optional
//!         access_delegation_mode: VENDED_CREDENTIALS|NONE         # optional
//!         support_nested_namespaces: <boolean>                    # optional
//!         support_stage_create: <boolean>                         # optional
//!         purge_requested: <boolean>                              # optional
//!         encode_entire_prefix: <boolean>                         # optional
//! ```
//!
//! Type-specific validation decides which platform blocks are supported for a particular
//! catalog `type`, which fields under that platform are legal, and which of those fields
//! are required.

use std::collections::HashSet;
use std::path::Path;

use super::dbt_catalogs::DbtCatalogs;
use dbt_common::serde_utils::try_get_bool;
use dbt_common::{ErrorCode, FsResult, err, fs_err};
use dbt_yaml::{self as yml};

trait StrExt {
    fn is_empty_or_whitespace(&self) -> bool;
}

impl StrExt for str {
    #[inline]
    fn is_empty_or_whitespace(&self) -> bool {
        self.trim().is_empty()
    }
}

// ===== Phase 1: Loader Handoff =====
// Preconditions:
// - YAML has already been loaded and parsed by the caller.
// - We still have access to the raw top-level mapping.
// Postconditions:
// - The loader can rebuild a borrowed v2 view over the same mapping after the
//   caller selects the v2 path.

impl DbtCatalogs {
    /// Phase 1 -> 3 handoff from the loaded raw YAML document into the borrowed
    /// v2 schema pipeline.
    ///
    /// The YAML has already been loaded and parsed. This method rebuilds a
    /// near-zero-copy typed v2 view over the raw mapping rather than
    /// materializing owned strings. A temporary compatibility alias is carried
    /// for untouched downstream code.
    pub fn view_v2(&self) -> FsResult<DbtCatalogsV2View<'_>> {
        DbtCatalogsV2View::from_mapping(&self.repr, &self.span)
    }
}

// ===== Phase 2: Shape Validation =====
// Preconditions:
// - The caller has selected the catalogs v2 path.
// - Validation still works directly on raw YAML mappings and values.
// Postconditions:
// - The document matches the strict v2 YAML shape.
// - Required structural keys and container types are present.
// - Platform namespaces are recognized and platform blocks only contain known
//   keys for that platform.
// - Type-specific semantics have not been interpreted yet.

fn get_str<'a>(m: &'a yml::Mapping, k: &str) -> FsResult<Option<&'a str>> {
    match m.get(yml::Value::from(k)) {
        Some(v) => match v {
            yml::Value::String(s, _) => Ok(Some(s.trim())),
            _ => Err(fs_err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(v.span().clone()),
                "Key '{}' must be a string",
                k
            )),
        },
        None => Ok(None),
    }
}

fn get_map<'a>(m: &'a yml::Mapping, k: &str) -> FsResult<Option<&'a yml::Mapping>> {
    match m.get(yml::Value::from(k)) {
        Some(v) => match v {
            yml::Value::Mapping(map, _) => Ok(Some(map)),
            _ => Err(fs_err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(v.span().clone()),
                "Key '{}' must be a mapping",
                k
            )),
        },
        None => Ok(None),
    }
}

fn validate_optional_bool(m: &yml::Mapping, k: &str) -> FsResult<()> {
    try_get_bool(m, k).map(|_| ())
}

fn get_seq<'a>(m: &'a yml::Mapping, k: &str) -> FsResult<Option<&'a yml::Sequence>> {
    match m.get(yml::Value::from(k)) {
        Some(v) => match v {
            yml::Value::Sequence(seq, _) => Ok(Some(seq)),
            _ => Err(fs_err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(v.span().clone()),
                "Key '{}' must be a sequence/list",
                k
            )),
        },
        None => Ok(None),
    }
}

fn get_u32(m: &yml::Mapping, k: &str) -> FsResult<Option<u32>> {
    m.get(yml::Value::from(k))
        .map(|v| match v {
            yml::Value::Number(n, span) => n
                .as_i64()
                .and_then(|i| u32::try_from(i).ok())
                .ok_or_else(|| {
                    fs_err!(
                        code => ErrorCode::InvalidConfig,
                        hacky_yml_loc => Some(span.clone()),
                        "Key '{}' must be a non-negative integer",
                        k
                    )
                }),
            _ => Err(fs_err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(v.span().clone()),
                "Key '{}' must be a non-negative integer",
                k
            )),
        })
        .transpose()
}

fn field_span<'a>(m: &'a yml::Mapping, k: &str) -> Option<&'a yml::Span> {
    m.get(yml::Value::from(k)).map(|v| v.span())
}

fn check_unknown_keys(m: &yml::Mapping, allowed: &[&str], ctx: &str) -> FsResult<()> {
    for k in m.keys() {
        let span = k.span();
        let Some(ks) = k.as_str() else {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(span.clone()),
                "Non-string key in {}",
                ctx
            );
        };
        if !allowed.iter().any(|a| a == &ks) {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(span.clone()),
                "Unknown key '{}' in {}",
                ks,
                ctx
            );
        }
    }
    Ok(())
}

fn key_err(key: &str, err_span: Option<&yml::Span>) -> Box<dbt_common::FsError> {
    fs_err!(
        code => ErrorCode::InvalidConfig,
        hacky_yml_loc => err_span.cloned(),
        "Missing required key '{}' in catalogs.yml",
        key
    )
}

fn require_mapping<'a>(value: &'a yml::Value, ctx: &str) -> FsResult<&'a yml::Mapping> {
    value.as_mapping().ok_or_else(|| {
        fs_err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => Some(value.span().clone()),
            "{} must be a mapping",
            ctx
        )
    })
}

/// Phase 2: validate the custom v2 YAML shape directly on raw YAML mappings.
///
/// This pass is intentionally structural:
/// - reject unknown keys
/// - enforce required keys
/// - ensure lists/mappings are in the right places
/// - ensure platform blocks only contain known keys for that platform
///
/// It does not validate catalog-type-specific field subsets, requiredness, or
/// typed value semantics yet.
pub fn validate_catalogs_v2_shape(map: &yml::Mapping, span: &yml::Span) -> FsResult<()> {
    if map.get(yml::Value::from("iceberg_catalogs")).is_some() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => Some(span.clone()),
            "v2 catalogs.yml uses 'catalogs', not 'iceberg_catalogs'"
        );
    }

    check_unknown_keys(map, &["catalogs"], "top-level catalogs.yml(v2)")?;

    let catalogs = get_seq(map, "catalogs")?.ok_or_else(|| key_err("catalogs", Some(span)))?;
    let mut seen_catalog_names = HashSet::new();

    for (idx, item) in catalogs.iter().enumerate() {
        let item_span = item.span();
        let catalog = require_mapping(item, &format!("catalogs[{idx}]"))?;
        check_unknown_keys(
            catalog,
            &["name", "type", "table_format", "config"],
            "catalog specification",
        )?;

        for required in ["name", "type", "table_format", "config"] {
            if !catalog.contains_key(yml::Value::from(required)) {
                return Err(key_err(required, Some(item_span)));
            }
        }

        let name = get_str(catalog, "name")?.ok_or_else(|| key_err("name", Some(item_span)))?;
        if name.is_empty_or_whitespace() {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(catalog, "name").cloned(),
                "catalogs[].name must be non-empty"
            );
        }
        if !seen_catalog_names.insert(name) {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(catalog, "name").cloned(),
                "Duplicate catalog name '{}'",
                name
            );
        }

        if let Some(value) = catalog.get(yml::Value::from("config")) {
            let config = require_mapping(value, &format!("catalogs[{idx}].config"))?;
            check_unknown_keys(config, ALL_V2_PLATFORMS, "catalogs[].config")?;

            for &platform in ALL_V2_PLATFORMS {
                if let Some(platform_value) = config.get(yml::Value::from(platform)) {
                    let _ = require_mapping(
                        platform_value,
                        &format!("catalogs[{idx}].config.{platform}"),
                    )?;
                }
            }
        }
    }

    Ok(())
}

// ===== Phase 3: Borrowed View + Semantic Validation =====
// Preconditions:
// - Phase 2 has already locked the raw v2 shape.
// - Platform blocks are structurally well-formed and only contain known keys
//   for their platform namespace.
// - The remaining work is semantic: table_format, type/platform support,
//   type-specific allowed field subsets, requiredness, and value constraints.
// Postconditions:
// - Borrowed catalog views exist over the validated raw mapping.
// - Every catalog entry is semantically valid for its type and platform mix.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2CatalogType {
    Horizon,
    Glue,
    IcebergRest,
    HiveMetastore,
    Unity,
    BiglakeMetastore,
}

impl V2CatalogType {
    fn parse(raw: &str, span: &yml::Span) -> FsResult<Self> {
        if raw.eq_ignore_ascii_case("horizon") {
            Ok(Self::Horizon)
        } else if raw.eq_ignore_ascii_case("glue") {
            Ok(Self::Glue)
        } else if raw.eq_ignore_ascii_case("iceberg_rest") {
            Ok(Self::IcebergRest)
        } else if raw.eq_ignore_ascii_case("hive_metastore") {
            Ok(Self::HiveMetastore)
        } else if raw.eq_ignore_ascii_case("unity") {
            Ok(Self::Unity)
        } else if raw.eq_ignore_ascii_case("biglake_metastore") {
            Ok(Self::BiglakeMetastore)
        } else {
            err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(span.clone()),
                "type '{}' invalid. choose one of (horizon|glue|iceberg_rest|unity|hive_metastore|biglake_metastore)",
                raw
            )
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Horizon => "horizon",
            Self::Glue => "glue",
            Self::IcebergRest => "iceberg_rest",
            Self::HiveMetastore => "hive_metastore",
            Self::Unity => "unity",
            Self::BiglakeMetastore => "biglake_metastore",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2TableFormat {
    Default,
    Iceberg,
}

impl V2TableFormat {
    fn parse(raw: &str, span: &yml::Span) -> FsResult<Self> {
        if raw.eq_ignore_ascii_case("default") {
            Ok(Self::Default)
        } else if raw.eq_ignore_ascii_case("iceberg") {
            Ok(Self::Iceberg)
        } else {
            err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(span.clone()),
                "table_format '{}' invalid. choose one of (default|iceberg)",
                raw
            )
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Iceberg => "iceberg",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2FileFormat {
    Delta,
    Parquet,
    Hudi,
}

impl V2FileFormat {
    pub fn parse(raw: &str, span: Option<yml::Span>) -> FsResult<Self> {
        if raw.eq_ignore_ascii_case("delta") {
            Ok(Self::Delta)
        } else if raw.eq_ignore_ascii_case("parquet") {
            Ok(Self::Parquet)
        } else if raw.eq_ignore_ascii_case("hudi") {
            Ok(Self::Hudi)
        } else {
            err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => span,
                "file_format '{}' invalid. choose one of (delta|parquet|hudi)",
                raw
            )
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniformMode {
    Enabled,
    Disabled,
}

impl UniformMode {
    pub fn from_bool(b: bool) -> Self {
        if b { Self::Enabled } else { Self::Disabled }
    }

    pub fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Debug)]
pub struct CatalogSpecV2View<'a> {
    repr: &'a yml::Mapping,
    pub name: &'a str,
    pub catalog_type: V2CatalogType,
    pub table_format: V2TableFormat,
    config: &'a yml::Mapping,
}

#[derive(Debug)]
pub struct DbtCatalogsV2View<'a> {
    pub catalogs: Vec<CatalogSpecV2View<'a>>,
}

impl<'a> CatalogSpecV2View<'a> {
    /// Build a borrowed typed catalog view from one raw catalog mapping.
    fn from_mapping(map: &'a yml::Mapping, span: &yml::Span) -> FsResult<Self> {
        let name = get_str(map, "name")?.ok_or_else(|| key_err("name", Some(span)))?;
        let raw_type = get_str(map, "type")?.ok_or_else(|| key_err("type", Some(span)))?;
        let raw_table_format =
            get_str(map, "table_format")?.ok_or_else(|| key_err("table_format", Some(span)))?;
        let type_span = field_span(map, "type").ok_or_else(|| key_err("type", Some(span)))?;
        let table_format_span =
            field_span(map, "table_format").ok_or_else(|| key_err("table_format", Some(span)))?;
        let catalog_type = V2CatalogType::parse(raw_type, type_span)?;
        let table_format = V2TableFormat::parse(raw_table_format, table_format_span)?;
        let config_map = get_map(map, "config")?.ok_or_else(|| key_err("config", Some(span)))?;

        Ok(Self {
            name,
            repr: map,
            catalog_type,
            table_format,
            config: config_map,
        })
    }

    fn field_span(&self, key: &str) -> Option<&'a yml::Span> {
        field_span(self.repr, key)
    }

    pub fn config_block(&self, platform: &str) -> Option<&'a yml::Mapping> {
        self.config
            .get(yml::Value::from(platform))
            .and_then(|v| v.as_mapping())
    }
}

impl<'a> DbtCatalogsV2View<'a> {
    /// Phase 3 entry point: rebuild a borrowed v2 view from the raw YAML mapping.
    ///
    /// This reruns Phase 2 shape validation first, then constructs typed borrowed
    /// views for later semantic validation.
    pub fn from_mapping(map: &'a yml::Mapping, span: &yml::Span) -> FsResult<Self> {
        validate_catalogs_v2_shape(map, span)?;

        let catalog_entries =
            get_seq(map, "catalogs")?.ok_or_else(|| key_err("catalogs", Some(span)))?;

        let mut catalogs = Vec::with_capacity(catalog_entries.len());
        for (idx, item) in catalog_entries.iter().enumerate() {
            let item_span = item.span();
            let m = match item.as_mapping() {
                Some(m) => m,
                None => {
                    return err!(
                        code => ErrorCode::InvalidConfig,
                        hacky_yml_loc => Some(item_span.clone()),
                        "catalogs[{idx}] must be a mapping"
                    );
                }
            };
            catalogs.push(CatalogSpecV2View::from_mapping(m, item_span)?);
        }

        Ok(Self { catalogs })
    }
}

fn is_valid_target_file_size(v: &str) -> bool {
    v.eq_ignore_ascii_case("AUTO")
        || v.eq_ignore_ascii_case("16MB")
        || v.eq_ignore_ascii_case("32MB")
        || v.eq_ignore_ascii_case("64MB")
        || v.eq_ignore_ascii_case("128MB")
}

fn is_valid_storage_serialization_policy(v: &str) -> bool {
    v.eq_ignore_ascii_case("COMPATIBLE") || v.eq_ignore_ascii_case("OPTIMIZED")
}

const SNOWFLAKE_MANAGED_SNOWFLAKE_KEYS: &[&str] = &[
    "external_volume",
    "change_tracking",
    "data_retention_time_in_days",
    "max_data_extension_time_in_days",
    "storage_serialization_policy",
    "iceberg_version",
    "base_location_root",
];

const GLUE_SNOWFLAKE_KEYS: &[&str] = &[
    "catalog_database",
    "auto_refresh",
    "max_data_extension_time_in_days",
    "target_file_size",
    "iceberg_version",
];

const UNITY_SNOWFLAKE_KEYS: &[&str] = &[
    "catalog_database",
    "auto_refresh",
    "max_data_extension_time_in_days",
    "target_file_size",
    "iceberg_version",
];

const UNITY_DATABRICKS_KEYS: &[&str] = &["file_format", "location_root", "use_uniform"];

const HIVE_METASTORE_DATABRICKS_KEYS: &[&str] = &["file_format"];

const BIGLAKE_BIGQUERY_KEYS: &[&str] = &["external_volume", "file_format", "base_location_root"];
const ALL_V2_PLATFORMS: &[&str] = &["snowflake", "databricks", "bigquery", "duckdb"];

// Keys map to safe DuckDB ATTACH options for iceberg REST catalogs.
// Credential-bearing values belong in profiles.yml `secrets`; catalogs.yml only
// stores `secret`, the name of the DuckDB secret to reference during ATTACH.
// See: https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs#attach-options
const DUCKDB_KEYS: &[&str] = &[
    "endpoint",
    "endpoint_type",
    "warehouse",
    "secret",
    "attach_as",
    "default_region",
    "default_schema",
    "max_table_staleness",
    "authorization_type",
    "access_delegation_mode",
    "support_nested_namespaces",
    "support_stage_create",
    "purge_requested",
    "encode_entire_prefix",
];

fn validate_platform_support(catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
    let catalog_platforms = match catalog.catalog_type {
        V2CatalogType::Horizon => &["snowflake"][..],
        V2CatalogType::Glue => &["snowflake", "duckdb"],
        V2CatalogType::IcebergRest => &["snowflake", "duckdb"],
        V2CatalogType::Unity => &["snowflake", "databricks"],
        V2CatalogType::HiveMetastore => &["databricks"],
        V2CatalogType::BiglakeMetastore => &["bigquery"],
    };

    for &platform in ALL_V2_PLATFORMS {
        if catalog.config_block(platform).is_some() && !catalog_platforms.contains(&platform) {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("type").cloned(),
                "dbt does not support {} on the {} 'type'",
                platform,
                catalog.catalog_type.as_str()
            );
        }
    }
    Ok(())
}

fn validate_u32_range(map: &yml::Mapping, field: &str, max: u32) -> FsResult<()> {
    if let Some(v) = get_u32(map, field)?
        && v > max
    {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(map, field).cloned(),
            "Key '{}' must be in 0..={}",
            field,
            max
        );
    }
    Ok(())
}

fn parse_horizon_catalog(catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
    validate_platform_support(catalog)?;
    if catalog.table_format != V2TableFormat::Iceberg {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("table_format").cloned(),
            "Catalog '{}' type 'horizon' requires table_format='iceberg'",
            catalog.name
        );
    }
    let Some(snowflake) = catalog.config_block("snowflake") else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' type 'horizon' requires config.snowflake",
            catalog.name
        );
    };
    if field_span(snowflake, "base_location_subpath").is_some() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(snowflake, "base_location_subpath").cloned(),
            "Catalog '{}' horizon/snowflake base_location_subpath is model-config only and may not be specified in catalogs.yml",
            catalog.name
        );
    }
    check_unknown_keys(
        snowflake,
        SNOWFLAKE_MANAGED_SNOWFLAKE_KEYS,
        "catalogs[].config.snowflake (horizon)",
    )?;

    let Some(external_volume) = get_str(snowflake, "external_volume")? else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' horizon/snowflake config requires 'external_volume'",
            catalog.name
        );
    };
    if external_volume.is_empty_or_whitespace() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(snowflake, "external_volume").cloned(),
            "Catalog '{}' horizon/snowflake 'external_volume' must be non-empty",
            catalog.name
        );
    }
    if let Some(base_location_root) = get_str(snowflake, "base_location_root")?
        && base_location_root.is_empty_or_whitespace()
    {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(snowflake, "base_location_root").cloned(),
            "Catalog '{}' horizon/snowflake base_location_root cannot be blank",
            catalog.name
        );
    }
    if let Some(policy) = get_str(snowflake, "storage_serialization_policy")?
        && !is_valid_storage_serialization_policy(policy)
    {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(snowflake, "storage_serialization_policy").cloned(),
            "storage_serialization_policy '{}' invalid (COMPATIBLE|OPTIMIZED)",
            policy
        );
    }
    validate_u32_range(snowflake, "data_retention_time_in_days", 90)?;
    validate_u32_range(snowflake, "max_data_extension_time_in_days", 90)?;
    validate_optional_bool(snowflake, "change_tracking")?;

    Ok(())
}

fn validate_duckdb_config(
    duckdb: &yml::Mapping,
    catalog: &CatalogSpecV2View<'_>,
    type_name: &str,
) -> FsResult<()> {
    check_unknown_keys(
        duckdb,
        DUCKDB_KEYS,
        &format!("catalogs[].config.duckdb ({})", type_name),
    )?;

    let has_endpoint = get_str(duckdb, "endpoint")?;
    let has_endpoint_type = get_str(duckdb, "endpoint_type")?;

    // endpoint and endpoint_type are mutually exclusive; exactly one is required
    match (has_endpoint, has_endpoint_type) {
        (None, None) => {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("type").cloned(),
                "Catalog '{}' {}/duckdb config requires 'endpoint' or 'endpoint_type'",
                catalog.name,
                type_name
            );
        }
        (Some(ep), Some(_)) if !ep.is_empty_or_whitespace() => {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(duckdb, "endpoint_type").cloned(),
                "Catalog '{}' {}/duckdb 'endpoint' and 'endpoint_type' are mutually exclusive",
                catalog.name,
                type_name
            );
        }
        (Some(ep), _) if ep.is_empty_or_whitespace() => {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(duckdb, "endpoint").cloned(),
                "Catalog '{}' {}/duckdb 'endpoint' must be non-empty",
                catalog.name,
                type_name
            );
        }
        (_, Some(et)) => {
            let val = et.trim();
            if !val.eq_ignore_ascii_case("GLUE") && !val.eq_ignore_ascii_case("S3_TABLES") {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => field_span(duckdb, "endpoint_type").cloned(),
                    "Catalog '{}' {}/duckdb 'endpoint_type' must be 'GLUE' or 'S3_TABLES'",
                    catalog.name,
                    type_name
                );
            }
            if val.eq_ignore_ascii_case("S3_TABLES") {
                let Some(warehouse) = get_str(duckdb, "warehouse")? else {
                    return err!(
                        code => ErrorCode::InvalidConfig,
                        hacky_yml_loc => field_span(duckdb, "endpoint_type").cloned(),
                        "Catalog '{}' {}/duckdb endpoint_type='S3_TABLES' requires 'warehouse'",
                        catalog.name,
                        type_name
                    );
                };
                if warehouse.is_empty_or_whitespace() {
                    return err!(
                        code => ErrorCode::InvalidConfig,
                        hacky_yml_loc => field_span(duckdb, "warehouse").cloned(),
                        "Catalog '{}' {}/duckdb 'warehouse' must be non-empty",
                        catalog.name,
                        type_name
                    );
                }
            }
        }
        _ => {}
    }

    // All remaining string fields: optional, but non-empty if present
    for key in [
        "warehouse",
        "secret",
        "attach_as",
        "default_region",
        "default_schema",
        "max_table_staleness",
    ] {
        if let Some(val) = get_str(duckdb, key)?
            && val.is_empty_or_whitespace()
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(duckdb, key).cloned(),
                "Catalog '{}' {}/duckdb '{}' must be non-empty",
                catalog.name,
                type_name,
                key
            );
        }
    }

    // authorization_type: constrained enum
    if let Some(auth_type) = get_str(duckdb, "authorization_type")? {
        if has_endpoint_type.is_some() {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(duckdb, "authorization_type").cloned(),
                "Catalog '{}' {}/duckdb 'authorization_type' cannot be combined with 'endpoint_type'",
                catalog.name,
                type_name
            );
        }
        let val = auth_type.trim();
        if !val.eq_ignore_ascii_case("OAUTH2")
            && !val.eq_ignore_ascii_case("SIGV4")
            && !val.eq_ignore_ascii_case("NONE")
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(duckdb, "authorization_type").cloned(),
                "Catalog '{}' {}/duckdb 'authorization_type' must be 'OAUTH2', 'SIGV4', or 'NONE'",
                catalog.name,
                type_name
            );
        }
    }

    // access_delegation_mode: constrained enum
    if let Some(mode) = get_str(duckdb, "access_delegation_mode")? {
        let val = mode.trim();
        if !val.eq_ignore_ascii_case("VENDED_CREDENTIALS") && !val.eq_ignore_ascii_case("NONE") {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(duckdb, "access_delegation_mode").cloned(),
                "Catalog '{}' {}/duckdb 'access_delegation_mode' must be 'VENDED_CREDENTIALS' or 'NONE'",
                catalog.name,
                type_name
            );
        }
    }

    for key in [
        "support_nested_namespaces",
        "support_stage_create",
        "purge_requested",
        "encode_entire_prefix",
    ] {
        validate_optional_bool(duckdb, key)?;
    }

    Ok(())
}

fn parse_glue_catalog(catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
    validate_platform_support(catalog)?;
    if catalog.table_format != V2TableFormat::Iceberg {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("table_format").cloned(),
            "Catalog '{}' type 'glue' requires table_format='iceberg'",
            catalog.name
        );
    }

    if catalog.config_block("snowflake").is_none() && catalog.config_block("duckdb").is_none() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' of type 'glue' requires at least one config block: snowflake or duckdb",
            catalog.name
        );
    }

    if let Some(snowflake) = catalog.config_block("snowflake") {
        check_unknown_keys(
            snowflake,
            GLUE_SNOWFLAKE_KEYS,
            "catalogs[].config.snowflake (glue)",
        )?;

        let Some(catalog_database) = get_str(snowflake, "catalog_database")? else {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("type").cloned(),
                "Catalog '{}' glue/snowflake config requires 'catalog_database'",
                catalog.name
            );
        };
        if catalog_database.is_empty_or_whitespace() {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(snowflake, "catalog_database").cloned(),
                "Catalog '{}' glue/snowflake 'catalog_database' must be non-empty",
                catalog.name
            );
        }
        if let Some(target_file_size) = get_str(snowflake, "target_file_size")?
            && !is_valid_target_file_size(target_file_size)
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(snowflake, "target_file_size").cloned(),
                "target_file_size '{}' invalid (AUTO|16MB|32MB|64MB|128MB)",
                target_file_size
            );
        }
        validate_u32_range(snowflake, "max_data_extension_time_in_days", 90)?;
        validate_optional_bool(snowflake, "auto_refresh")?;
    }

    if let Some(duckdb) = catalog.config_block("duckdb") {
        validate_duckdb_config(duckdb, catalog, "glue")?;
    }

    Ok(())
}

fn parse_linked_catalog(catalog: &CatalogSpecV2View<'_>, type_name: &str) -> FsResult<()> {
    validate_platform_support(catalog)?;
    if catalog.table_format != V2TableFormat::Iceberg {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("table_format").cloned(),
            "Catalog '{}' type '{}' requires table_format='iceberg'",
            catalog.name,
            type_name
        );
    }
    if catalog.config_block("snowflake").is_none() && catalog.config_block("databricks").is_none() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' of type '{}' requires at least one config block: snowflake or databricks",
            catalog.name,
            type_name
        );
    }

    if let Some(snowflake) = catalog.config_block("snowflake") {
        check_unknown_keys(
            snowflake,
            UNITY_SNOWFLAKE_KEYS,
            "catalogs[].config.snowflake (unity)",
        )?;
        let Some(catalog_database) = get_str(snowflake, "catalog_database")? else {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("type").cloned(),
                "Catalog '{}' {}/snowflake config requires 'catalog_database'",
                catalog.name,
                type_name
            );
        };
        if catalog_database.is_empty_or_whitespace() {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(snowflake, "catalog_database").cloned(),
                "Catalog '{}' {}/snowflake 'catalog_database' must be non-empty",
                catalog.name,
                type_name
            );
        }
        if let Some(target_file_size) = get_str(snowflake, "target_file_size")?
            && !is_valid_target_file_size(target_file_size)
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(snowflake, "target_file_size").cloned(),
                "target_file_size '{}' invalid (AUTO|16MB|32MB|64MB|128MB)",
                target_file_size
            );
        }
        validate_u32_range(snowflake, "max_data_extension_time_in_days", 90)?;
        validate_optional_bool(snowflake, "auto_refresh")?;
    }

    if let Some(databricks) = catalog.config_block("databricks") {
        check_unknown_keys(
            databricks,
            UNITY_DATABRICKS_KEYS,
            "catalogs[].config.databricks (unity)",
        )?;
        let Some(file_format) = get_str(databricks, "file_format")? else {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("type").cloned(),
                "Catalog '{}' {}/databricks config requires 'file_format' (delta|parquet)",
                catalog.name,
                type_name
            );
        };

        let file_format_span = field_span(databricks, "file_format").cloned();
        let file_format = V2FileFormat::parse(file_format, file_format_span.clone())?;
        let use_uniform =
            UniformMode::from_bool(try_get_bool(databricks, "use_uniform")?.unwrap_or(false));

        match (file_format, use_uniform) {
            (V2FileFormat::Delta, UniformMode::Enabled)
            | (V2FileFormat::Parquet, UniformMode::Disabled) => Ok(()),
            (V2FileFormat::Delta, UniformMode::Disabled) => err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => file_format_span,
                "Catalog '{}' {}/databricks use_uniform: false (or unset) requires file_format: parquet",
                catalog.name,
                type_name
            ),
            (V2FileFormat::Parquet, UniformMode::Enabled) => err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => file_format_span,
                "Catalog '{}' {}/databricks use_uniform: true requires file_format: delta",
                catalog.name,
                type_name
            ),
            (V2FileFormat::Hudi, _) => err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => file_format_span,
                "Catalog '{}' {}/databricks file_format 'hudi' is not valid for unity (use delta or parquet)",
                catalog.name,
                type_name
            ),
        }?;
        if let Some(location_root) = get_str(databricks, "location_root")?
            && location_root.is_empty_or_whitespace()
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(databricks, "location_root").cloned(),
                "Catalog '{}' {}/databricks location_root cannot be blank",
                catalog.name,
                type_name
            );
        }
    }

    Ok(())
}

fn parse_hive_metastore_catalog(catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
    validate_platform_support(catalog)?;
    if catalog.table_format != V2TableFormat::Default {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("table_format").cloned(),
            "Catalog '{}' type 'hive_metastore' requires table_format='default'",
            catalog.name
        );
    }
    let Some(databricks) = catalog.config_block("databricks") else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' type 'hive_metastore' requires config.databricks",
            catalog.name
        );
    };
    check_unknown_keys(
        databricks,
        HIVE_METASTORE_DATABRICKS_KEYS,
        "catalogs[].config.databricks (hive_metastore)",
    )?;
    let Some(file_format) = get_str(databricks, "file_format")? else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' hive_metastore/databricks config requires 'file_format' (delta|parquet|hudi)",
            catalog.name
        );
    };
    if !file_format.eq_ignore_ascii_case("delta")
        && !file_format.eq_ignore_ascii_case("parquet")
        && !file_format.eq_ignore_ascii_case("hudi")
    {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(databricks, "file_format").cloned(),
            "Catalog '{}' hive_metastore/databricks file_format must be one of (delta|parquet|hudi)",
            catalog.name
        );
    }
    Ok(())
}

fn parse_biglake_metastore_catalog(catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
    validate_platform_support(catalog)?;
    if catalog.table_format != V2TableFormat::Iceberg {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("table_format").cloned(),
            "Catalog '{}' type 'biglake_metastore' requires table_format='iceberg'",
            catalog.name
        );
    }
    let Some(bigquery) = catalog.config_block("bigquery") else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' type 'biglake_metastore' requires config.bigquery",
            catalog.name
        );
    };
    check_unknown_keys(
        bigquery,
        BIGLAKE_BIGQUERY_KEYS,
        "catalogs[].config.bigquery (biglake_metastore)",
    )?;

    let Some(external_volume) = get_str(bigquery, "external_volume")? else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' BigLake config requires 'external_volume'",
            catalog.name
        );
    };
    if external_volume.is_empty_or_whitespace() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(bigquery, "external_volume").cloned(),
            "Catalog '{}' BigLake 'external_volume' must be non-empty",
            catalog.name
        );
    }
    if !external_volume.starts_with("gs://") {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(bigquery, "external_volume").cloned(),
            "Catalog '{}' BigLake 'external_volume' must be a path to a Cloud Storage bucket (gs://<bucket_name>)",
            catalog.name
        );
    }

    let Some(file_format) = get_str(bigquery, "file_format")? else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' BigLake config requires 'file_format' (parquet)",
            catalog.name
        );
    };
    if !file_format.eq_ignore_ascii_case("parquet") {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(bigquery, "file_format").cloned(),
            "Catalog '{}' BigLake file_format must be 'parquet'",
            catalog.name
        );
    }
    if let Some(base_location_root) = get_str(bigquery, "base_location_root")?
        && base_location_root.is_empty_or_whitespace()
    {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => field_span(bigquery, "base_location_root").cloned(),
            "Catalog '{}' BigLake base_location_root cannot be blank",
            catalog.name
        );
    }

    Ok(())
}

fn parse_iceberg_rest_catalog(catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
    validate_platform_support(catalog)?;
    if catalog.table_format != V2TableFormat::Iceberg {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("table_format").cloned(),
            "Catalog '{}' type 'iceberg_rest' requires table_format='iceberg'",
            catalog.name
        );
    }

    if catalog.config_block("snowflake").is_none() && catalog.config_block("duckdb").is_none() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' of type 'iceberg_rest' requires at least one config block: snowflake or duckdb",
            catalog.name
        );
    }

    if let Some(snowflake) = catalog.config_block("snowflake") {
        check_unknown_keys(
            snowflake,
            GLUE_SNOWFLAKE_KEYS,
            "catalogs[].config.snowflake (iceberg_rest)",
        )?;
        let Some(catalog_database) = get_str(snowflake, "catalog_database")? else {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("type").cloned(),
                "Catalog '{}' iceberg_rest/snowflake config requires 'catalog_database'",
                catalog.name
            );
        };
        if catalog_database.is_empty_or_whitespace() {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(snowflake, "catalog_database").cloned(),
                "Catalog '{}' iceberg_rest/snowflake 'catalog_database' must be non-empty",
                catalog.name
            );
        }
        if let Some(target_file_size) = get_str(snowflake, "target_file_size")?
            && !is_valid_target_file_size(target_file_size)
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(snowflake, "target_file_size").cloned(),
                "target_file_size '{}' invalid (AUTO|16MB|32MB|64MB|128MB)",
                target_file_size
            );
        }
        validate_u32_range(snowflake, "max_data_extension_time_in_days", 90)?;
        validate_optional_bool(snowflake, "auto_refresh")?;
    }

    if let Some(duckdb) = catalog.config_block("duckdb") {
        validate_duckdb_config(duckdb, catalog, "iceberg_rest")?;
    }

    Ok(())
}

pub fn validate_catalogs_v2(spec: &DbtCatalogsV2View<'_>, _path: &Path) -> FsResult<()> {
    for catalog in &spec.catalogs {
        let () = match catalog.catalog_type {
            V2CatalogType::Horizon => parse_horizon_catalog(catalog)?,
            V2CatalogType::Glue => parse_glue_catalog(catalog)?,
            V2CatalogType::IcebergRest => parse_iceberg_rest_catalog(catalog)?,
            V2CatalogType::Unity => parse_linked_catalog(catalog, "unity")?,
            V2CatalogType::HiveMetastore => parse_hive_metastore_catalog(catalog)?,
            V2CatalogType::BiglakeMetastore => parse_biglake_metastore_catalog(catalog)?,
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_yaml as yml;
    use std::path::Path;

    fn parse_and_validate(yaml: &str) -> FsResult<()> {
        let v: yml::Value = yml::from_str(yaml).unwrap();
        let v_span = v.span();
        let m = v.as_mapping().expect("top-level YAML must be a mapping");
        validate_catalogs_v2_shape(m, v_span)?;
        let view = DbtCatalogsV2View::from_mapping(m, v_span)?;
        validate_catalogs_v2(&view, Path::new("<test>"))?;
        Ok(())
    }

    #[test]
    fn unity_multiplatform_v2_valid() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_DB"
        auto_refresh: true
      databricks:
        file_format: delta
        location_root: "s3://bucket/path"
        use_uniform: true
"#;
        parse_and_validate(yaml).expect("v2 should validate");
    }

    #[test]
    fn unity_databricks_parquet_managed_iceberg_valid() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: parquet
        use_uniform: false
"#;
        parse_and_validate(yaml).expect("parquet + use_uniform=false should validate");
    }

    #[test]
    fn unity_databricks_parquet_use_uniform_unset_valid() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: parquet
"#;
        parse_and_validate(yaml).expect("parquet with use_uniform unset should validate");
    }

    #[test]
    fn horizon_v2_valid() {
        let yaml = r#"
catalogs:
  - name: sf_native
    type: horizon
    table_format: iceberg
    config:
      snowflake:
        external_volume: my_external_volume
        base_location_root: analytics/iceberg/dbt
        storage_serialization_policy: COMPATIBLE
        data_retention_time_in_days: 1
        max_data_extension_time_in_days: 14
        change_tracking: false
"#;
        parse_and_validate(yaml).expect("v2 horizon should validate");
    }

    #[test]
    fn glue_v2_valid() {
        let yaml = r#"
catalogs:
  - name: glue_cat
    type: glue
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_CLD"
        auto_refresh: true
        target_file_size: AUTO
"#;
        parse_and_validate(yaml).expect("v2 glue should validate");
    }

    #[test]
    fn iceberg_rest_v2_valid() {
        let yaml = r#"
catalogs:
  - name: rest_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_REST_CLD"
        auto_refresh: true
        max_data_extension_time_in_days: 1
        target_file_size: AUTO
"#;
        parse_and_validate(yaml).expect("v2 iceberg_rest should validate");
    }

    #[test]
    fn iceberg_rest_rejects_databricks_block() {
        let yaml = r#"
catalogs:
  - name: rest_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_REST_CLD"
      databricks:
        file_format: delta
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("does not support databricks on the iceberg_rest"),
            "unexpected error: {res:?}"
        );
    }

    #[test]
    fn hive_metastore_v2_valid() {
        let yaml = r#"
catalogs:
  - name: hive
    type: hive_metastore
    table_format: default
    config:
      databricks:
        file_format: hudi
"#;
        parse_and_validate(yaml).expect("v2 hive_metastore should validate");
    }

    #[test]
    fn biglake_metastore_v2_valid() {
        let yaml = r#"
catalogs:
  - name: cat1
    type: biglake_metastore
    table_format: iceberg
    config:
      bigquery:
        external_volume: "gs://bucket"
        file_format: parquet
        base_location_root: "root1"
"#;
        parse_and_validate(yaml).expect("v2 bigquery should validate");
    }

    #[test]
    fn v2_rejects_legacy_iceberg_catalogs_key() {
        let yaml = r#"
iceberg_catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config: {}
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("uses 'catalogs', not 'iceberg_catalogs'"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn unity_rejects_bigquery_block_in_config() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_DB"
      bigquery:
        external_volume: "gs://bucket"
        file_format: parquet
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("does not support bigquery on the unity"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn horizon_rejects_bigquery_platform_block() {
        let yaml = r#"
catalogs:
  - name: my_catalog
    type: horizon
    table_format: iceberg
    config:
      bigquery:
        external_volume: "gs://bucket"
        file_format: parquet
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("does not support bigquery on the horizon"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn horizon_rejects_unity_only_snowflake_fields() {
        let yaml = r#"
catalogs:
  - name: sf_native
    type: horizon
    table_format: iceberg
    config:
      snowflake:
        external_volume: my_external_volume
        catalog_database: SOME_DB
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("Unknown key 'catalog_database'"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn horizon_rejects_catalog_base_location_subpath() {
        let yaml = r#"
catalogs:
  - name: sf_native
    type: horizon
    table_format: iceberg
    config:
      snowflake:
        external_volume: my_external_volume
        base_location_subpath: model_only
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("base_location_subpath is model-config only"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn glue_rejects_horizon_only_snowflake_fields() {
        let yaml = r#"
catalogs:
  - name: glue_cat
    type: glue
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_CLD"
        external_volume: should_not_be_here
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("Unknown key 'external_volume'"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn unity_rejects_horizon_only_snowflake_fields() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_DB"
        external_volume: should_not_be_here
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("Unknown key 'external_volume'"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn iceberg_rest_snowflake_only_still_valid() {
        let yaml = r#"
catalogs:
  - name: rest_sf
    type: iceberg_rest
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_DB"
        auto_refresh: true
"#;
        parse_and_validate(yaml).expect("iceberg_rest + snowflake should validate");
    }

    #[test]
    fn unity_databricks_parquet_with_use_uniform_is_rejected() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      databricks:
        use_uniform: true
        file_format: parquet
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("use_uniform: true requires file_format: delta"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn unity_databricks_delta_without_use_uniform_is_rejected() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: delta
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("use_uniform: false (or unset) requires file_format: parquet"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn unity_databricks_delta_with_use_uniform_false_is_rejected() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: delta
        use_uniform: false
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("use_uniform: false (or unset) requires file_format: parquet"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn unity_databricks_unknown_file_format_is_rejected() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: iceberg
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("invalid. choose one of (delta|parquet|hudi)"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn top_level_platform_specific_keys_are_rejected() {
        let yaml = r#"
catalogs:
  - name: linked_catalog
    type: unity
    table_format: iceberg
    file_format: parquet
    config:
      databricks: {}
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("Unknown key 'file_format' in catalog specification"),
            "unexpected error: {msg}"
        );
    }

    // ===== DuckDB + IcebergRest tests =====

    #[test]
    fn glue_duckdb_v2_valid() {
        let yaml = r#"
catalogs:
  - name: glue_duck
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://glue.us-east-1.amazonaws.com"
"#;
        parse_and_validate(yaml).expect("glue + duckdb should validate");
    }

    #[test]
    fn iceberg_rest_duckdb_v2_valid() {
        let yaml = r#"
catalogs:
  - name: rest_duck
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://my-iceberg-rest.example.com"
        secret: "my_secret"
        attach_as: "my_catalog"
"#;
        parse_and_validate(yaml).expect("iceberg_rest + duckdb should validate");
    }

    #[test]
    fn iceberg_rest_duckdb_and_snowflake_v2_valid() {
        let yaml = r#"
catalogs:
  - name: rest_mixed
    type: iceberg_rest
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_DB"
      duckdb:
        endpoint: "https://my-iceberg-rest.example.com"
"#;
        parse_and_validate(yaml).expect("iceberg_rest + snowflake + duckdb should validate");
    }

    #[test]
    fn glue_snowflake_only_still_valid() {
        let yaml = r#"
catalogs:
  - name: glue_sf
    type: glue
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_CLD"
        auto_refresh: true
"#;
        parse_and_validate(yaml).expect("glue + snowflake should still validate");
    }

    #[test]
    fn iceberg_rest_duckdb_missing_endpoint() {
        let yaml = r#"
catalogs:
  - name: rest_duck
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        secret: "my_secret"
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(msg.contains("'endpoint'"), "unexpected error: {msg}");
    }

    #[test]
    fn iceberg_rest_duckdb_blank_endpoint() {
        let yaml = r#"
catalogs:
  - name: rest_duck
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "   "
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("'endpoint' must be non-empty"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn iceberg_rest_duckdb_blank_secret() {
        let yaml = r#"
catalogs:
  - name: rest_duck
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://my-rest.example.com"
        secret: ""
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("'secret' must be non-empty"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn iceberg_rest_duckdb_blank_attach_as() {
        let yaml = r#"
catalogs:
  - name: rest_duck
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://my-rest.example.com"
        attach_as: ""
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("'attach_as' must be non-empty"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn unity_duckdb_v2_rejected() {
        let yaml = r#"
catalogs:
  - name: unity_duck
    type: unity
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("does not support duckdb on the unity"),
            "unexpected error: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // DuckDB config: endpoint_type
    // -----------------------------------------------------------------------

    #[test]
    fn duckdb_endpoint_type_glue_valid() {
        let yaml = r#"
catalogs:
  - name: glue_cat
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint_type: GLUE
"#;
        parse_and_validate(yaml).expect("endpoint_type=GLUE should validate");
    }

    #[test]
    fn duckdb_endpoint_type_s3_tables_valid() {
        let yaml = r#"
catalogs:
  - name: s3t_cat
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint_type: S3_TABLES
        warehouse: "arn:aws:s3tables:us-east-1:123456789012:bucket/example"
"#;
        parse_and_validate(yaml).expect("endpoint_type=S3_TABLES should validate");
    }

    #[test]
    fn duckdb_endpoint_type_invalid_value() {
        let yaml = r#"
catalogs:
  - name: bad_cat
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint_type: INVALID
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("'GLUE' or 'S3_TABLES'"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_endpoint_and_endpoint_type_mutual_exclusion() {
        let yaml = r#"
catalogs:
  - name: both_cat
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        endpoint_type: GLUE
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("mutually exclusive"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_s3_tables_endpoint_type_requires_warehouse() {
        let yaml = r#"
catalogs:
  - name: s3t_cat
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint_type: S3_TABLES
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("requires 'warehouse'"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_neither_endpoint_nor_endpoint_type() {
        let yaml = r#"
catalogs:
  - name: empty_cat
    type: glue
    table_format: iceberg
    config:
      duckdb:
        secret: "my_secret"
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("'endpoint' or 'endpoint_type'"),
            "unexpected: {res:?}"
        );
    }

    // -----------------------------------------------------------------------
    // DuckDB config: authorization_type, access_delegation_mode
    // -----------------------------------------------------------------------

    #[test]
    fn duckdb_authorization_type_valid() {
        for auth_type in ["OAUTH2", "SIGV4", "NONE"] {
            let yaml = format!(
                r#"
catalogs:
  - name: auth_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        authorization_type: {auth_type}
"#
            );
            parse_and_validate(&yaml)
                .unwrap_or_else(|e| panic!("authorization_type={auth_type} should validate: {e}"));
        }
    }

    #[test]
    fn duckdb_authorization_type_invalid() {
        let yaml = r#"
catalogs:
  - name: auth_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        authorization_type: BEARER
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("'OAUTH2', 'SIGV4', or 'NONE'"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_authorization_type_cannot_combine_with_endpoint_type() {
        let yaml = r#"
catalogs:
  - name: auth_cat
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint_type: GLUE
        authorization_type: SIGV4
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("cannot be combined with 'endpoint_type'"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_access_delegation_mode_valid() {
        for mode in ["VENDED_CREDENTIALS", "NONE"] {
            let yaml = format!(
                r#"
catalogs:
  - name: deleg_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        access_delegation_mode: {mode}
"#
            );
            parse_and_validate(&yaml)
                .unwrap_or_else(|e| panic!("access_delegation_mode={mode} should validate: {e}"));
        }
    }

    #[test]
    fn duckdb_access_delegation_mode_invalid() {
        let yaml = r#"
catalogs:
  - name: deleg_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        access_delegation_mode: REMOTE
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("'VENDED_CREDENTIALS' or 'NONE'"),
            "unexpected: {res:?}"
        );
    }

    // -----------------------------------------------------------------------
    // DuckDB config: full config with all optional keys
    // -----------------------------------------------------------------------

    #[test]
    fn duckdb_full_config_all_optional_keys() {
        let yaml = r#"
catalogs:
  - name: full_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://my-catalog.example.com"
        warehouse: "warehouse_name"
        secret: "my_secret"
        attach_as: "my_db"
        default_region: "us-east-1"
        default_schema: "demo"
        max_table_staleness: "10 minutes"
        authorization_type: OAUTH2
        access_delegation_mode: VENDED_CREDENTIALS
        support_nested_namespaces: true
        support_stage_create: false
        purge_requested: true
        encode_entire_prefix: true
"#;
        parse_and_validate(yaml).expect("full config should validate");
    }

    #[test]
    fn duckdb_credential_values_belong_in_profile_secrets() {
        let yaml = r#"
catalogs:
  - name: rest_duck
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        client_secret: "actual-secret-value"
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error for credential-bearing key");
        assert!(
            format!("{res:?}").contains("client_secret"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_boolean_attach_options_validate_type() {
        let yaml = r#"
catalogs:
  - name: rest_duck
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        support_stage_create: "yes"
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error for non-boolean attach option");
        assert!(
            format!("{res:?}").contains("support_stage_create"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_unknown_key_rejected() {
        let yaml = r#"
catalogs:
  - name: unk_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        bogus_key: "value"
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error for unknown key");
        assert!(
            format!("{res:?}").contains("bogus_key"),
            "unexpected: {res:?}"
        );
    }

    #[test]
    fn duckdb_blank_warehouse_invalid() {
        let yaml = r#"
catalogs:
  - name: bad_cat
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://example.com"
        warehouse: "   "
"#;
        let res = parse_and_validate(yaml);
        assert!(res.is_err(), "expected error");
        assert!(
            format!("{res:?}").contains("'warehouse' must be non-empty"),
            "unexpected: {res:?}"
        );
    }
}
