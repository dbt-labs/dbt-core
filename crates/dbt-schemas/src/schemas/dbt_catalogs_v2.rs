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
//!         base_location_root: <path string>                       # optional, non-empty if present
//!
//!         # model config only; specifying this in catalogs.yml is invalid
//!         base_location_subpath: <string>                         # optional
//!
//!   # type: glue
//!   # supported platforms: snowflake
//!   # a snowflake config block is required
//!   - name: glue_catalog
//!     type: glue
//!     table_format: iceberg
//!     config:
//!       snowflake:
//!         catalog_database: <string>                              # required, non-empty
//!         auto_refresh: <boolean>                                 # optional
//!         max_data_extension_time_in_days: <u32 in 0..=90>        # optional
//!         target_file_size: AUTO|16MB|32MB|64MB|128MB             # optional
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
//! ```
//!
//! Type-specific validation decides which platform blocks are supported for a particular
//! catalog `type`, which fields under that platform are legal, and which of those fields
//! are required.

use std::collections::HashSet;
use std::path::Path;

use super::dbt_catalogs::DbtCatalogs;
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
    match m.get(yml::Value::from(k)) {
        Some(v) => match v {
            yml::Value::Bool(_, _) => Ok(()),
            yml::Value::String(s, _)
                if s.trim().eq_ignore_ascii_case("true")
                    || s.trim().eq_ignore_ascii_case("false") =>
            {
                Ok(())
            }
            _ => Err(fs_err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(v.span().clone()),
                "Key '{}' must be a boolean",
                k
            )),
        },
        None => Ok(()),
    }
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
            check_unknown_keys(
                config,
                &["snowflake", "databricks", "bigquery"],
                "catalogs[].config",
            )?;

            for platform in ["snowflake", "databricks", "bigquery"] {
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
    // TODO: Instead of a Vec, let's make this a hash table for O(1) access of catalog configs.
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
    "base_location_root",
];

const GLUE_SNOWFLAKE_KEYS: &[&str] = &[
    "catalog_database",
    "auto_refresh",
    "max_data_extension_time_in_days",
    "target_file_size",
];

const UNITY_SNOWFLAKE_KEYS: &[&str] = &[
    "catalog_database",
    "auto_refresh",
    "max_data_extension_time_in_days",
    "target_file_size",
];

const UNITY_DATABRICKS_KEYS: &[&str] = &["file_format", "location_root", "use_uniform"];

const HIVE_METASTORE_DATABRICKS_KEYS: &[&str] = &["file_format"];

const BIGLAKE_BIGQUERY_KEYS: &[&str] = &["external_volume", "file_format", "base_location_root"];

fn validate_platform_support(catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
    let catalog_platforms = match catalog.catalog_type {
        V2CatalogType::Horizon => &["snowflake"][..],
        V2CatalogType::Glue => &["snowflake"],
        V2CatalogType::IcebergRest => &["snowflake"],
        V2CatalogType::Unity => &["snowflake", "databricks"],
        V2CatalogType::HiveMetastore => &["databricks"],
        V2CatalogType::BiglakeMetastore => &["bigquery"],
    };

    for platform in ["snowflake", "databricks", "bigquery"] {
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
    let Some(snowflake) = catalog.config_block("snowflake") else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' type 'iceberg_rest' requires config.snowflake",
            catalog.name
        );
    };
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

    let Some(snowflake) = catalog.config_block("snowflake") else {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => catalog.field_span("type").cloned(),
            "Catalog '{}' type 'glue' requires config.snowflake",
            catalog.name
        );
    };
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
                "Catalog '{}' {}/databricks config requires 'file_format' (delta)",
                catalog.name,
                type_name
            );
        };
        if !file_format.eq_ignore_ascii_case("delta") {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(databricks, "file_format").cloned(),
                "Catalog '{}' {}/databricks file_format must be 'delta'",
                catalog.name,
                type_name
            );
        }
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
        validate_optional_bool(databricks, "use_uniform")?;
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
        use_uniform: false
"#;
        parse_and_validate(yaml).expect("v2 should validate");
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
    fn unity_databricks_requires_delta_even_with_use_uniform() {
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
            msg.contains("file_format must be 'delta'"),
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
}
