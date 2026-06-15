//! catalogs.yml v2 — table-driven validation and JSON schema generation.
//!
//! ```yaml
//! catalogs:
//!   - name: <string>
//!     type: <catalog type>
//!     table_format: <iceberg|default>
//!     config:
//!       <platform>:
//!         # per-platform fields — see CATALOG_SCHEMAS + FieldSpec arrays below,
//!         # or run `dbt man --schema catalog` for the canonical JSON schema.
//! ```

use std::collections::HashSet;
use std::path::Path;

use super::dbt_catalogs::DbtCatalogs;
use dbt_common::serde_utils::try_get_bool;
use dbt_common::{ErrorCode, FsResult, err, fs_err};
use dbt_yaml::{self as yml};

const ALL_V2_PLATFORMS: &[&str] = &["snowflake", "databricks", "bigquery", "duckdb"];

const TARGET_FILE_SIZES: &[&str] = &["AUTO", "16MB", "32MB", "64MB", "128MB"];
const STORAGE_SERIALIZATION_POLICIES: &[&str] = &["COMPATIBLE", "OPTIMIZED"];
const DUCKDB_ENDPOINT_TYPES: &[&str] = &["GLUE", "S3_TABLES"];
const DUCKDB_AUTHORIZATION_TYPES: &[&str] = &["OAUTH2", "SIGV4", "NONE"];
const DUCKDB_ACCESS_DELEGATION_MODES: &[&str] = &["VENDED_CREDENTIALS", "NONE"];
const LOCAL_FS_FILE_FORMATS: &[&str] = &["parquet", "csv", "json"];
// unity only supports delta (with UniForm) or parquet; `hudi` is rejected for
// unity by `validate_unity_semantics`, so it is excluded here and from the
// published JSON schema. hive_metastore does accept hudi (separate const).
const UNITY_DATABRICKS_FILE_FORMATS: &[&str] = &["delta", "parquet"];
const HIVE_METASTORE_FILE_FORMATS: &[&str] = &["delta", "parquet", "hudi"];
const BIGLAKE_FILE_FORMATS: &[&str] = &["parquet"];

fn matches_enum_ci(v: &str, allowed: &[&str]) -> bool {
    allowed.iter().any(|a| v.eq_ignore_ascii_case(a))
}

#[derive(Debug, Clone, Copy)]
enum FieldKind {
    Str,
    Bool,
    U32 { max: Option<u32> },
    Enum(&'static [&'static str]),
}

#[derive(Debug, Clone, Copy)]
struct FieldSpec {
    name: &'static str,
    kind: FieldKind,
    required: bool,
    non_empty: bool,
    forbidden: bool,
    doc: &'static str,
}

impl FieldSpec {
    const fn new(name: &'static str, kind: FieldKind) -> Self {
        Self {
            name,
            kind,
            required: false,
            non_empty: false,
            forbidden: false,
            doc: "",
        }
    }
    const fn string(name: &'static str) -> Self {
        Self::new(name, FieldKind::Str)
    }
    const fn boolean(name: &'static str) -> Self {
        Self::new(name, FieldKind::Bool)
    }
    const fn u32_plain(name: &'static str) -> Self {
        Self::new(name, FieldKind::U32 { max: None })
    }
    const fn u32_max(name: &'static str, max: u32) -> Self {
        Self::new(name, FieldKind::U32 { max: Some(max) })
    }
    const fn enumerated(name: &'static str, allowed: &'static [&'static str]) -> Self {
        Self::new(name, FieldKind::Enum(allowed))
    }
    const fn required(mut self) -> Self {
        self.required = true;
        self
    }
    const fn non_empty(mut self) -> Self {
        self.non_empty = true;
        self
    }
    const fn forbidden(mut self) -> Self {
        self.forbidden = true;
        self
    }
    const fn doc(mut self, doc: &'static str) -> Self {
        self.doc = doc;
        self
    }
}

const HORIZON_SNOWFLAKE_FIELDS: &[FieldSpec] = &[
    FieldSpec::string("external_volume")
        .required()
        .non_empty()
        .doc("Non-empty external volume name registered in Snowflake."),
    FieldSpec::boolean("change_tracking"),
    FieldSpec::u32_max("data_retention_time_in_days", 90)
        .doc("Days to retain table data after deletion. Range 0–90."),
    FieldSpec::u32_max("max_data_extension_time_in_days", 90)
        .doc("Days beyond the retention period to extend data availability. Range 0–90."),
    FieldSpec::enumerated("storage_serialization_policy", STORAGE_SERIALIZATION_POLICIES),
    FieldSpec::u32_plain("iceberg_version").doc("Iceberg spec version, e.g. 3 for Iceberg V3."),
    FieldSpec::string("base_location_root")
        .non_empty()
        .doc("Catalog-wide storage path prefix for all Iceberg tables."),
    FieldSpec::string("base_location_subpath")
        .forbidden()
        .doc("Catalog '{}' horizon/snowflake base_location_subpath is model-config only and may not be specified in catalogs.yml"),
];

const LINKED_SNOWFLAKE_FIELDS: &[FieldSpec] = &[
    FieldSpec::string("catalog_database")
        .required()
        .non_empty()
        .doc("Name of the Snowflake database linked to the external catalog."),
    FieldSpec::boolean("auto_refresh"),
    FieldSpec::u32_max("max_data_extension_time_in_days", 90)
        .doc("Days beyond the retention period to extend data availability. Range 0–90."),
    FieldSpec::enumerated("target_file_size", TARGET_FILE_SIZES),
    FieldSpec::u32_plain("iceberg_version").doc("Iceberg spec version, e.g. 3 for Iceberg V3."),
];

const DUCKDB_ICEBERG_FIELDS: &[FieldSpec] = &[
    FieldSpec::string("endpoint")
        .non_empty()
        .doc("Full REST catalog URL. Mutually exclusive with endpoint_type."),
    FieldSpec::enumerated("endpoint_type", DUCKDB_ENDPOINT_TYPES)
        .doc("Managed endpoint type. Mutually exclusive with endpoint."),
    FieldSpec::string("warehouse")
        .non_empty()
        .doc("S3 warehouse URI. Required when endpoint_type is S3_TABLES."),
    FieldSpec::string("secret")
        .non_empty()
        .doc("Name of a DuckDB secret from profiles.yml to use for authentication."),
    FieldSpec::string("attach_as").non_empty(),
    FieldSpec::string("default_region").non_empty(),
    FieldSpec::string("default_schema").non_empty(),
    FieldSpec::string("max_table_staleness").non_empty(),
    FieldSpec::enumerated("authorization_type", DUCKDB_AUTHORIZATION_TYPES),
    FieldSpec::enumerated("access_delegation_mode", DUCKDB_ACCESS_DELEGATION_MODES),
    FieldSpec::boolean("support_nested_namespaces"),
    FieldSpec::boolean("support_stage_create"),
    FieldSpec::boolean("purge_requested"),
    FieldSpec::boolean("encode_entire_prefix"),
];

const UNITY_DATABRICKS_FIELDS: &[FieldSpec] = &[
    FieldSpec::enumerated("file_format", UNITY_DATABRICKS_FILE_FORMATS).required(),
    FieldSpec::string("location_root").non_empty(),
    FieldSpec::boolean("use_uniform")
        .doc("UniForm mode. true requires file_format: delta; false (default) requires parquet."),
];

const HIVE_METASTORE_DATABRICKS_FIELDS: &[FieldSpec] =
    &[FieldSpec::enumerated("file_format", HIVE_METASTORE_FILE_FORMATS).required()];

const BIGLAKE_BIGQUERY_FIELDS: &[FieldSpec] = &[
    FieldSpec::string("external_volume")
        .required()
        .non_empty()
        .doc("Cloud Storage bucket path (gs://<bucket_name>)."),
    FieldSpec::enumerated("file_format", BIGLAKE_FILE_FORMATS).required(),
    FieldSpec::string("base_location_root").non_empty(),
    FieldSpec::string("connection_id").non_empty(),
];

const DUCKLAKE_DUCKDB_FIELDS: &[FieldSpec] = &[
    FieldSpec::string("metadata_path").required().non_empty(),
    FieldSpec::string("data_path").non_empty(),
    FieldSpec::string("attach_as").non_empty(),
    FieldSpec::string("metadata_schema").non_empty(),
    FieldSpec::boolean("create_if_not_exists"),
    FieldSpec::boolean("read_only"),
    FieldSpec::boolean("encrypted"),
];

const LOCAL_FILESYSTEM_DUCKDB_FIELDS: &[FieldSpec] = &[
    FieldSpec::string("root_path").required().non_empty(),
    FieldSpec::enumerated("file_format", LOCAL_FS_FILE_FORMATS)
        .doc("Local file extension / DuckDB COPY format. Defaults to parquet."),
];

#[derive(Debug, Clone, Copy)]
struct PlatformBlock {
    key: &'static str,
    fields: &'static [FieldSpec],
}

impl PlatformBlock {
    const fn new(key: &'static str, fields: &'static [FieldSpec]) -> Self {
        Self { key, fields }
    }
}

#[derive(Debug, Clone, Copy)]
enum ConfigPresence {
    AllRequired,
    AtLeastOne,
}

#[derive(Debug, Clone, Copy)]
struct CatalogTypeSchema {
    type_name: &'static str,
    table_format: &'static str,
    description: &'static str,
    presence: ConfigPresence,
    platforms: &'static [PlatformBlock],
}

const CATALOG_SCHEMAS: &[CatalogTypeSchema] = &[
    CatalogTypeSchema {
        type_name: "horizon",
        table_format: "iceberg",
        description: "Snowflake-managed Iceberg catalog (Horizon). Snowflake platform only.",
        presence: ConfigPresence::AllRequired,
        platforms: &[PlatformBlock::new("snowflake", HORIZON_SNOWFLAKE_FIELDS)],
    },
    CatalogTypeSchema {
        type_name: "glue",
        table_format: "iceberg",
        description: "AWS Glue catalog. Supports config.snowflake and/or config.duckdb.",
        presence: ConfigPresence::AtLeastOne,
        platforms: &[
            PlatformBlock::new("snowflake", LINKED_SNOWFLAKE_FIELDS),
            PlatformBlock::new("duckdb", DUCKDB_ICEBERG_FIELDS),
        ],
    },
    CatalogTypeSchema {
        type_name: "iceberg_rest",
        table_format: "iceberg",
        description: "Iceberg REST catalog. Supports config.snowflake and/or config.duckdb.",
        presence: ConfigPresence::AtLeastOne,
        platforms: &[
            PlatformBlock::new("snowflake", LINKED_SNOWFLAKE_FIELDS),
            PlatformBlock::new("duckdb", DUCKDB_ICEBERG_FIELDS),
        ],
    },
    CatalogTypeSchema {
        type_name: "unity",
        table_format: "iceberg",
        description: "Databricks Unity catalog. Supports config.snowflake and/or config.databricks.",
        presence: ConfigPresence::AtLeastOne,
        platforms: &[
            PlatformBlock::new("snowflake", LINKED_SNOWFLAKE_FIELDS),
            PlatformBlock::new("databricks", UNITY_DATABRICKS_FIELDS),
        ],
    },
    CatalogTypeSchema {
        type_name: "hive_metastore",
        table_format: "default",
        description: "Databricks Hive Metastore catalog. Databricks platform only.",
        presence: ConfigPresence::AllRequired,
        platforms: &[PlatformBlock::new(
            "databricks",
            HIVE_METASTORE_DATABRICKS_FIELDS,
        )],
    },
    CatalogTypeSchema {
        type_name: "biglake_metastore",
        table_format: "iceberg",
        description: "BigLake Metastore catalog. BigQuery platform only.",
        presence: ConfigPresence::AllRequired,
        platforms: &[PlatformBlock::new("bigquery", BIGLAKE_BIGQUERY_FIELDS)],
    },
    CatalogTypeSchema {
        type_name: "ducklake",
        table_format: "default",
        description: "DuckLake metadata store catalog. DuckDB platform only.",
        presence: ConfigPresence::AllRequired,
        platforms: &[PlatformBlock::new("duckdb", DUCKLAKE_DUCKDB_FIELDS)],
    },
    CatalogTypeSchema {
        type_name: "local_filesystem",
        table_format: "default",
        description: "Local filesystem catalog. DuckDB platform only.",
        presence: ConfigPresence::AllRequired,
        platforms: &[PlatformBlock::new("duckdb", LOCAL_FILESYSTEM_DUCKDB_FIELDS)],
    },
];

// ===== YAML helpers =====

trait StrExt {
    fn is_empty_or_whitespace(&self) -> bool;
}

impl StrExt for str {
    #[inline]
    fn is_empty_or_whitespace(&self) -> bool {
        self.trim().is_empty()
    }
}

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

// ===== Loader Handoff =====

impl DbtCatalogs {
    /// Rebuild a zero-copy typed v2 view over the raw YAML mapping.
    pub fn view_v2(&self) -> FsResult<DbtCatalogsV2View<'_>> {
        DbtCatalogsV2View::from_mapping(&self.repr, &self.span)
    }
}

// ===== Phase 1: Shape Validation =====
// Preconditions:
// - YAML has been loaded and parsed by the caller.
// Postconditions:
// - Document matches the strict v2 envelope: only known top-level and
//   per-entry keys, all required keys present, no duplicate names,
//   config and platform blocks are mappings.
pub fn validate_catalogs_v2_shape(map: &yml::Mapping, span: &yml::Span) -> FsResult<()> {
    if map.get(yml::Value::from("iceberg_catalogs")).is_some() {
        return err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => Some(span.clone()),
            "catalogs.yml v2 uses the key 'catalogs:', not 'iceberg_catalogs:'"
        );
    }

    check_unknown_keys(map, &["catalogs"], "top-level catalogs.yml(v2)").map_err(|_| {
        fs_err!(
            code => ErrorCode::InvalidConfig,
            hacky_yml_loc => Some(span.clone()),
            "catalogs.yml v2 accepts only a top-level 'catalogs:' key"
        )
    })?;

    let catalogs = get_seq(map, "catalogs")?.ok_or_else(|| fs_err!(
        code => ErrorCode::InvalidConfig,
        hacky_yml_loc => Some(span.clone()),
        "catalogs.yml requires a 'catalogs:' list, e.g.:\n  catalogs:\n    - name: my_catalog\n      type: horizon\n      table_format: iceberg\n      config:\n        snowflake: {{ ... }}"
    ))?;
    let mut seen_catalog_names = HashSet::new();

    for (idx, item) in catalogs.iter().enumerate() {
        let item_span = item.span();
        let catalog = require_mapping(item, &format!("catalogs[{idx}]"))
            .map_err(|_| fs_err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(item_span.clone()),
                "Each entry under 'catalogs:' must be a mapping with name, type, table_format, and config"
            ))?;

        check_unknown_keys(
            catalog,
            &["name", "type", "table_format", "config"],
            "catalog entry",
        )?;

        for required in ["name", "type", "table_format", "config"] {
            if !catalog.contains_key(yml::Value::from(required)) {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => Some(item_span.clone()),
                    "Catalog entry is missing '{}'. Each entry requires: name, type, table_format, config",
                    required
                );
            }
        }

        let name = get_str(catalog, "name")?.ok_or_else(|| key_err("name", Some(item_span)))?;
        if name.is_empty_or_whitespace() {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(catalog, "name").cloned(),
                "Catalog name must be a non-empty string"
            );
        }
        if !seen_catalog_names.insert(name) {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(catalog, "name").cloned(),
                "Duplicate catalog name '{}', each catalog must have a unique name",
                name
            );
        }

        if let Some(value) = catalog.get(yml::Value::from("config")) {
            let config = require_mapping(value, &format!("catalogs[{idx}].config"))
                .map_err(|_| fs_err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => Some(value.span().clone()),
                    "Catalog '{}' config must be a mapping of platform blocks (e.g. snowflake:, duckdb:, databricks:, bigquery:)",
                    name
                ))?;
            for &platform in ALL_V2_PLATFORMS {
                if let Some(platform_value) = config.get(yml::Value::from(platform)) {
                    require_mapping(platform_value, platform)
                        .map_err(|_| fs_err!(
                            code => ErrorCode::InvalidConfig,
                            hacky_yml_loc => Some(platform_value.span().clone()),
                            "Catalog '{}' config.{} must be a mapping of key-value configuration fields",
                            name, platform
                        ))?;
                }
            }
        }
    }

    Ok(())
}

// ===== Phases 2+3: Structural + Semantic Validation =====
// Preconditions:
// - Phase 1 shape validation has passed.
// - Raw YAML envelope is well-formed: required keys present, config/platform
//   blocks are mappings, no duplicate names.
// Phase 2 (structural): table_format matches type, platform keys are known and
//   allowed for this type, platform presence (AllRequired/AtLeastOne), per-field
//   type checking, requiredness, non-empty, enum membership, forbidden fields.
// Phase 3 (semantic): cross-field constraints that span multiple fields within
//   a platform block.
// Postconditions:
// - Every catalog entry is fully valid for its type and platform mix.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2CatalogType {
    Horizon,
    Glue,
    IcebergRest,
    HiveMetastore,
    Unity,
    BiglakeMetastore,
    DuckLake,
    LocalFilesystem,
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
        } else if raw.eq_ignore_ascii_case("ducklake") {
            Ok(Self::DuckLake)
        } else if raw.eq_ignore_ascii_case("local_filesystem") {
            Ok(Self::LocalFilesystem)
        } else {
            err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(span.clone()),
                "type '{}' invalid. choose one of (horizon|glue|iceberg_rest|unity|hive_metastore|biglake_metastore|ducklake|local_filesystem)",
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
            Self::DuckLake => "ducklake",
            Self::LocalFilesystem => "local_filesystem",
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
    /// Runs Phase 1 shape validation, then constructs zero-copy typed views.
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

// ===== CatalogRegistry =====

struct CatalogRegistry {
    schemas: &'static [CatalogTypeSchema],
}

impl CatalogRegistry {
    fn new() -> Self {
        Self {
            schemas: CATALOG_SCHEMAS,
        }
    }

    fn type_schema(&self, ct: V2CatalogType) -> FsResult<&'static CatalogTypeSchema> {
        let type_name = ct.as_str();
        self.schemas
            .iter()
            .find(|s| s.type_name == type_name)
            .ok_or_else(|| {
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "Unknown catalog type '{}'",
                    type_name
                )
            })
    }

    fn validate_structural(&self, catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
        let schema = self.type_schema(catalog.catalog_type)?;

        if catalog.table_format.as_str() != schema.table_format {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("table_format").cloned(),
                "Catalog '{}' type '{}' requires table_format='{}'",
                catalog.name, schema.type_name, schema.table_format
            );
        }

        if let Some(k) = catalog
            .config
            .keys()
            .find(|k| k.as_str().is_none_or(|s| !ALL_V2_PLATFORMS.contains(&s)))
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => Some(k.span().clone()),
                "Unknown key '{}' in catalogs[].config",
                k.as_str().unwrap_or("<non-string>")
            );
        }

        if let Some(&platform) = ALL_V2_PLATFORMS.iter().find(|&&p| {
            catalog.config_block(p).is_some() && !schema.platforms.iter().any(|s| s.key == p)
        }) {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => catalog.field_span("type").cloned(),
                "dbt does not support {} on the {} 'type'",
                platform, schema.type_name
            );
        }

        match schema.presence {
            ConfigPresence::AllRequired => {
                if let Some(missing) = schema
                    .platforms
                    .iter()
                    .find(|p| catalog.config_block(p.key).is_none())
                {
                    return err!(
                        code => ErrorCode::InvalidConfig,
                        hacky_yml_loc => catalog.field_span("type").cloned(),
                        "Catalog '{}' type '{}' requires config.{}",
                        catalog.name, schema.type_name, missing.key
                    );
                }
            }
            ConfigPresence::AtLeastOne => {
                if !schema
                    .platforms
                    .iter()
                    .any(|p| catalog.config_block(p.key).is_some())
                {
                    let keys: Vec<&str> = schema.platforms.iter().map(|p| p.key).collect();
                    return err!(
                        code => ErrorCode::InvalidConfig,
                        hacky_yml_loc => catalog.field_span("type").cloned(),
                        "Catalog '{}' of type '{}' requires at least one config block: {}",
                        catalog.name, schema.type_name, keys.join(" or ")
                    );
                }
            }
        }

        for platform in schema.platforms {
            if let Some(block) = catalog.config_block(platform.key) {
                Self::validate_fields(
                    block,
                    platform.fields,
                    &format!("catalogs[].config.{} ({})", platform.key, schema.type_name),
                    catalog.name,
                )?;
            }
        }

        Ok(())
    }

    fn validate_semantic(&self, catalog: &CatalogSpecV2View<'_>) -> FsResult<()> {
        match catalog.catalog_type {
            V2CatalogType::Glue => {
                if let Some(duckdb) = catalog.config_block("duckdb") {
                    Self::validate_duckdb_semantics(duckdb, catalog, "glue")?;
                }
            }
            V2CatalogType::IcebergRest => {
                if let Some(duckdb) = catalog.config_block("duckdb") {
                    Self::validate_duckdb_semantics(duckdb, catalog, "iceberg_rest")?;
                }
            }
            V2CatalogType::Unity => {
                if let Some(databricks) = catalog.config_block("databricks") {
                    Self::validate_unity_semantics(databricks, catalog)?;
                }
            }
            V2CatalogType::BiglakeMetastore => {
                if let Some(bigquery) = catalog.config_block("bigquery") {
                    Self::validate_biglake_semantics(bigquery, catalog)?;
                }
            }
            V2CatalogType::Horizon
            | V2CatalogType::HiveMetastore
            | V2CatalogType::DuckLake
            | V2CatalogType::LocalFilesystem => {}
        }
        Ok(())
    }

    pub fn json_schema(&self) -> serde_json::Value {
        let entries: Vec<serde_json::Value> = self
            .schemas
            .iter()
            .map(|cts| {
                let config_props: serde_json::Map<String, serde_json::Value> = cts
                    .platforms
                    .iter()
                    .map(|p| (p.key.to_string(), Self::fields_schema(p.fields)))
                    .collect();
                let mut config_required = Vec::new();
                let mut config = serde_json::json!({
                    "type": "object",
                    "properties": config_props,
                    "additionalProperties": false,
                });
                match cts.presence {
                    ConfigPresence::AllRequired => {
                        config_required.extend(cts.platforms.iter().map(|p| p.key));
                        config["required"] = serde_json::json!(config_required);
                    }
                    ConfigPresence::AtLeastOne => {
                        config["minProperties"] = serde_json::json!(1);
                    }
                }
                serde_json::json!({
                    "type": "object",
                    "description": cts.description,
                    "required": ["name", "type", "table_format", "config"],
                    "additionalProperties": false,
                    "properties": {
                        "name": {
                            "type": "string",
                            "minLength": 1,
                            "description": "Unique catalog name within this project.",
                        },
                        "type": { "const": cts.type_name },
                        "table_format": { "const": cts.table_format },
                        "config": config,
                    },
                })
            })
            .collect();

        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "DbtCatalogsFile",
            "description": "Top-level `catalogs.yml` (v2) schema.",
            "type": "object",
            "required": ["catalogs"],
            "additionalProperties": false,
            "properties": {
                "catalogs": {
                    "type": "array",
                    "items": { "oneOf": entries },
                },
            },
        })
    }

    fn fields_schema(fields: &[FieldSpec]) -> serde_json::Value {
        let mut props = serde_json::Map::new();
        let mut required = Vec::new();
        for f in fields.iter().filter(|f| !f.forbidden) {
            let mut schema = match f.kind {
                FieldKind::Str => serde_json::json!({ "type": "string" }),
                FieldKind::Bool => serde_json::json!({ "type": "boolean" }),
                FieldKind::U32 { max } => {
                    let mut num = serde_json::json!({ "type": "integer", "minimum": 0 });
                    if let Some(m) = max {
                        num["maximum"] = serde_json::json!(m);
                    }
                    num
                }
                FieldKind::Enum(allowed) => {
                    serde_json::json!({ "type": "string", "enum": allowed })
                }
            };
            if f.non_empty && matches!(f.kind, FieldKind::Str) {
                schema["minLength"] = serde_json::json!(1);
            }
            if !f.doc.is_empty() {
                schema["description"] = serde_json::json!(f.doc);
            }
            props.insert(f.name.to_string(), schema);
            if f.required {
                required.push(f.name);
            }
        }
        let mut obj = serde_json::json!({
            "type": "object",
            "properties": props,
            "additionalProperties": false,
        });
        if !required.is_empty() {
            obj["required"] = serde_json::json!(required);
        }
        obj
    }

    fn validate_fields(
        map: &yml::Mapping,
        fields: &[FieldSpec],
        ctx: &str,
        catalog_name: &str,
    ) -> FsResult<()> {
        for k in map.keys() {
            let span = k.span();
            let Some(ks) = k.as_str() else {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => Some(span.clone()),
                    "Non-string key in {}",
                    ctx
                );
            };
            if !fields.iter().any(|f| f.name == ks) {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => Some(span.clone()),
                    "Unknown key '{}' in {}",
                    ks, ctx
                );
            }
        }

        if let Some(f) = fields
            .iter()
            .filter(|f| f.forbidden)
            .find(|f| field_span(map, f.name).is_some())
        {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(map, f.name).cloned(),
                "{}",
                f.doc.replace("{}", catalog_name)
            );
        }

        for f in fields.iter().filter(|f| !f.forbidden) {
            match f.kind {
                FieldKind::Str => {
                    let val = get_str(map, f.name)?;
                    if f.required && val.is_none() {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            hacky_yml_loc => field_span(map, f.name).cloned(),
                            "Catalog '{}' {} requires '{}'",
                            catalog_name, ctx, f.name
                        );
                    }
                    if f.non_empty {
                        if let Some(v) = val {
                            if v.is_empty_or_whitespace() {
                                return err!(
                                    code => ErrorCode::InvalidConfig,
                                    hacky_yml_loc => field_span(map, f.name).cloned(),
                                    "Catalog '{}' {} '{}' must be non-empty",
                                    catalog_name, ctx, f.name
                                );
                            }
                        }
                    }
                }
                FieldKind::Bool => {
                    let val = match try_get_bool(map, f.name) {
                        Ok(v) => v,
                        Err(_) if map.get(yml::Value::from(f.name)).is_some() => {
                            return Err(fs_err!(
                                code => ErrorCode::InvalidConfig,
                                hacky_yml_loc => field_span(map, f.name).cloned(),
                                "Key '{}' must be a boolean",
                                f.name
                            ));
                        }
                        Err(e) => return Err(e),
                    };
                    if f.required && val.is_none() {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            hacky_yml_loc => field_span(map, f.name).cloned(),
                            "Catalog '{}' {} requires '{}'",
                            catalog_name, ctx, f.name
                        );
                    }
                }
                FieldKind::U32 { max } => {
                    let val = get_u32(map, f.name)?;
                    if f.required && val.is_none() {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            hacky_yml_loc => field_span(map, f.name).cloned(),
                            "Catalog '{}' {} requires '{}'",
                            catalog_name, ctx, f.name
                        );
                    }
                    if let Some(v) = val {
                        if let Some(m) = max {
                            if v > m {
                                return err!(
                                    code => ErrorCode::InvalidConfig,
                                    hacky_yml_loc => field_span(map, f.name).cloned(),
                                    "Key '{}' must be in 0..={}",
                                    f.name, m
                                );
                            }
                        }
                    }
                }
                FieldKind::Enum(allowed) => {
                    let val = get_str(map, f.name)?;
                    if f.required && val.is_none() {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            hacky_yml_loc => field_span(map, f.name).cloned(),
                            "Catalog '{}' {} requires '{}'",
                            catalog_name, ctx, f.name
                        );
                    }
                    if let Some(v) = val {
                        if !matches_enum_ci(v, allowed) {
                            let choices = allowed.join("|");
                            return err!(
                                code => ErrorCode::InvalidConfig,
                                hacky_yml_loc => field_span(map, f.name).cloned(),
                                "{} '{}' invalid ({})",
                                f.name, v, choices
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_duckdb_semantics(
        duckdb: &yml::Mapping,
        catalog: &CatalogSpecV2View<'_>,
        type_name: &str,
    ) -> FsResult<()> {
        let has_endpoint = get_str(duckdb, "endpoint")?;
        let has_endpoint_type = get_str(duckdb, "endpoint_type")?;

        match (has_endpoint, has_endpoint_type) {
            (None, None) => {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => catalog.field_span("type").cloned(),
                    "Catalog '{}' {}/duckdb config requires 'endpoint' or 'endpoint_type'",
                    catalog.name, type_name
                );
            }
            (Some(ep), Some(_)) if !ep.is_empty_or_whitespace() => {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => field_span(duckdb, "endpoint_type").cloned(),
                    "Catalog '{}' {}/duckdb 'endpoint' and 'endpoint_type' are mutually exclusive",
                    catalog.name, type_name
                );
            }
            (Some(ep), _) if ep.is_empty_or_whitespace() => {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => field_span(duckdb, "endpoint").cloned(),
                    "Catalog '{}' {}/duckdb 'endpoint' must be non-empty",
                    catalog.name, type_name
                );
            }
            (_, Some(et)) => {
                let val = et.trim();
                if !matches_enum_ci(val, DUCKDB_ENDPOINT_TYPES) {
                    return err!(
                        code => ErrorCode::InvalidConfig,
                        hacky_yml_loc => field_span(duckdb, "endpoint_type").cloned(),
                        "Catalog '{}' {}/duckdb 'endpoint_type' must be 'GLUE' or 'S3_TABLES'",
                        catalog.name, type_name
                    );
                }
                if val.eq_ignore_ascii_case("S3_TABLES") {
                    let Some(warehouse) = get_str(duckdb, "warehouse")? else {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            hacky_yml_loc => field_span(duckdb, "endpoint_type").cloned(),
                            "Catalog '{}' {}/duckdb endpoint_type='S3_TABLES' requires 'warehouse'",
                            catalog.name, type_name
                        );
                    };
                    if warehouse.is_empty_or_whitespace() {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            hacky_yml_loc => field_span(duckdb, "warehouse").cloned(),
                            "Catalog '{}' {}/duckdb 'warehouse' must be non-empty",
                            catalog.name, type_name
                        );
                    }
                }
            }
            _ => {}
        }

        if let Some(_auth_type) = get_str(duckdb, "authorization_type")? {
            if has_endpoint_type.is_some() {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    hacky_yml_loc => field_span(duckdb, "authorization_type").cloned(),
                    "Catalog '{}' {}/duckdb 'authorization_type' cannot be combined with 'endpoint_type'",
                    catalog.name, type_name
                );
            }
        }

        Ok(())
    }

    fn validate_unity_semantics(
        databricks: &yml::Mapping,
        catalog: &CatalogSpecV2View<'_>,
    ) -> FsResult<()> {
        let file_format_str = get_str(databricks, "file_format")?
            .expect("structural validation ensures file_format is present");
        let file_format_span = field_span(databricks, "file_format").cloned();
        let file_format = V2FileFormat::parse(file_format_str, file_format_span.clone())?;
        let use_uniform =
            UniformMode::from_bool(try_get_bool(databricks, "use_uniform")?.unwrap_or(false));

        match (file_format, use_uniform) {
            (V2FileFormat::Delta, UniformMode::Enabled)
            | (V2FileFormat::Parquet, UniformMode::Disabled) => Ok(()),
            (V2FileFormat::Delta, UniformMode::Disabled) => err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => file_format_span,
                "Catalog '{}' unity/databricks use_uniform: false (or unset) requires file_format: parquet",
                catalog.name
            ),
            (V2FileFormat::Parquet, UniformMode::Enabled) => err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => file_format_span,
                "Catalog '{}' unity/databricks use_uniform: true requires file_format: delta",
                catalog.name
            ),
            (V2FileFormat::Hudi, _) => err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => file_format_span,
                "Catalog '{}' unity/databricks file_format 'hudi' is not valid for unity (use delta or parquet)",
                catalog.name
            ),
        }
    }

    fn validate_biglake_semantics(
        bigquery: &yml::Mapping,
        catalog: &CatalogSpecV2View<'_>,
    ) -> FsResult<()> {
        let external_volume = get_str(bigquery, "external_volume")?
            .expect("structural validation ensures external_volume is present");
        if !external_volume.starts_with("gs://") {
            return err!(
                code => ErrorCode::InvalidConfig,
                hacky_yml_loc => field_span(bigquery, "external_volume").cloned(),
                "Catalog '{}' biglake_metastore/bigquery 'external_volume' must be a GCS path starting with gs://",
                catalog.name
            );
        }
        Ok(())
    }
}

pub fn validate_catalogs_v2(spec: &DbtCatalogsV2View<'_>, _path: &Path) -> FsResult<()> {
    let registry = CatalogRegistry::new();
    for catalog in &spec.catalogs {
        registry.validate_structural(catalog)?;
        registry.validate_semantic(catalog)?;
    }
    Ok(())
}

/// Build the draft-07 JSON schema for `catalogs.yml` (v2), used by
/// `dbt man --schema catalog`. The schema is generated from the same
/// `CATALOG_SCHEMAS` / `FieldSpec` descriptor tables that drive validation, so
/// it cannot drift from the parser. Each catalog `type` becomes a `oneOf`
/// branch discriminated by a `const` `type`, so editors narrow `config` field
/// completions to those valid for the selected catalog type.
pub fn catalogs_v2_json_schema() -> serde_json::Value {
    CatalogRegistry::new().json_schema()
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
    fn biglake_accepts_connection_id() {
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
        connection_id: "cool_connection"
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
            msg.contains("not 'iceberg_catalogs:'"),
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
            msg.contains("file_format") && msg.contains("invalid") && msg.contains("delta"),
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
            msg.contains("Unknown key 'file_format' in catalog entry"),
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
            format!("{res:?}").contains("endpoint_type") && format!("{res:?}").contains("invalid"),
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
            format!("{res:?}").contains("authorization_type")
                && format!("{res:?}").contains("invalid"),
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
            format!("{res:?}").contains("access_delegation_mode")
                && format!("{res:?}").contains("invalid"),
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

    // ===== DuckLake tests =====

    #[test]
    fn ducklake_duckdb_v2_valid() {
        let yaml = r#"
catalogs:
  - name: my_lake
    type: ducklake
    table_format: default
    config:
      duckdb:
        metadata_path: "metadata.ducklake"
"#;
        parse_and_validate(yaml).expect("ducklake minimal config should validate");
    }

    #[test]
    fn ducklake_duckdb_v2_all_options() {
        let yaml = r#"
catalogs:
  - name: my_lake
    type: ducklake
    table_format: default
    config:
      duckdb:
        metadata_path: "metadata.ducklake"
        data_path: "data/"
        attach_as: "lake"
        metadata_schema: "my_schema"
        create_if_not_exists: true
        read_only: false
        encrypted: false
"#;
        parse_and_validate(yaml).expect("ducklake full config should validate");
    }

    #[test]
    fn ducklake_missing_metadata_path() {
        let yaml = r#"
catalogs:
  - name: my_lake
    type: ducklake
    table_format: default
    config:
      duckdb:
        data_path: "data/"
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(msg.contains("'metadata_path'"), "unexpected error: {msg}");
    }

    #[test]
    fn ducklake_wrong_table_format() {
        let yaml = r#"
catalogs:
  - name: my_lake
    type: ducklake
    table_format: iceberg
    config:
      duckdb:
        metadata_path: "metadata.ducklake"
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("requires table_format='default'"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn ducklake_snowflake_block_rejected() {
        let yaml = r#"
catalogs:
  - name: my_lake
    type: ducklake
    table_format: default
    config:
      snowflake:
        external_volume: "EV"
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("does not support snowflake on the ducklake"),
            "unexpected error: {msg}"
        );
    }

    // ===== Local filesystem tests =====

    #[test]
    fn local_filesystem_duckdb_v2_valid() {
        let yaml = r#"
catalogs:
  - name: local_files
    type: local_filesystem
    table_format: default
    config:
      duckdb:
        root_path: "data/local_files"
        file_format: parquet
"#;
        parse_and_validate(yaml).expect("local filesystem config should validate");
    }

    #[test]
    fn local_filesystem_missing_root_path() {
        let yaml = r#"
catalogs:
  - name: local_files
    type: local_filesystem
    table_format: default
    config:
      duckdb:
        file_format: parquet
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(msg.contains("'root_path'"), "unexpected error: {msg}");
    }

    #[test]
    fn local_filesystem_wrong_table_format() {
        let yaml = r#"
catalogs:
  - name: local_files
    type: local_filesystem
    table_format: iceberg
    config:
      duckdb:
        root_path: "data/local_files"
"#;
        let res = parse_and_validate(yaml);
        let msg = format!("{res:?}");
        assert!(res.is_err(), "expected error but got Ok");
        assert!(
            msg.contains("requires table_format='default'"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn json_schema_round_trips_and_covers_all_types() {
        let schema = CatalogRegistry::new().json_schema();
        let pretty = serde_json::to_string_pretty(&schema).unwrap();
        let reparsed: serde_json::Value = serde_json::from_str(&pretty).unwrap();
        assert_eq!(schema, reparsed, "schema must round-trip through JSON");

        let items = schema["properties"]["catalogs"]["items"]["oneOf"]
            .as_array()
            .expect("oneOf array");
        let type_names: Vec<&str> = items
            .iter()
            .filter_map(|i| i["properties"]["type"]["const"].as_str())
            .collect();
        for cts in CATALOG_SCHEMAS {
            assert!(
                type_names.contains(&cts.type_name),
                "missing {}",
                cts.type_name
            );
        }
    }

    /// Drift guard: the schema's accepted config fields for every
    /// `(type, platform)` block must match the descriptor tables the validators
    /// use, modulo `forbidden` fields (which are model-config only and excluded
    /// from the published schema). Adding or renaming a `FieldSpec` updates both
    /// the accepted-key set and the published schema together.
    #[test]
    fn schema_config_fields_match_descriptor_tables() {
        let schema = catalogs_v2_json_schema();
        let branches = schema["properties"]["catalogs"]["items"]["oneOf"]
            .as_array()
            .expect("oneOf array");
        for cts in CATALOG_SCHEMAS {
            let branch = branches
                .iter()
                .find(|b| b["properties"]["type"]["const"] == serde_json::json!(cts.type_name))
                .unwrap_or_else(|| panic!("missing schema branch for {}", cts.type_name));
            let config_props = branch["properties"]["config"]["properties"]
                .as_object()
                .expect("config properties");

            let mut schema_platforms: Vec<&str> = config_props.keys().map(String::as_str).collect();
            schema_platforms.sort_unstable();
            let mut table_platforms: Vec<&str> = cts.platforms.iter().map(|p| p.key).collect();
            table_platforms.sort_unstable();
            assert_eq!(
                schema_platforms, table_platforms,
                "platform blocks for {} diverge",
                cts.type_name
            );

            for platform in cts.platforms {
                let mut schema_fields: Vec<&str> = config_props[platform.key]["properties"]
                    .as_object()
                    .expect("platform field properties")
                    .keys()
                    .map(String::as_str)
                    .collect();
                schema_fields.sort_unstable();
                let mut table_fields: Vec<&str> = platform
                    .fields
                    .iter()
                    .filter(|f| !f.forbidden)
                    .map(|f| f.name)
                    .collect();
                table_fields.sort_unstable();
                assert_eq!(
                    schema_fields, table_fields,
                    "schema fields for {}/{} diverge from the descriptor table",
                    cts.type_name, platform.key
                );
            }
        }
    }
}
