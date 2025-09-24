//! dbt_catalogs.yml schema: typed, borrowed view + validation (no IO).
//!
//! Top-level YAML shape (strict keys):
//!   catalogs:
//!     - name|catalog_name: <string, non-empty>
//!       active_write_integration: <string, non-empty>
//!       write_integrations:
//!         - name|integration_name: <string, non-empty>
//!           catalog_type: built_in | iceberg_rest                // case-insensitive
//!           table_format: iceberg                                // default; case-insensitive
//!           external_volume: <string>                            // required & non-empty for built_in; required for REST write-support
//!           base_location_root: <string>                         // built_in only; if present, non-empty
//!           base_location_subpath: <string>                      // built_in only; if present, non-empty
//!           catalog_linked_database: <string>                    // REST write-support gate
//!           adapter_properties:                                  // variant-specific; strict keys
//!             // built_in:
//!             storage_serialization_policy: COMPATIBLE|OPTIMIZED // case-insensitive
//!             data_retention_time_in_days: <u32 in 0..=90>
//!             max_data_extension_time_in_days: <u32 in 0..=90>
//!             change_tracking: <bool>
//!             target_file_size: 'AUTO'|'16MB'|'32MB'|'64MB'|'128MB'  // case-insensitive
//!             // REST:
//!             target_file_size: 'AUTO'|'16MB'|'32MB'|'64MB'|'128MB'  // case-insensitive
//!             auto_refresh: <bool>
//!             max_data_extension_time_in_days: <u32 in 0..=90>
//!             catalog_namespace: <string, if present non-empty>

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use dbt_common::{ErrorCode, FsResult, err, fs_err};
use dbt_serde_yaml as yml;

/// A validated catalogs.yml mapping.
/// - Holds the raw YAML Mapping so external consumers can copy as needed.
/// - Provides a borrowed, zero-copy typed view on demand.
#[derive(Debug, Clone)]
pub struct DbtCatalogs {
    pub repr: yml::Mapping,
}

impl DbtCatalogs {
    /// Borrow the validated raw mapping.
    pub fn mapping(&self) -> &yml::Mapping {
        &self.repr
    }

    /// Move out the validated raw mapping (for out-of-crate consumers to copy from).
    pub fn into_mapping(self) -> yml::Mapping {
        self.repr
    }

    /// Borrowed, zero-copy typed view (validated shape).
    pub fn view(&self) -> FsResult<DbtCatalogsView<'_>> {
        DbtCatalogsView::from_mapping(&self.repr)
    }

    /// Ergonomic helper: return the `catalogs` sequence (validated presence).
    pub fn catalogs_seq(&self) -> FsResult<&yml::Sequence> {
        self.repr
            .get(yml::Value::from("catalogs"))
            .and_then(|yaml_value| yaml_value.as_sequence())
            .ok_or_else(|| fs_err!(ErrorCode::InvalidConfig, "Missing 'catalogs'"))
    }
}

// If adding a new enum type, also add the expected string to match it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogType {
    BuiltIn,
    IcebergRest,
}

const CATALOG_TYPES: [(&str, CatalogType); 2] = [
    ("built_in", CatalogType::BuiltIn),
    ("iceberg_rest", CatalogType::IcebergRest),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    Iceberg,
}

#[derive(Debug, Default)]
pub struct BuiltInPropsView {
    pub storage_serialization_policy: Option<SerializationPolicy>,
    pub max_data_extension_time_in_days: Option<u32>,
    pub data_retention_time_in_days: Option<u32>,
    pub change_tracking: Option<bool>,
    pub target_file_size: Option<TargetFileSize>,
}

#[derive(Debug, Default)]
pub struct RestPropsView<'a> {
    pub target_file_size: Option<TargetFileSize>,
    pub auto_refresh: Option<bool>,
    pub max_data_extension_time_in_days: Option<u32>,
    pub catalog_namespace: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationPolicy {
    COMPATIBLE,
    OPTIMIZED,
}

/// 'AUTO' | '16MB' | '32MB' | '64MB' | '128MB'
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetFileSize {
    AUTO,
    _16MB,
    _32MB,
    _64MB,
    _128MB,
}

#[derive(Debug)]
pub enum AdapterPropsView<'a> {
    BuiltIn(BuiltInPropsView),
    Rest(RestPropsView<'a>),
}

fn get_str<'a>(m: &'a yml::Mapping, k: &str) -> Option<&'a str> {
    m.get(yml::Value::from(k)).and_then(|v| v.as_str())
}

fn get_map<'a>(m: &'a yml::Mapping, k: &str) -> Option<&'a yml::Mapping> {
    m.get(yml::Value::from(k)).and_then(|v| v.as_mapping())
}

fn get_seq<'a>(m: &'a yml::Mapping, k: &str) -> Option<&'a yml::Sequence> {
    m.get(yml::Value::from(k)).and_then(|v| v.as_sequence())
}

fn get_bool(m: &yml::Mapping, k: &str) -> FsResult<Option<bool>> {
    m.get(yml::Value::from(k))
        .map(|v| {
            v.as_bool()
                .ok_or_else(|| fs_err!(ErrorCode::InvalidConfig, "Key '{}' must be a boolean", k))
        })
        .transpose()
}

fn get_u32(m: &yml::Mapping, k: &str) -> FsResult<Option<u32>> {
    m.get(yml::Value::from(k))
        .map(|v| {
            v.as_i64()
                .and_then(|i| u32::try_from(i).ok())
                .ok_or_else(|| {
                    fs_err!(
                        ErrorCode::InvalidConfig,
                        "Key '{}' must be a non-negative integer",
                        k
                    )
                })
        })
        .transpose()
}

fn key_err(key: &str) -> Box<dbt_common::FsError> {
    fs_err!(
        ErrorCode::InvalidConfig,
        "Missing required key '{}' in catalogs.yml",
        key
    )
}

fn is_blank(s: &str) -> bool {
    s.trim().is_empty()
}

fn check_unknown_keys(m: &yml::Mapping, allowed: &[&str], ctx: &str) -> FsResult<()> {
    for k in m.keys() {
        let Some(ks) = k.as_str() else {
            return err!(ErrorCode::InvalidConfig, "Non-string key in {}", ctx);
        };
        if !allowed.iter().any(|a| a == &ks) {
            return err!(ErrorCode::InvalidConfig, "Unknown key '{}' in {}", ks, ctx);
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct WriteIntegrationView<'a> {
    pub integration_name: &'a str, // alias: name
    pub catalog_type: CatalogType,
    pub table_format: TableFormat, // defaults to iceberg
    pub external_volume: Option<&'a str>,

    // built_in only
    pub base_location_root: Option<&'a str>,
    pub base_location_subpath: Option<&'a str>,

    pub adapter_properties: Option<AdapterPropsView<'a>>,
    // REST linked-DB gate (forward compat)
    pub catalog_linked_database: Option<&'a str>,
}

#[derive(Debug)]
pub struct CatalogSpecView<'a> {
    pub catalog_name: &'a str, // alias: name
    pub active_write_integration: &'a str,
    pub write_integrations: Vec<WriteIntegrationView<'a>>,
}

#[derive(Debug)]
pub struct DbtCatalogsView<'a> {
    pub catalogs: Vec<CatalogSpecView<'a>>,
}

impl<'a> DbtCatalogsView<'a> {
    pub fn from_mapping(map: &'a yml::Mapping) -> FsResult<Self> {
        check_unknown_keys(map, &["catalogs"], "top-level catalogs.yml")?;

        let catalog_entries = get_seq(map, "catalogs").ok_or_else(|| key_err("catalogs"))?;

        let mut catalogs = Vec::with_capacity(catalog_entries.len());
        for (idx, item) in catalog_entries.iter().enumerate() {
            let m = item.as_mapping().ok_or_else(|| {
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "catalogs[{idx}] must be a mapping"
                )
            })?;
            catalogs.push(CatalogSpecView::from_mapping(m)?);
        }
        Ok(Self { catalogs })
    }
}

impl<'a> CatalogSpecView<'a> {
    pub fn from_mapping(map: &'a yml::Mapping) -> FsResult<Self> {
        check_unknown_keys(
            map,
            &[
                "catalog_name",
                "name",
                "active_write_integration",
                "write_integrations",
            ],
            "catalog specification",
        )?;

        if get_str(map, "catalog_name").is_some() && get_str(map, "name").is_some() {
            return err!(
                ErrorCode::InvalidConfig,
                "Both 'catalog_name' and 'name' provided in catalog specification; provide exactly one"
            );
        }

        let catalog_name = get_str(map, "catalog_name")
            .or_else(|| get_str(map, "name"))
            .ok_or_else(|| key_err("catalog_name|name"))?;

        let active = get_str(map, "active_write_integration")
            .ok_or_else(|| key_err("active_write_integration"))?;

        let write_integration_entries =
            get_seq(map, "write_integrations").ok_or_else(|| key_err("write_integrations"))?;
        let mut write_integrations = Vec::with_capacity(write_integration_entries.len());
        for (idx, item) in write_integration_entries.iter().enumerate() {
            let write_integration_mapping = item.as_mapping().ok_or_else(|| {
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "write_integrations[{idx}] must be a mapping"
                )
            })?;
            write_integrations.push(WriteIntegrationView::from_mapping(
                write_integration_mapping,
            )?);
        }
        Ok(Self {
            catalog_name,
            active_write_integration: active,
            write_integrations,
        })
    }
}

impl<'a> WriteIntegrationView<'a> {
    pub fn from_mapping(map: &'a yml::Mapping) -> FsResult<Self> {
        check_unknown_keys(
            map,
            &[
                "integration_name",
                "name",
                "catalog_type",
                "table_format",
                "external_volume",
                "base_location_root",
                "base_location_subpath",
                "adapter_properties",
                "catalog_linked_database",
            ],
            "write_integration",
        )?;

        if get_str(map, "integration_name").is_some() && get_str(map, "name").is_some() {
            return err!(
                ErrorCode::InvalidConfig,
                "Both 'integration_name' and 'name' provided in write_integration; provide exactly one"
            );
        }

        if map.get(yml::Value::from("adapter_properties")).is_some()
            && get_map(map, "adapter_properties").is_none()
        {
            return err!(
                ErrorCode::InvalidConfig,
                "adapter_properties must be a mapping"
            );
        }

        let integration_name = get_str(map, "integration_name")
            .or_else(|| get_str(map, "name"))
            .ok_or_else(|| key_err("integration_name|name"))?;

        let catalog_type_raw =
            get_str(map, "catalog_type").ok_or_else(|| key_err("catalog_type"))?;
        let catalog_type = CATALOG_TYPES
            .iter()
            .find_map(|(name, v)| catalog_type_raw.eq_ignore_ascii_case(name).then_some(*v))
            .ok_or_else(|| {
                let opts = CATALOG_TYPES
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join("|");
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "catalog_type '{}' invalid. choose one of ({})",
                    catalog_type_raw,
                    opts
                )
            })?;

        let table_format_raw =
            get_str(map, "table_format").ok_or_else(|| key_err("table_format"))?;
        let table_format = if table_format_raw.eq_ignore_ascii_case("iceberg") {
            TableFormat::Iceberg
        } else {
            return err!(
                ErrorCode::InvalidConfig,
                "table_format '{}' invalid (only 'iceberg' supported)",
                table_format_raw
            );
        };

        let external_volume = get_str(map, "external_volume");
        let base_location_root = get_str(map, "base_location_root");
        let base_location_subpath = get_str(map, "base_location_subpath");

        let catalog_linked_database = get_str(map, "catalog_linked_database");

        let adapter_properties = get_map(map, "adapter_properties")
            .map(|properties| parse_adapter_properties(properties, catalog_type))
            .transpose()?;

        Ok(Self {
            integration_name,
            catalog_type,
            table_format,
            external_volume,
            base_location_root,
            base_location_subpath,
            adapter_properties,
            catalog_linked_database,
        })
    }
}

fn parse_adapter_properties<'a>(
    properties: &'a yml::Mapping,
    catalog_type: CatalogType,
) -> FsResult<AdapterPropsView<'a>> {
    match catalog_type {
        CatalogType::BuiltIn => {
            check_unknown_keys(
                properties,
                &[
                    "storage_serialization_policy",
                    "data_retention_time_in_days",
                    "max_data_extension_time_in_days",
                    "change_tracking",
                    "target_file_size",
                ],
                "adapter_properties(built_in)",
            )?;
            Ok(AdapterPropsView::BuiltIn(BuiltInPropsView {
                storage_serialization_policy: get_str(properties, "storage_serialization_policy")
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| {
                        if s.eq_ignore_ascii_case("compatible") {
                            Ok(SerializationPolicy::COMPATIBLE)
                        } else if s.eq_ignore_ascii_case("optimized") {
                            Ok(SerializationPolicy::OPTIMIZED)
                        } else {
                            err!(
                                ErrorCode::InvalidConfig,
                                "storage_serialization_policy '{}' invalid (COMPATIBLE|OPTIMIZED)",
                                s
                            )
                        }
                    })
                    .transpose()?,
                max_data_extension_time_in_days: get_u32(
                    properties,
                    "max_data_extension_time_in_days",
                )?,
                data_retention_time_in_days: get_u32(properties, "data_retention_time_in_days")?,
                change_tracking: get_bool(properties, "change_tracking")?,
                target_file_size: target_file_size(get_str(properties, "target_file_size"))?,
            }))
        }
        CatalogType::IcebergRest => {
            check_unknown_keys(
                properties,
                &[
                    "target_file_size",
                    "auto_refresh",
                    "max_data_extension_time_in_days",
                    "catalog_namespace",
                ],
                "adapter_properties(iceberg_rest)",
            )?;
            Ok(AdapterPropsView::Rest(RestPropsView {
                target_file_size: target_file_size(get_str(properties, "target_file_size"))?,
                auto_refresh: get_bool(properties, "auto_refresh")?,
                max_data_extension_time_in_days: get_u32(
                    properties,
                    "max_data_extension_time_in_days",
                )?,
                catalog_namespace: get_str(properties, "catalog_namespace"),
            }))
        }
    }
}

fn target_file_size(maybe_target_file_size: Option<&str>) -> FsResult<Option<TargetFileSize>> {
    maybe_target_file_size
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| {
            if v.eq_ignore_ascii_case("AUTO") {
                Ok(TargetFileSize::AUTO)
            } else if v.eq_ignore_ascii_case("16MB") {
                Ok(TargetFileSize::_16MB)
            } else if v.eq_ignore_ascii_case("32MB") {
                Ok(TargetFileSize::_32MB)
            } else if v.eq_ignore_ascii_case("64MB") {
                Ok(TargetFileSize::_64MB)
            } else if v.eq_ignore_ascii_case("128MB") {
                Ok(TargetFileSize::_128MB)
            } else {
                err!(
                    ErrorCode::InvalidConfig,
                    "target_file_size '{}' invalid (AUTO|16MB|32MB|64MB|128MB)",
                    v
                )
            }
        })
        .transpose()
}

// Structurally recursive down the struct of a catalog entry and validate
pub fn validate_catalogs(spec: &DbtCatalogsView<'_>, path: &Path) -> FsResult<()> {
    let err_loc = || PathBuf::from(path);

    // === 1. Ensure no duplicate-name catalogs, ensure no blanks. One pass.
    let mut seen_catalogs = HashSet::new();
    for catalog in &spec.catalogs {
        if is_blank(catalog.catalog_name) {
            return err!(
                code => ErrorCode::InvalidConfig,
                loc  => err_loc(),
                "Catalog field 'name' or 'catalog_name' must be non-empty"
            );
        }
        if !seen_catalogs.insert(catalog.catalog_name) {
            return err!(
                code => ErrorCode::InvalidConfig,
                loc  => err_loc(),
                "Duplicate catalog name '{}'", catalog.catalog_name
            );
        }
    }

    for catalog in &spec.catalogs {
        // === 2. Every catalog requires an active write integration
        if is_blank(catalog.active_write_integration) {
            return err!(
                code => ErrorCode::InvalidConfig,
                loc  => err_loc(),
                "In catalog '{}', 'active_write_integration' must be non-empty",
                catalog.catalog_name
            );
        }

        // === 3. Avoid duplicate write integrations
        let mut seen = HashSet::new();
        catalog
            .write_integrations
            .iter()
            .try_for_each(|write_integration| {
                seen.insert(write_integration.integration_name)
                    .then_some(())
                    .ok_or_else(|| {
                        fs_err!(
                            code => ErrorCode::InvalidConfig,
                            loc  => err_loc(),
                            "Duplicate write_integration name '{}' in catalog '{}'",
                            write_integration.integration_name, catalog.catalog_name
                        )
                    })
            })?;

        // === 4. Ensure named write integration actually exists
        if catalog
            .write_integrations
            .iter()
            .all(|w| w.integration_name != catalog.active_write_integration)
        {
            let choices = if catalog.write_integrations.is_empty() {
                "<none>".to_string()
            } else {
                catalog
                    .write_integrations
                    .iter()
                    .map(|w| w.integration_name)
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            return err!(
                code => ErrorCode::InvalidConfig,
                loc  => err_loc(),
                "In catalog '{}', active_write_integration '{}' not found. Available: {}",
                catalog.catalog_name, catalog.active_write_integration, choices
            );
        }

        // === 5. Validate write integration entries
        for write_integration in &catalog.write_integrations {
            // must have a name
            if is_blank(write_integration.integration_name) {
                return err!(
                    code => ErrorCode::InvalidConfig,
                    loc  => err_loc(),
                    "Catalog '{}' integration has no field 'name' or 'integration_name'",
                    catalog.catalog_name
                );
            }

            // === 6. Validate other write integration fields modulo catalog type
            match write_integration.catalog_type {
                // === 6a. built in catalog type
                CatalogType::BuiltIn => {
                    let ext_vol_missing = write_integration
                        .external_volume
                        .map(|s| s.trim().is_empty())
                        .unwrap_or(true);
                    if ext_vol_missing {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            loc  => err_loc(),
                            "Catalog '{}' integration '{}': 'external_volume' must be set for built_in",
                            catalog.catalog_name, write_integration.integration_name
                        );
                    }
                }

                // === 6b. iceberg rest catalog type
                CatalogType::IcebergRest => {
                    // REST forbids base_location_* fields
                    if write_integration.base_location_root.is_some()
                        || write_integration.base_location_subpath.is_some()
                    {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            loc  => err_loc(),
                            "Catalog '{}' integration '{}': base_location_* not valid for iceberg_rest",
                            catalog.catalog_name, write_integration.integration_name
                        );
                    }

                    // REST: blank catalog_namespace invalid if field listed
                    if let Some(AdapterPropsView::Rest(rp)) = &write_integration.adapter_properties
                    {
                        if let Some(ns) = rp.catalog_namespace {
                            if is_blank(ns) {
                                return err!(
                                    code => ErrorCode::InvalidConfig,
                                    loc  => err_loc(),
                                    "Catalog '{}' integration '{}': 'catalog_namespace' cannot be blank",
                                    catalog.catalog_name, write_integration.integration_name
                                );
                            }
                        }
                    }

                    let write_properties_set = match &write_integration.adapter_properties {
                        Some(AdapterPropsView::Rest(rest_properties)) => {
                            rest_properties.max_data_extension_time_in_days.is_some()
                                || rest_properties.target_file_size.is_some()
                        }
                        _ => false,
                    };

                    let catalog_linked_database_name: Option<&str> = write_integration
                        .catalog_linked_database
                        .map(|s| {
                            let t = s.trim();
                            if t.is_empty() {
                                err!(
                                    code => ErrorCode::InvalidConfig,
                                    loc  => err_loc(),
                                    "Catalog '{}' integration '{}': 'catalog_linked_database' cannot be blank",
                                    catalog.catalog_name, write_integration.integration_name
                                )
                            } else {
                                Ok(t)
                            }
                        })
                        .transpose()?;

                    // handle external volume
                    let ext_vol_missing = write_integration
                        .external_volume
                        .map(|s| s.trim().is_empty())
                        .unwrap_or(true);

                    let needs_external_volume =
                        catalog_linked_database_name.is_some() || write_properties_set;
                    if needs_external_volume && ext_vol_missing {
                        return err!(
                            code => ErrorCode::InvalidConfig,
                            loc  => err_loc(),
                            "Catalog '{}' integration '{}': 'external_volume' required for REST write-support \
                             (catalog_linked_database set or writer properties set: target_file_size / max_data_extension_time_in_days)",
                            catalog.catalog_name, write_integration.integration_name
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_serde_yaml as yml;
    use std::path::Path;

    fn parse_view_and_validate(yaml: &str) -> FsResult<()> {
        let v: yml::Value = yml::from_str(yaml).unwrap();
        let m = v.as_mapping().expect("top-level YAML must be a mapping");
        let view = DbtCatalogsView::from_mapping(m)?;
        validate_catalogs(&view, Path::new("<test>"))
    }

    #[test]
    fn master_full_catalog_valid() {
        let yaml = r#"
catalogs:
  - catalog_name: sf_native
    active_write_integration: sf_native_int
    write_integrations:
      - name: sf_native_int
        catalog_type: built_in
        table_format: iceberg
        external_volume: dbt_external_volume
        base_location_root: analytics/iceberg/dbt
        base_location_subpath: team_a/warehouse
        adapter_properties:
          storage_serialization_policy: COMPATIBLE
          data_retention_time_in_days: 1
          max_data_extension_time_in_days: 14
          change_tracking: false
          target_file_size: "64MB"
  - name: polaris
    active_write_integration: polaris_int
    write_integrations:
      - name: polaris_int
        catalog_type: iceberg_rest
        # external_volume is allowed (optional) for REST:
        external_volume: ext_vol_ok_for_rest
        table_format: iceberg
        adapter_properties:
          target_file_size: AUTO
          auto_refresh: true
          max_data_extension_time_in_days: 14
          catalog_namespace: "CAT_NS/SCHEMA_NS"
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_ok(),
            "expected master full catalog to validate, got: {:?}",
            res.err()
        );
    }

    #[test]
    fn happy_built_in_minimal() {
        let yaml = r#"
catalogs:
  - catalog_name: sf_native
    active_write_integration: sf_native_int
    write_integrations:
      - name: sf_native_int
        catalog_type: built_in
        external_volume: dbt_external_volume
        table_format: iceberg
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_ok(),
            "built_in minimal should validate: {:?}",
            res.err()
        );
    }

    #[test]
    fn happy_rest_minimal_without_ext_vol() {
        let yaml = r#"
catalogs:
  - name: polaris
    active_write_integration: polaris_int
    write_integrations:
      - name: polaris_int
        catalog_type: iceberg_rest
        table_format: iceberg
        adapter_properties:
          auto_refresh: true
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_ok(),
            "rest minimal (no external_volume) should validate: {:?}",
            res.err()
        );
    }

    #[test]
    fn accepts_name_alias_for_catalog() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        table_format: iceberg
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(
            v.is_ok(),
            "alias `name` for catalog should parse: {:?}",
            v.err()
        );
        let v = v.unwrap();
        assert_eq!(v.catalogs.len(), 1);
        assert_eq!(v.catalogs[0].catalog_name, "c1");
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(res.is_ok(), "alias case should validate: {:?}", res.err());
    }

    #[test]
    fn write_integrations_must_be_mapping() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations: [42]
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(
            v.is_err(),
            "non-mapping write_integration element must fail"
        );
    }

    #[test]
    fn missing_catalogs_key() {
        let yaml = r#"
not_catalogs: []
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "missing top-level 'catalogs' must fail");
    }

    #[test]
    fn missing_active_write_integration() {
        let yaml = r#"
    catalogs:
      - name: c1
        write_integrations:
          - name: i1
            catalog_type: built_in
            external_volume: ev
    "#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "missing required key should fail parse");
    }

    #[test]
    fn invalid_catalog_type() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: mango
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "invalid catalog_type must fail parse");
    }

    #[test]
    fn invalid_table_format() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: built_in
        table_format: delta
        external_volume: ev
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "invalid table_format must fail parse");
    }

    #[test]
    fn invalid_storage_serialization_policy() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: built_in
        external_volume: ev
        adapter_properties:
          storage_serialization_policy: FASTEST
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(
            v.is_err(),
            "invalid storage_serialization_policy must fail parse"
        );
    }

    #[test]
    fn invalid_target_file_size_value() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        adapter_properties:
          target_file_size: "1MB"
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "invalid target_file_size must fail parse");
    }

    #[test]
    fn built_in_requires_external_volume() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: built_in
        # external_volume: missing
        table_format: iceberg
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m).unwrap();
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(
            res.is_err(),
            "built_in without external_volume must fail validation"
        );
    }

    #[test]
    fn rest_forbids_base_location_fields() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        base_location_root: some/root
        table_format: iceberg
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m).unwrap();
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(
            res.is_err(),
            "rest with base_location_* must fail validation"
        );
    }

    #[test]
    fn active_write_integration_must_exist() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: does_not_exist
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        table_format: iceberg
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m).unwrap();
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(
            res.is_err(),
            "active write integration must exist among write_integrations"
        );
    }

    #[test]
    fn case_insensitive_enums_parse() {
        let yaml = r#"
catalogs:
  - name: C
    active_write_integration: I
    write_integrations:
      - name: I
        catalog_type: BUILT_IN
        table_format: ICEBERG
        external_volume: EV
        adapter_properties:
          storage_serialization_policy: compatible
          target_file_size: auto
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_ok(),
            "case-insensitive enums should validate: {:?}",
            res.err()
        );
    }

    #[test]
    fn blank_catalog_name_fails() {
        let yaml = r#"
catalogs:
  - catalog_name: "   "
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
"#;
        let res = parse_view_and_validate(yaml);
        assert!(res.is_err(), "blank catalog_name must fail validation");
    }

    #[test]
    fn blank_integration_name_or_active_fails() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: " "
    write_integrations:
      - name: "   "
        catalog_type: iceberg_rest
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_err(),
            "blank integration name / active_write_integration must fail"
        );
    }

    #[test]
    fn blank_base_location_fields_fail() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: built_in
        external_volume: ev
        base_location_root: " "
        base_location_subpath: "   "
"#;
        let res = parse_view_and_validate(yaml);
        assert!(res.is_err(), "blank base_location_* must fail for built_in");
    }

    #[test]
    fn blank_catalog_namespace_fails() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        adapter_properties:
          catalog_namespace: "  "
"#;
        let res = parse_view_and_validate(yaml);
        assert!(res.is_err(), "blank catalog_namespace must fail");
    }

    #[test]
    fn rest_linked_db_requires_external_volume() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        catalog_linked_database: true
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_err(),
            "linked DB true without external_volume must fail"
        );
    }

    #[test]
    fn rest_write_prop_requires_external_volume_ext_volen_without_linked_db() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        adapter_properties:
          max_data_extension_time_in_days: 1
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_err(),
            "write-support prop without external_volume must fail"
        );
    }

    #[test]
    fn rest_linked_db_false_conflicts_with_write_prop() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        catalog_linked_database: false
        adapter_properties:
          max_data_extension_time_in_days: 1
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_err(),
            "write-support properties not allowed when no linked database"
        );
    }

    #[test]
    fn rest_linked_db_write_ok_with_external_volume() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        external_volume: ev
        table_format: iceberg
        catalog_linked_database: true
        adapter_properties:
          max_data_extension_time_in_days: 1
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_ok(),
            "linked DB write-support should pass when external_volume present"
        );
    }

    #[test]
    fn strict_unknown_key_built_in_fails() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: built_in
        external_volume: ev
        adapter_properties:
          storage_serialisation_policy: COMPATIBLE   # typo (s/z)
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(
            v.is_err(),
            "unknown key in built_in adapter_properties must fail parse/validation"
        );
    }

    #[test]
    fn strict_unknown_key_rest_fails() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: iceberg_rest
        adapter_properties:
          foo: 1
"#;
        let v = parse_view_and_validate(yaml);
        assert!(
            v.is_err(),
            "unknown key in rest adapter_properties must fail"
        );
    }

    #[test]
    fn non_string_key_in_adapter_properities_fails() {
        let yaml = r#"
catalogs:
  - name: c1
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: built_in
        external_volume: ev
        adapter_properties: { 123: "bad" }
"#;
        let v = parse_view_and_validate(yaml);
        assert!(v.is_err(), "non-string key in adapter_properties must fail");
    }

    #[test]
    fn top_level_unknown_key_fails() {
        let yaml = r#"
    catalogs:
      - name: c1
        active_write_integration: i1
        write_integrations:
          - name: i1
            catalog_type: iceberg_rest
    extra_top_level: true
    "#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "unknown key at top-level must fail parse");
    }

    #[test]
    fn catalog_spec_unknown_key_fails() {
        let yaml = r#"
    catalogs:
      - name: c1
        active_write_integration: i1
        write_integrations: []
        unexpected: 42
    "#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "unknown key in catalog spec must fail parse");
    }

    #[test]
    fn write_integration_unknown_key_fails() {
        let yaml = r#"
    catalogs:
      - name: c1
        active_write_integration: i1
        write_integrations:
          - name: i1
            catalog_type: iceberg_rest
            oops: true
    "#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(
            v.is_err(),
            "unknown key in write_integration must fail parse"
        );
    }

    #[test]
    fn catalog_alias_collision_fails() {
        let yaml = r#"
    catalogs:
      - catalog_name: c1
        name: also_c1
        active_write_integration: i1
        write_integrations:
          - name: i1
            catalog_type: iceberg_rest
    "#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(
            v.is_err(),
            "providing both catalog_name and name must fail parse"
        );
    }

    #[test]
    fn integration_alias_collision_fails() {
        let yaml = r#"
    catalogs:
      - name: c1
        active_write_integration: i1
        write_integrations:
          - integration_name: i1
            name: also_i1
            catalog_type: iceberg_rest
    "#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(
            v.is_err(),
            "providing both integration_name and name must fail parse"
        );
    }

    #[test]
    fn built_in_blank_external_volume_fails() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: built_in
        external_volume: "  "
        table_format: iceberg
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m).unwrap();
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(res.is_err(), "blank external_volume must fail");
    }

    #[test]
    fn rest_forbids_base_location_subpath() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: iceberg_rest
        base_location_subpath: foo/bar
        table_format: iceberg
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m).unwrap();
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(res.is_err());
    }

    #[test]
    fn negative_u32_fails_when_present() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: built_in
        external_volume: ev
        adapter_properties:
          data_retention_time_in_days: -1
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "negative u32 must fail parse");
    }

    #[test]
    fn non_bool_change_tracking_fails() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: built_in
        external_volume: ev
        adapter_properties:
          change_tracking: "yes"
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "non-bool change_tracking must fail");
    }

    #[test]
    fn adapter_properties_wrong_type_fails() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: iceberg_rest
        adapter_properties: []
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m);
        assert!(v.is_err(), "adapter_properties must be a mapping");
    }

    #[test]
    fn rest_target_file_size_requires_ext_vol() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: iceberg_rest
        adapter_properties:
          target_file_size: "64MB"
"#;
        let res = parse_view_and_validate(yaml);
        assert!(res.is_err(), "REST writer prop requires external_volume");
    }

    #[test]
    fn rest_linked_db_false_conflicts_with_target_file_size() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: iceberg_rest
        catalog_linked_database: false
        external_volume: ev
        adapter_properties:
          target_file_size: AUTO
"#;
        let res = parse_view_and_validate(yaml);
        assert!(
            res.is_err(),
            "writer properties not allowed when linked_db=false"
        );
    }

    #[test]
    fn duplicate_integration_names_fail() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i1
    write_integrations:
      - name: i1
        table_format: iceberg
        catalog_type: iceberg_rest
        external_volume: ev
      - name: i1
        catalog_type: built_in
        table_format: iceberg
        external_volume: ev
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m).unwrap();
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(res.is_err(), "duplicate integration names must fail");
    }

    #[test]
    fn duplicate_catalog_names_fail() {
        let yaml = r#"
catalogs:
  - name: c
    active_write_integration: i1
    write_integrations: [{ name: i1, catalog_type: iceberg_rest, table_format: iceberg}]
  - catalog_name: c
    active_write_integration: j1
    write_integrations: [{ name: j1, catalog_type: built_in, external_volume: ev, table_format: iceberg}]
"#;
        let yml_val: yml::Value = yml::from_str(yaml).unwrap();
        let m = yml_val
            .as_mapping()
            .expect("top-level YAML must be a mapping");
        let v = DbtCatalogsView::from_mapping(m).unwrap();
        let res = validate_catalogs(&v, Path::new("<test>"));
        assert!(res.is_err(), "duplicate catalog names must fail");
    }
}
