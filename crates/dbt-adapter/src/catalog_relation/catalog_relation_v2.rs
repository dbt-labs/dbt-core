use super::*;
use dbt_common::string_utils::try_parse_bool_str;
use dbt_schemas::schemas::dbt_catalogs_v2::{
    CatalogSpecV2View, DbtCatalogsV2View, UniformMode, V2CatalogType, V2FileFormat,
};
use dbt_yaml as yml;

const FIELD_CATALOG_NAME: &str = "catalog_name";
const FIELD_CATALOG_TYPE: &str = "catalog_type";
const FIELD_TABLE_FORMAT: &str = "table_format";
const FIELD_EXTERNAL_VOLUME: &str = "external_volume";
const FIELD_BASE_LOCATION_ROOT: &str = "base_location_root";
const FIELD_BASE_LOCATION_SUBPATH: &str = "base_location_subpath";
const FIELD_TRANSIENT: &str = "transient";
const FIELD_CHANGE_TRACKING: &str = "change_tracking";
const FIELD_DATA_RETENTION_TIME_IN_DAYS: &str = "data_retention_time_in_days";
const FIELD_STORAGE_SERIALIZATION_POLICY: &str = "storage_serialization_policy";
const FIELD_FILE_FORMAT: &str = "file_format";
const FIELD_LOCATION_ROOT: &str = "location_root";
const FIELD_USE_UNIFORM: &str = "use_uniform";
const FIELD_AUTO_REFRESH: &str = "auto_refresh";
const FIELD_MAX_DATA_EXTENSION_TIME_IN_DAYS: &str = "max_data_extension_time_in_days";
const FIELD_TARGET_FILE_SIZE: &str = "target_file_size";
const FIELD_STORAGE_URI: &str = "storage_uri";

// databricks
const ADAPTER_PROP_LOCATION_ROOT: &str = "location_root";
const ADAPTER_PROP_USE_UNIFORM: &str = "use_uniform";

// snowflake cld
const ADAPTER_PROP_CATALOG_DATABASE: &str = "catalog_database";
const ADAPTER_PROP_AUTO_REFRESH: &str = "auto_refresh";
const ADAPTER_PROP_MAX_DATA_EXTENSION_TIME_IN_DAYS: &str = "max_data_extension_time_in_days";
const ADAPTER_PROP_TARGET_FILE_SIZE: &str = "target_file_size";

const MODEL_NONE_SENTINEL: &str = "none";

pub(super) fn from_model_config_and_catalogs_v2(
    adapter_type: AdapterType,
    model: &Value,
    catalogs: Arc<DbtCatalogs>,
) -> AdapterResult<CatalogRelation> {
    // V2 relation building assumes a structured model config object. If a bare
    // string still reaches this layer, that is a caller bug rather than a
    // supported v2 input shape.
    debug_assert!(
        model.kind() != ValueKind::String,
        "catalogs.yml v2 received a bare string model config; this is unsupported and indicates a parser bug."
    );

    if CatalogRelation::get_model_adapter_properties(model, adapter_type).is_some() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "catalogs.yml v2 supports top-level model overrides only; model adapter_properties are not supported",
        ));
    }

    let catalog_name = match adapter_type {
        AdapterType::Databricks => {
            let model_catalog_name = model_catalog_name(model, AdapterType::Databricks);
            let wants_iceberg = CatalogRelation::get_model_config_value(
                model,
                FIELD_TABLE_FORMAT,
                AdapterType::Databricks,
            )
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case(DBX_ICEBERG_TABLE_FORMAT))
            .unwrap_or(false);

            match model_catalog_name {
                None if !wants_iceberg => {
                    return Ok(CatalogRelation::default_catalog_relation_databricks());
                }
                None => {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "On Databricks, table_format=iceberg requires a `catalog_name` to select a v2 catalog (unity or hive_metastore).",
                    ));
                }
                Some(catalog_name) => catalog_name,
            }
        }
        AdapterType::Snowflake => match model_catalog_name(model, AdapterType::Snowflake) {
            None => return CatalogRelation::build_without_catalogs_yml(model),
            Some(catalog_name) => catalog_name,
        },
        AdapterType::Bigquery => {
            let model_catalog_name = model_catalog_name(model, AdapterType::Bigquery);
            let wants_iceberg = CatalogRelation::get_model_config_value(
                model,
                FIELD_TABLE_FORMAT,
                AdapterType::Bigquery,
            )
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case(ICEBERG_TABLE_FORMAT))
            .unwrap_or(false);

            match model_catalog_name {
                None if !wants_iceberg => {
                    return Ok(CatalogRelation::default_catalog_relation_bigquery());
                }
                None => {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "On Bigquery, table_format=iceberg requires catalogs.yml and a `catalog_name` that selects a v2 catalog.",
                    ));
                }
                Some(catalog_name) => catalog_name,
            }
        }
        AdapterType::DuckDB => match model_catalog_name(model, AdapterType::DuckDB) {
            None => return Ok(CatalogRelation::default_catalog_relation_duckdb()),
            Some(catalog_name) => catalog_name,
        },
        _ => Err(AdapterError::new(
            AdapterErrorKind::Internal,
            format!("build_relation_catalog cannot be invoked by an adapter {adapter_type:?}"),
        ))?,
    };

    let spec = parse_v2_view(&catalogs)?;
    let catalog = find_v2_catalog(&spec, &catalog_name)?;

    if CatalogRelation::get_model_config_value(model, FIELD_CATALOG_TYPE, adapter_type).is_some() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "catalog_type may only be specified in write integration entries of catalogs.yml",
        ));
    }

    match (adapter_type, catalog.catalog_type) {
        (AdapterType::Databricks, V2CatalogType::Unity) => {
            CatalogRelation::build_databricks_unity_with_catalogs_v2(model, catalog, &catalog_name)
        }
        (AdapterType::Databricks, V2CatalogType::HiveMetastore) => {
            CatalogRelation::build_databricks_hive_with_catalogs_v2(model, catalog, &catalog_name)
        }
        (AdapterType::Snowflake, V2CatalogType::Horizon) => {
            CatalogRelation::build_horizon_with_catalogs_v2(model, catalog, &catalog_name)
        }
        (AdapterType::Snowflake, V2CatalogType::Glue) => {
            CatalogRelation::build_snowflake_linked_with_catalogs_v2(
                model,
                catalog,
                &catalog_name,
                "glue",
            )
        }
        (AdapterType::Snowflake, V2CatalogType::IcebergRest) => {
            CatalogRelation::build_snowflake_linked_with_catalogs_v2(
                model,
                catalog,
                &catalog_name,
                "iceberg_rest",
            )
        }
        (AdapterType::Snowflake, V2CatalogType::Unity) => {
            CatalogRelation::build_snowflake_linked_with_catalogs_v2(
                model,
                catalog,
                &catalog_name,
                "unity",
            )
        }
        (AdapterType::Bigquery, V2CatalogType::BiglakeMetastore) => {
            CatalogRelation::build_bigquery_biglake_with_catalogs_v2(model, catalog, &catalog_name)
        }
        (AdapterType::DuckDB, V2CatalogType::Glue)
        | (AdapterType::DuckDB, V2CatalogType::IcebergRest) => {
            CatalogRelation::build_duckdb_with_catalogs_v2(model, catalog, &catalog_name)
        }
        (AdapterType::DuckDB, other) => Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!(
                "Catalog '{catalog_name}' has type '{}'; DuckDB v2 mapping supports only 'glue' and 'iceberg_rest'",
                other.as_str()
            ),
        )),
        (AdapterType::Databricks, other) => Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!(
                "Catalog '{catalog_name}' has type '{}'; Databricks v2 mapping supports only 'unity' and 'hive_metastore'",
                other.as_str()
            ),
        )),
        (AdapterType::Snowflake, other) => Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!(
                "Catalog '{catalog_name}' has type '{}'; Snowflake v2 mapping supports only 'horizon', 'glue', 'iceberg_rest', and 'unity'",
                other.as_str()
            ),
        )),
        (AdapterType::Bigquery, other) => Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!(
                "Catalog '{catalog_name}' has type '{}'; Bigquery v2 mapping supports only 'biglake_metastore'",
                other.as_str()
            ),
        )),
        _ => Err(AdapterError::new(
            AdapterErrorKind::Internal,
            format!("build_relation_catalog cannot be invoked by an adapter {adapter_type:?}"),
        )),
    }
}

fn parse_v2_view<'a>(catalogs: &'a DbtCatalogs) -> AdapterResult<DbtCatalogsV2View<'a>> {
    catalogs
        .view_v2()
        .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, format!("{e}")))
}

fn parse_model_bool(
    model: &Value,
    key: &str,
    adapter_type: AdapterType,
) -> AdapterResult<Option<bool>> {
    let raw = CatalogRelation::get_model_config_value(model, key, adapter_type);
    try_parse_bool_str(raw.as_deref(), key)
        .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, e.to_string()))
}

fn parse_model_u32(
    model: &Value,
    key: &str,
    adapter_type: AdapterType,
) -> AdapterResult<Option<u32>> {
    CatalogRelation::get_model_config_value(model, key, adapter_type)
        .map(|v| {
            v.parse::<u32>().map_err(|_| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!("Model field '{key}' must be a non-negative integer"),
                )
            })
        })
        .transpose()
}

fn model_catalog_name(model: &Value, adapter_type: AdapterType) -> Option<String> {
    CatalogRelation::get_model_config_value(model, FIELD_CATALOG_NAME, adapter_type).and_then(|s| {
        let t = s.trim();
        if t.eq_ignore_ascii_case(MODEL_NONE_SENTINEL) {
            None
        } else {
            Some(t.to_string())
        }
    })
}

fn get_yaml_str<'a>(map: &'a yml::Mapping, key: &str) -> Option<&'a str> {
    map.get(yml::Value::from(key))
        .and_then(|v| v.as_str())
        .map(str::trim)
}

fn get_yaml_bool(map: &yml::Mapping, key: &str) -> Option<bool> {
    map.get(yml::Value::from(key)).and_then(|v| v.as_bool())
}

fn get_yaml_u32(map: &yml::Mapping, key: &str) -> Option<u32> {
    map.get(yml::Value::from(key)).and_then(|v| {
        v.as_i64()
            .and_then(|i| u32::try_from(i).ok())
            .or_else(|| v.as_u64().and_then(|u| u32::try_from(u).ok()))
    })
}

fn is_valid_databricks_file_format(v: &str) -> bool {
    v.eq_ignore_ascii_case("delta")
        || v.eq_ignore_ascii_case("parquet")
        || v.eq_ignore_ascii_case("hudi")
}

fn find_v2_catalog<'a>(
    spec: &'a DbtCatalogsV2View<'a>,
    catalog_name: &str,
) -> AdapterResult<&'a CatalogSpecV2View<'a>> {
    spec.catalogs
        .iter()
        .find(|catalog| catalog.name == catalog_name)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("Catalog '{catalog_name}' not found in catalogs.yml"),
            )
        })
}

fn require_platform_block<'a>(
    catalog: &'a CatalogSpecV2View<'a>,
    catalog_name: &str,
    platform: &str,
) -> AdapterResult<&'a yml::Mapping> {
    catalog.config_block(platform).ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            format!("Catalog '{catalog_name}' is missing config.{platform}"),
        )
    })
}

fn reject_unsupported_snowflake_linked_v2_model_fields(
    model: &Value,
    type_name: &str,
) -> AdapterResult<()> {
    if CatalogRelation::get_model_config_value(model, FIELD_TRANSIENT, AdapterType::Snowflake)
        .is_some()
    {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "transient may not be specified for ICEBERG catalogs. Snowflake built-in catalog DDL does not support transient ICEBERG tables.",
        ));
    }

    for field in [
        FIELD_EXTERNAL_VOLUME,
        FIELD_BASE_LOCATION_ROOT,
        FIELD_BASE_LOCATION_SUBPATH,
        FIELD_CHANGE_TRACKING,
        FIELD_DATA_RETENTION_TIME_IN_DAYS,
        FIELD_STORAGE_SERIALIZATION_POLICY,
    ] {
        if CatalogRelation::get_model_config_value(model, field, AdapterType::Snowflake).is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("Snowflake v2 {type_name} does not support model field '{field}' yet."),
            ));
        }
    }

    Ok(())
}

fn reject_unsupported_databricks_hive_v2_model_fields(model: &Value) -> AdapterResult<()> {
    for field in [FIELD_LOCATION_ROOT, FIELD_USE_UNIFORM] {
        if CatalogRelation::get_model_config_value(model, field, AdapterType::Databricks).is_some()
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("Databricks v2 hive_metastore does not support model field '{field}'."),
            ));
        }
    }

    Ok(())
}

fn uppercase_table_format(catalog: &CatalogSpecV2View<'_>) -> String {
    catalog.table_format.as_str().to_ascii_uppercase()
}

impl CatalogRelation {
    fn build_databricks_unity_with_catalogs_v2(
        model: &Value,
        catalog: &CatalogSpecV2View<'_>,
        catalog_name: &str,
    ) -> AdapterResult<CatalogRelation> {
        let databricks = require_platform_block(catalog, catalog_name, "databricks")?;

        let table_format = catalog.table_format.as_str().to_string();

        let mut file_format =
            Self::get_model_config_value(model, FIELD_FILE_FORMAT, AdapterType::Databricks)
                .or_else(|| get_yaml_str(databricks, FIELD_FILE_FORMAT).map(|s| s.to_string()))
                .unwrap_or_else(|| DELTA_TABLE_FORMAT.to_string());
        file_format.make_ascii_lowercase();

        let location_root =
            Self::get_model_config_value(model, FIELD_LOCATION_ROOT, AdapterType::Databricks)
                .or_else(|| get_yaml_str(databricks, FIELD_LOCATION_ROOT).map(|s| s.to_string()));

        let mut external_volume = None;
        let mut adapter_properties = BTreeMap::new();
        let use_uniform = UniformMode::from_bool(
            parse_model_bool(model, FIELD_USE_UNIFORM, AdapterType::Databricks)?
                .or_else(|| get_yaml_bool(databricks, FIELD_USE_UNIFORM))
                .unwrap_or(false),
        );

        if let Some(location_root) = location_root {
            if location_root.trim().is_empty() {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "Databricks v2 location_root cannot be blank or whitespace",
                ));
            }
            external_volume = Self::dbx_build_external_volume_for_location(model, &location_root);
            adapter_properties.insert(ADAPTER_PROP_LOCATION_ROOT.to_string(), location_root);
        }

        let file_format_enum = V2FileFormat::parse(&file_format, None)
            .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, format!("{e}")))?;

        match (file_format_enum, use_uniform) {
            (V2FileFormat::Delta, UniformMode::Enabled)
            | (V2FileFormat::Parquet, UniformMode::Disabled) => Ok(()),
            (V2FileFormat::Delta, UniformMode::Disabled) => Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Databricks v2 unity use_uniform: false (or unset) requires file_format: parquet",
            )),
            (V2FileFormat::Parquet, UniformMode::Enabled) => Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Databricks v2 unity use_uniform: true requires file_format: delta",
            )),
            (V2FileFormat::Hudi, _) => Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Databricks v2 unity does not support file_format 'hudi' (use delta or parquet)",
            )),
        }?;

        adapter_properties.insert(
            ADAPTER_PROP_USE_UNIFORM.to_string(),
            use_uniform.is_enabled().to_string(),
        );
        Ok(CatalogRelation {
            adapter_type: AdapterType::Databricks,
            catalog_name: Some(catalog_name.to_string()),
            integration_name: None,
            catalog_type: DATABRICKS_UNITY_CATALOG.to_string(),
            table_format,
            file_format: Some(file_format),
            external_volume,
            base_location: None,
            adapter_properties,
            is_transient: None,
        })
    }

    fn build_databricks_hive_with_catalogs_v2(
        model: &Value,
        catalog: &CatalogSpecV2View<'_>,
        catalog_name: &str,
    ) -> AdapterResult<CatalogRelation> {
        reject_unsupported_databricks_hive_v2_model_fields(model)?;

        let databricks = require_platform_block(catalog, catalog_name, "databricks")?;

        let mut file_format =
            Self::get_model_config_value(model, FIELD_FILE_FORMAT, AdapterType::Databricks)
                .or_else(|| get_yaml_str(databricks, FIELD_FILE_FORMAT).map(|s| s.to_string()))
                .unwrap_or_else(|| DELTA_TABLE_FORMAT.to_string());
        file_format.make_ascii_lowercase();
        if !is_valid_databricks_file_format(&file_format) {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Databricks v2 hive_metastore file_format must be one of (delta|parquet|hudi)",
            ));
        }

        let adapter_properties = BTreeMap::new();

        Ok(CatalogRelation {
            adapter_type: AdapterType::Databricks,
            catalog_name: Some(catalog_name.to_string()),
            integration_name: None,
            catalog_type: DATABRICKS_HIVE_METASTORE.to_string(),
            table_format: catalog.table_format.as_str().to_string(),
            file_format: Some(file_format),
            external_volume: None,
            base_location: None,
            adapter_properties,
            is_transient: None,
        })
    }

    fn build_snowflake_linked_with_catalogs_v2(
        model: &Value,
        catalog: &CatalogSpecV2View<'_>,
        catalog_name: &str,
        type_name: &str,
    ) -> AdapterResult<CatalogRelation> {
        reject_unsupported_snowflake_linked_v2_model_fields(model, type_name)?;

        let snowflake = require_platform_block(catalog, catalog_name, "snowflake")?;

        let table_format = uppercase_table_format(catalog);

        let mut adapter_properties = BTreeMap::new();
        let database = get_yaml_str(snowflake, ADAPTER_PROP_CATALOG_DATABASE)
            .map(|db| db.to_string())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!(
                        "Catalog '{catalog_name}' {type_name}/snowflake requires catalog_database"
                    ),
                )
            })?;
        adapter_properties.insert(ADAPTER_PROP_CATALOG_DATABASE.to_string(), database);

        let auto_refresh = parse_model_bool(model, FIELD_AUTO_REFRESH, AdapterType::Snowflake)?
            .or_else(|| get_yaml_bool(snowflake, FIELD_AUTO_REFRESH));
        if let Some(auto_refresh) = auto_refresh {
            adapter_properties.insert(
                ADAPTER_PROP_AUTO_REFRESH.to_string(),
                auto_refresh.to_string(),
            );
        }

        let max_data_extension_time_in_days = parse_model_u32(
            model,
            FIELD_MAX_DATA_EXTENSION_TIME_IN_DAYS,
            AdapterType::Snowflake,
        )?
        .or_else(|| get_yaml_u32(snowflake, FIELD_MAX_DATA_EXTENSION_TIME_IN_DAYS));
        if let Some(max_data_extension_time_in_days) = max_data_extension_time_in_days {
            adapter_properties.insert(
                ADAPTER_PROP_MAX_DATA_EXTENSION_TIME_IN_DAYS.to_string(),
                max_data_extension_time_in_days.to_string(),
            );
        }

        let target_file_size =
            Self::get_model_config_value(model, FIELD_TARGET_FILE_SIZE, AdapterType::Snowflake)
                .or_else(|| get_yaml_str(snowflake, FIELD_TARGET_FILE_SIZE).map(|s| s.to_string()));
        if let Some(target_file_size) = target_file_size {
            adapter_properties.insert(ADAPTER_PROP_TARGET_FILE_SIZE.to_string(), target_file_size);
        }

        Ok(CatalogRelation {
            adapter_type: AdapterType::Snowflake,
            catalog_name: Some(catalog_name.to_string()),
            integration_name: None,
            catalog_type: CatalogType::SnowflakeIcebergRest.as_str().to_string(),
            table_format,
            external_volume: None,
            base_location: None,
            adapter_properties,
            is_transient: Some(false),
            file_format: None,
        })
    }

    fn build_horizon_with_catalogs_v2(
        model: &Value,
        catalog: &CatalogSpecV2View<'_>,
        catalog_name: &str,
    ) -> AdapterResult<CatalogRelation> {
        if Self::get_model_config_value(model, FIELD_TRANSIENT, AdapterType::Snowflake).is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "transient may not be specified for ICEBERG catalogs. Snowflake built-in catalog DDL does not support transient ICEBERG tables.",
            ));
        }

        let snowflake = require_platform_block(catalog, catalog_name, "snowflake")?;

        let external_volume =
            Self::get_model_config_value(model, FIELD_EXTERNAL_VOLUME, AdapterType::Snowflake)
                .or_else(|| get_yaml_str(snowflake, FIELD_EXTERNAL_VOLUME).map(|s| s.to_string()));

        let base_location_root =
            Self::get_model_config_value(model, FIELD_BASE_LOCATION_ROOT, AdapterType::Snowflake)
                .or_else(|| {
                    get_yaml_str(snowflake, FIELD_BASE_LOCATION_ROOT).map(|s| s.to_string())
                });
        let base_location_subpath = Self::get_model_config_value(
            model,
            FIELD_BASE_LOCATION_SUBPATH,
            AdapterType::Snowflake,
        );

        let schema = Self::get_model_config_value(model, "schema", AdapterType::Snowflake);
        let identifier = Self::get_model_config_value(model, "alias", AdapterType::Snowflake)
            .or_else(|| Self::get_model_config_value(model, "identifier", AdapterType::Snowflake));
        let base_location = Self::build_base_location(
            &base_location_root,
            &base_location_subpath,
            &schema,
            &identifier,
        );

        let mut adapter_properties = BTreeMap::new();

        let change_tracking =
            parse_model_bool(model, FIELD_CHANGE_TRACKING, AdapterType::Snowflake)?
                .or_else(|| get_yaml_bool(snowflake, FIELD_CHANGE_TRACKING));
        if let Some(change_tracking) = change_tracking {
            adapter_properties.insert(
                FIELD_CHANGE_TRACKING.to_string(),
                change_tracking.to_string(),
            );
        }

        let data_retention_time_in_days = parse_model_u32(
            model,
            FIELD_DATA_RETENTION_TIME_IN_DAYS,
            AdapterType::Snowflake,
        )?
        .or_else(|| get_yaml_u32(snowflake, FIELD_DATA_RETENTION_TIME_IN_DAYS));
        if let Some(data_retention_time_in_days) = data_retention_time_in_days {
            adapter_properties.insert(
                FIELD_DATA_RETENTION_TIME_IN_DAYS.to_string(),
                data_retention_time_in_days.to_string(),
            );
        }

        let max_data_extension_time_in_days = parse_model_u32(
            model,
            FIELD_MAX_DATA_EXTENSION_TIME_IN_DAYS,
            AdapterType::Snowflake,
        )?
        .or_else(|| get_yaml_u32(snowflake, FIELD_MAX_DATA_EXTENSION_TIME_IN_DAYS));
        if let Some(max_data_extension_time_in_days) = max_data_extension_time_in_days {
            adapter_properties.insert(
                FIELD_MAX_DATA_EXTENSION_TIME_IN_DAYS.to_string(),
                max_data_extension_time_in_days.to_string(),
            );
        }

        let storage_serialization_policy = Self::get_model_config_value(
            model,
            FIELD_STORAGE_SERIALIZATION_POLICY,
            AdapterType::Snowflake,
        )
        .or_else(|| {
            get_yaml_str(snowflake, FIELD_STORAGE_SERIALIZATION_POLICY).map(|s| s.to_string())
        });
        if let Some(storage_serialization_policy) = storage_serialization_policy {
            adapter_properties.insert(
                FIELD_STORAGE_SERIALIZATION_POLICY.to_string(),
                storage_serialization_policy,
            );
        }

        Ok(CatalogRelation {
            adapter_type: AdapterType::Snowflake,
            catalog_name: Some(catalog_name.to_string()),
            integration_name: None,
            catalog_type: CatalogType::SnowflakeBuiltIn.as_str().to_string(),
            table_format: uppercase_table_format(catalog),
            external_volume,
            base_location: Some(base_location),
            adapter_properties,
            is_transient: Some(false),
            file_format: None,
        })
    }

    fn build_bigquery_biglake_with_catalogs_v2(
        model: &Value,
        catalog: &CatalogSpecV2View<'_>,
        catalog_name: &str,
    ) -> AdapterResult<CatalogRelation> {
        let bigquery = require_platform_block(catalog, catalog_name, "bigquery")?;

        let external_volume = Self::get_model_config_value(
            model,
            FIELD_EXTERNAL_VOLUME,
            AdapterType::Bigquery,
        )
        .or_else(|| get_yaml_str(bigquery, FIELD_EXTERNAL_VOLUME).map(|s| s.to_string()))
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Catalog '{catalog_name}' biglake_metastore/bigquery requires external_volume"
                ),
            )
        })?;
        if external_volume.trim().is_empty() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Catalog '{catalog_name}' biglake_metastore/bigquery external_volume cannot be blank"
                ),
            ));
        }
        if !external_volume.starts_with("gs://") {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Catalog '{catalog_name}' biglake_metastore/bigquery external_volume must start with gs://"
                ),
            ));
        }

        let mut file_format = Self::get_model_config_value(
            model,
            FIELD_FILE_FORMAT,
            AdapterType::Bigquery,
        )
        .or_else(|| get_yaml_str(bigquery, FIELD_FILE_FORMAT).map(|s| s.to_string()))
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("Catalog '{catalog_name}' biglake_metastore/bigquery requires file_format"),
            )
        })?;
        file_format.make_ascii_lowercase();
        if !file_format.eq_ignore_ascii_case("parquet") {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Catalog '{catalog_name}' biglake_metastore/bigquery file_format must be parquet"
                ),
            ));
        }

        let base_location_root =
            Self::get_model_config_value(model, FIELD_BASE_LOCATION_ROOT, AdapterType::Bigquery)
                .or_else(|| {
                    get_yaml_str(bigquery, FIELD_BASE_LOCATION_ROOT).map(|s| s.to_string())
                });
        if let Some(base_location_root) = base_location_root.as_deref()
            && base_location_root.trim().is_empty()
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Bigquery v2 base_location_root cannot be blank or whitespace",
            ));
        }

        let base_location_subpath =
            Self::get_model_config_value(model, FIELD_BASE_LOCATION_SUBPATH, AdapterType::Bigquery);
        let schema = Self::get_model_config_value(model, "schema", AdapterType::Bigquery);
        let identifier = Self::get_model_config_value(model, "alias", AdapterType::Bigquery)
            .or_else(|| Self::get_model_config_value(model, "identifier", AdapterType::Bigquery));
        let base_location = Self::build_base_location(
            &base_location_root,
            &base_location_subpath,
            &schema,
            &identifier,
        );

        let storage_uri =
            Self::get_model_config_value(model, FIELD_STORAGE_URI, AdapterType::Bigquery)
                .unwrap_or_else(|| format!("{external_volume}/{base_location}"));

        let mut adapter_properties = BTreeMap::new();
        adapter_properties.insert(FIELD_STORAGE_URI.to_string(), storage_uri);

        Ok(CatalogRelation {
            adapter_type: AdapterType::Bigquery,
            catalog_name: Some(catalog_name.to_string()),
            integration_name: None,
            catalog_type: BIGQUERY_BIGLAKE_METASTORE.to_string(),
            table_format: catalog.table_format.as_str().to_string(),
            adapter_properties,
            is_transient: None,
            external_volume: None,
            base_location: None,
            file_format: Some(file_format),
        })
    }

    fn build_duckdb_with_catalogs_v2(
        _model: &Value,
        catalog: &CatalogSpecV2View<'_>,
        catalog_name: &str,
    ) -> AdapterResult<CatalogRelation> {
        let duckdb = require_platform_block(catalog, catalog_name, "duckdb")?;

        let table_format = uppercase_table_format(catalog);

        let endpoint = get_yaml_str(duckdb, "endpoint").map(|s| s.to_string());
        let endpoint_type = get_yaml_str(duckdb, "endpoint_type").map(|s| s.to_string());
        let warehouse = get_yaml_str(duckdb, "warehouse").map(|s| s.to_string());
        let secret = get_yaml_str(duckdb, "secret").map(|s| s.to_string());
        let attach_as = get_yaml_str(duckdb, "attach_as").map(|s| s.to_string());
        // Use same sanitization as ATTACH SQL generation so routing and attachment stay in sync
        let alias = sanitize_duckdb_identifier(attach_as.as_deref().unwrap_or(catalog_name));

        if endpoint.is_none() && endpoint_type.is_none() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Catalog '{catalog_name}' duckdb config requires 'endpoint' or 'endpoint_type'"
                ),
            ));
        }

        let mut adapter_properties = BTreeMap::new();
        if let Some(endpoint) = endpoint {
            adapter_properties.insert("endpoint".to_string(), endpoint);
        }
        if let Some(endpoint_type) = endpoint_type {
            adapter_properties.insert("endpoint_type".to_string(), endpoint_type);
        }
        if let Some(warehouse) = warehouse {
            adapter_properties.insert("warehouse".to_string(), warehouse);
        }
        if let Some(ref secret) = secret {
            adapter_properties.insert("secret".to_string(), secret.clone());
        }
        adapter_properties.insert("attached_database".to_string(), alias);

        Ok(CatalogRelation {
            adapter_type: AdapterType::DuckDB,
            catalog_name: Some(catalog_name.to_string()),
            integration_name: None,
            catalog_type: catalog.catalog_type.as_str().to_string(),
            table_format,
            file_format: None,
            external_volume: None,
            base_location: None,
            adapter_properties,
            is_transient: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::Value as JVal;
    use serde_json::json;
    use std::path::Path;

    fn adapter_type_to_attr(adapter_type: AdapterType) -> String {
        match adapter_type {
            AdapterType::Snowflake => "snowflake_attr".to_string(),
            AdapterType::Bigquery => "bigquery_attr".to_string(),
            AdapterType::Databricks => "databricks_attr".to_string(),
            AdapterType::DuckDB => "duckdb_attr".to_string(),
            _ => panic!("Not yet supported"),
        }
    }

    fn model(adapter_type: AdapterType, config: serde_json::Value) -> JVal {
        let mut model_map = serde_json::Map::new();
        let mut config_map = serde_json::Map::new();
        const TOP_LEVEL_KEYS: [&str; 12] = [
            "catalog_name",
            "schema",
            "identifier",
            "database",
            "alias",
            "file_format",
            "location_root",
            "use_uniform",
            "external_volume",
            "auto_refresh",
            "max_data_extension_time_in_days",
            "target_file_size",
        ];
        if let serde_json::Value::Object(config) = config {
            for (key, value) in config {
                if TOP_LEVEL_KEYS.iter().any(|k| k.eq_ignore_ascii_case(&key)) {
                    model_map.insert(key, value);
                } else {
                    config_map.insert(key, value);
                }
            }
            model_map.insert(
                adapter_type_to_attr(adapter_type),
                serde_json::Value::Object(config_map),
            );
            JVal::from_serialize(serde_json::Value::Object(model_map))
        } else {
            panic!("Config is not a JSON object");
        }
    }

    fn model_deprecated_config(config: serde_json::Value) -> JVal {
        let mut model_map = serde_json::Map::new();
        if let serde_json::Value::Object(config) = config {
            model_map.insert(
                "config".to_owned(),
                serde_json::Value::Object(config.clone()),
            );
            for (key, value) in config {
                model_map.insert(key, value);
            }
            JVal::from_serialize(serde_json::Value::Object(model_map))
        } else {
            panic!("Config is not a JSON object");
        }
    }

    fn load_catalogs_yaml(yaml: &str) -> DbtCatalogs {
        use dbt_schemas::schemas::dbt_catalogs_v2::validate_catalogs_v2;
        let parsed: dbt_yaml::Value = dbt_yaml::from_str(yaml).expect("valid YAML");
        let (repr, span) = match parsed {
            dbt_yaml::Value::Mapping(m, s) => (m, s),
            _ => panic!("expected top-level mapping"),
        };
        let catalogs = DbtCatalogs::new(repr, span);
        let view = catalogs.view_v2().expect("valid v2 view");
        validate_catalogs_v2(&view, Path::new("<test>")).expect("valid v2 catalogs");
        catalogs
    }

    #[test]
    fn databricks_v2_unity_catalog_builds_relation() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: delta
        use_uniform: true
"#,
        );
        let conf = json!({ "catalog_name": "UC" });
        let ms = [
            model(AdapterType::Databricks, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let r = from_model_config_and_catalogs_v2(
                AdapterType::Databricks,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap();

            assert_eq!(r.catalog_name.as_deref(), Some("UC"));
            assert!(r.integration_name.is_none());
            assert_eq!(r.catalog_type, "unity");
            assert_eq!(r.table_format, "iceberg");
            assert_eq!(r.file_format.as_deref(), Some("delta"));
        }
    }

    #[test]
    fn databricks_v2_unity_catalog_builds_relation_parquet_managed_iceberg() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: parquet
"#,
        );
        let conf = json!({ "catalog_name": "UC" });
        let ms = [
            model(AdapterType::Databricks, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let r = from_model_config_and_catalogs_v2(
                AdapterType::Databricks,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap();

            assert_eq!(r.catalog_type, "unity");
            assert_eq!(r.file_format.as_deref(), Some("parquet"));
        }
    }

    #[test]
    fn databricks_v2_unity_model_override_rejects_invalid_combo() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: parquet
"#,
        );
        let conf = json!({
            "catalog_name": "UC",
            "file_format": "parquet",
            "use_uniform": true,
        });
        let ms = [
            model(AdapterType::Databricks, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let err = from_model_config_and_catalogs_v2(
                AdapterType::Databricks,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap_err();

            assert!(
                format!("{err}")
                    .contains("Databricks v2 unity use_uniform: true requires file_format: delta")
            );
        }
    }

    #[test]
    fn databricks_v2_unity_model_override_rejects_delta_without_use_uniform() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: delta
        use_uniform: true
"#,
        );
        let conf = json!({ "catalog_name": "UC", "use_uniform": false });
        let ms = [
            model(AdapterType::Databricks, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let err = from_model_config_and_catalogs_v2(
                AdapterType::Databricks,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap_err();

            assert!(format!("{err}").contains(
                "Databricks v2 unity use_uniform: false (or unset) requires file_format: parquet"
            ));
        }
    }

    #[test]
    fn databricks_v2_hive_metastore_allows_hudi() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: HMS
    type: hive_metastore
    table_format: default
    config:
      databricks:
        file_format: hudi
"#,
        );
        let conf = json!({ "catalog_name": "HMS" });
        let ms = [
            model(AdapterType::Databricks, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let r = from_model_config_and_catalogs_v2(
                AdapterType::Databricks,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap();

            assert_eq!(r.catalog_name.as_deref(), Some("HMS"));
            assert_eq!(r.catalog_type, "hive_metastore");
            assert_eq!(r.table_format, "default");
            assert_eq!(r.file_format.as_deref(), Some("hudi"));
        }
    }

    #[test]
    fn v2_rejects_model_adapter_properties() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      databricks:
        file_format: delta
        use_uniform: true
"#,
        );
        let model = JVal::from_serialize(json!({
            "catalog_name": "UC",
            "databricks_attr": {
                "adapter_properties": {
                    "location_root": "s3://bucket/path"
                }
            }
        }));

        let err =
            from_model_config_and_catalogs_v2(AdapterType::Databricks, &model, Arc::new(catalogs))
                .unwrap_err();

        assert!(
            format!("{err}").contains("catalogs.yml v2 supports top-level model overrides only")
        );
    }

    #[test]
    fn bigquery_v2_biglake_catalog_builds_relation() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: BQ
    type: biglake_metastore
    table_format: iceberg
    config:
      bigquery:
        external_volume: gs://bucket
        file_format: parquet
        base_location_root: root
"#,
        );
        let conf = json!({
            "catalog_name": "BQ",
            "schema": "analytics",
            "alias": "events"
        });
        let ms = [
            model(AdapterType::Bigquery, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let r = from_model_config_and_catalogs_v2(
                AdapterType::Bigquery,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap();

            assert_eq!(r.catalog_name.as_deref(), Some("BQ"));
            assert!(r.integration_name.is_none());
            assert_eq!(r.catalog_type, "biglake_metastore");
            assert_eq!(r.table_format, "iceberg");
            assert_eq!(r.file_format.as_deref(), Some("parquet"));
            assert!(r.external_volume.is_none());
            assert!(r.base_location.is_none());
            assert_eq!(
                r.adapter_properties.get("storage_uri").map(|s| s.as_str()),
                Some("gs://bucket/root/analytics/events")
            );
        }
    }

    #[test]
    fn bigquery_v2_biglake_model_values_override_yaml_values() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: BQ
    type: biglake_metastore
    table_format: iceberg
    config:
      bigquery:
        external_volume: gs://bucket
        file_format: parquet
        base_location_root: root
"#,
        );
        let conf = json!({
            "catalog_name": "BQ",
            "external_volume": "gs://other-bucket",
            "base_location_root": "override",
            "base_location_subpath": "leaf",
            "schema": "analytics",
            "alias": "events"
        });
        let ms = [
            model(AdapterType::Bigquery, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let r = from_model_config_and_catalogs_v2(
                AdapterType::Bigquery,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap();

            assert_eq!(r.file_format.as_deref(), Some("parquet"));
            assert_eq!(
                r.adapter_properties.get("storage_uri").map(|s| s.as_str()),
                Some("gs://other-bucket/override/analytics/events/leaf")
            );
        }
    }

    #[test]
    fn snowflake_v2_unity_catalog_builds_cld_relation() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_CLD"
        auto_refresh: true
"#,
        );
        let conf = json!({ "catalog_name": "UC", "schema": "S", "identifier": "I" });
        let ms = [
            model(AdapterType::Snowflake, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let r = from_model_config_and_catalogs_v2(
                AdapterType::Snowflake,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap();

            assert_eq!(r.catalog_name.as_deref(), Some("UC"));
            assert!(r.integration_name.is_none());
            assert_eq!(r.catalog_type, CatalogType::SnowflakeIcebergRest.as_str());
            assert_eq!(r.table_format, ICEBERG_TABLE_FORMAT);
            assert_eq!(
                r.adapter_properties
                    .get("catalog_database")
                    .map(|s| s.as_str()),
                Some("MY_CLD")
            );
        }
    }

    #[test]
    fn snowflake_v2_unity_uses_yaml_catalog_database_over_model_database() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "YAML_DB"
        auto_refresh: false
        max_data_extension_time_in_days: 30
        target_file_size: "16MB"
"#,
        );
        let conf = json!({
            "catalog_name": "UC",
            "database": "MODEL_DB",
            "auto_refresh": true,
            "max_data_extension_time_in_days": 7,
            "target_file_size": "32MB"
        });
        let ms = [
            model(AdapterType::Snowflake, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let r = from_model_config_and_catalogs_v2(
                AdapterType::Snowflake,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap();

            assert_eq!(
                r.adapter_properties
                    .get("catalog_database")
                    .map(|s| s.as_str()),
                Some("YAML_DB")
            );
            assert_eq!(
                r.adapter_properties.get("auto_refresh").map(|s| s.as_str()),
                Some("true")
            );
            assert_eq!(
                r.adapter_properties
                    .get("max_data_extension_time_in_days")
                    .map(|s| s.as_str()),
                Some("7")
            );
            assert_eq!(
                r.adapter_properties
                    .get("target_file_size")
                    .map(|s| s.as_str()),
                Some("32MB")
            );
        }
    }

    #[test]
    fn snowflake_v2_unity_rejects_stubbed_model_fields() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: UC
    type: unity
    table_format: iceberg
    config:
      snowflake:
        catalog_database: "MY_CLD"
"#,
        );
        let conf = json!({ "catalog_name": "UC", "external_volume": "EV" });
        let ms = [
            model(AdapterType::Snowflake, conf.clone()),
            model_deprecated_config(conf),
        ];

        for m in ms {
            let err = from_model_config_and_catalogs_v2(
                AdapterType::Snowflake,
                &m,
                Arc::new(catalogs.clone()),
            )
            .unwrap_err();
            assert!(format!("{err}").contains(
                "Snowflake v2 unity does not support model field 'external_volume' yet."
            ));
        }
    }

    // ===== DuckDB v2 tests =====

    #[test]
    fn duckdb_no_catalog_returns_default() {
        // DuckDB with no catalog_name should return a default relation
        // We need *some* catalogs.yml for the v2 path, but DuckDB with no
        // catalog_name should early-exit before looking it up.
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: my_rest
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://rest.example.com"
"#,
        );
        let conf = json!({});
        let m = model(AdapterType::DuckDB, conf);

        let r =
            from_model_config_and_catalogs_v2(AdapterType::DuckDB, &m, Arc::new(catalogs)).unwrap();

        assert!(r.catalog_name.is_none());
        assert!(r.integration_name.is_none());
        assert_eq!(r.catalog_type, "duckdb");
        assert_eq!(r.table_format, "default");
        assert!(r.file_format.is_none());
        assert!(r.adapter_properties.is_empty());
    }

    #[test]
    fn duckdb_catalog_name_without_catalogs_yml_errors() {
        // catalog_name specified but no matching catalog in catalogs.yml -> error
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: other_catalog
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://glue.example.com"
"#,
        );
        let conf = json!({ "catalog_name": "nonexistent" });
        let m = model(AdapterType::DuckDB, conf);

        let err = from_model_config_and_catalogs_v2(AdapterType::DuckDB, &m, Arc::new(catalogs))
            .unwrap_err();

        assert!(format!("{err}").contains("Catalog 'nonexistent' not found in catalogs.yml"));
    }

    #[test]
    fn duckdb_v2_glue_catalog_builds_relation() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: my_glue
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://glue.example.com"
        secret: "my_secret"
        attach_as: "glue_db"
"#,
        );
        let conf = json!({ "catalog_name": "my_glue" });
        let m = model(AdapterType::DuckDB, conf);

        let r =
            from_model_config_and_catalogs_v2(AdapterType::DuckDB, &m, Arc::new(catalogs)).unwrap();

        assert_eq!(r.catalog_name.as_deref(), Some("my_glue"));
        assert!(r.integration_name.is_none());
        assert_eq!(r.catalog_type, "glue");
        assert_eq!(r.table_format, ICEBERG_TABLE_FORMAT);
        assert!(r.file_format.is_none());
        assert_eq!(
            r.adapter_properties.get("endpoint").map(|s| s.as_str()),
            Some("https://glue.example.com")
        );
        assert_eq!(
            r.adapter_properties.get("secret").map(|s| s.as_str()),
            Some("my_secret")
        );
        assert_eq!(
            r.adapter_properties
                .get("attached_database")
                .map(|s| s.as_str()),
            Some("glue_db")
        );
    }

    #[test]
    fn duckdb_v2_iceberg_rest_catalog_builds_relation() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: my_rest
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://rest.example.com"
"#,
        );
        let conf = json!({ "catalog_name": "my_rest" });
        let m = model(AdapterType::DuckDB, conf);

        let r =
            from_model_config_and_catalogs_v2(AdapterType::DuckDB, &m, Arc::new(catalogs)).unwrap();

        assert_eq!(r.catalog_name.as_deref(), Some("my_rest"));
        assert_eq!(r.catalog_type, "iceberg_rest");
        assert_eq!(r.table_format, ICEBERG_TABLE_FORMAT);
        assert_eq!(
            r.adapter_properties.get("endpoint").map(|s| s.as_str()),
            Some("https://rest.example.com")
        );
        // When no attach_as, alias defaults to catalog_name
        assert_eq!(
            r.adapter_properties
                .get("attached_database")
                .map(|s| s.as_str()),
            Some("my_rest")
        );
        // No secret specified
        assert!(!r.adapter_properties.contains_key("secret"));
    }

    #[test]
    fn duckdb_v2_glue_catalog_builds_relation_with_endpoint_type() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: my_glue
    type: glue
    table_format: iceberg
    config:
      duckdb:
        endpoint_type: "GLUE"
        warehouse: "123456789012"
        attach_as: "glue_db"
"#,
        );
        let conf = json!({ "catalog_name": "my_glue" });
        let m = model(AdapterType::DuckDB, conf);

        let r =
            from_model_config_and_catalogs_v2(AdapterType::DuckDB, &m, Arc::new(catalogs)).unwrap();

        assert_eq!(
            r.adapter_properties
                .get("endpoint_type")
                .map(|s| s.as_str()),
            Some("GLUE")
        );
        assert_eq!(
            r.adapter_properties.get("warehouse").map(|s| s.as_str()),
            Some("123456789012")
        );
        assert!(!r.adapter_properties.contains_key("endpoint"));
        assert_eq!(
            r.adapter_properties
                .get("attached_database")
                .map(|s| s.as_str()),
            Some("glue_db")
        );
    }

    #[test]
    fn duckdb_v2_unsupported_catalog_type_errors() {
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: my_horizon
    type: horizon
    table_format: iceberg
    config:
      snowflake:
        external_volume: "EV"
"#,
        );
        let conf = json!({ "catalog_name": "my_horizon" });
        let m = model(AdapterType::DuckDB, conf);

        let err = from_model_config_and_catalogs_v2(AdapterType::DuckDB, &m, Arc::new(catalogs))
            .unwrap_err();

        assert!(
            format!("{err}").contains("DuckDB v2 mapping supports only 'glue' and 'iceberg_rest'")
        );
    }

    #[test]
    fn duckdb_v2_catalog_name_none_returns_default() {
        // DuckDB with catalog_name = "none" should return default
        let catalogs = load_catalogs_yaml(
            r#"
catalogs:
  - name: my_rest
    type: iceberg_rest
    table_format: iceberg
    config:
      duckdb:
        endpoint: "https://rest.example.com"
"#,
        );
        let conf = json!({ "catalog_name": "none" });
        let m = model(AdapterType::DuckDB, conf);

        let r =
            from_model_config_and_catalogs_v2(AdapterType::DuckDB, &m, Arc::new(catalogs)).unwrap();

        assert!(r.catalog_name.is_none());
        assert_eq!(r.catalog_type, "duckdb");
        assert_eq!(r.table_format, "default");
    }
}
