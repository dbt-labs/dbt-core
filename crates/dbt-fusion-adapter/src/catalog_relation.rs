use dbt_schemas::schemas::dbt_catalogs::CatalogType;
use dbt_schemas::schemas::dbt_catalogs::DbtCatalogs;
use dbt_serde_yaml::{Mapping as YmlMapping, Span, Value as YmlValue};
use minijinja::{
    Value,
    value::{Object, ValueKind},
};
use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};

// Jinja DDL tends to have comparisons against uppercase strings
// TODO(versufacit): dbt core currently has a notion of the default store as a catalog.
// We may diverge from this. Implemented now for legacy compatibility ahead of Coalesce;
// https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-snowflake/src/dbt/include/snowflake/macros/relations/table/create.sql#L8
const SNOWFLAKE_RELATION_STORE: &str = "INFO_SCHEMA";
const ICEBERG_BUILT_IN_CATALOG: &str = "BUILT_IN";
const DEFAULT_TABLE_FORMAT: &str = "DEFAULT";
const ICEBERG_TABLE_FORMAT: &str = "ICEBERG";

const LEGACY_CONFIG_ICEBERG_ATTRIBUTE_ERR: &str = "The external_volume and base_location_* model attributes are not able to \
    be specified on table_format=default models (includes models without an explicit \
    table_format). For other table formats, use catalogs.yml write integrations.";

const ALLOWED_CATALOG_TYPE_FORMATS: [&str; 2] = [DEFAULT_TABLE_FORMAT, ICEBERG_TABLE_FORMAT];
const ALLOWED_TABLE_FORMATS_DISPLAY: &str = "DEFAULT|ICEBERG";

#[derive(Debug, serde::Serialize)]
pub struct CatalogRelation {
    // identity / routing
    pub catalog_name: Option<String>,
    pub integration_name: Option<String>,

    // type & format
    pub catalog_type: String,
    pub table_format: String,

    // common
    pub external_volume: Option<String>,

    // built_in only
    // synthesized base_location_root and base_location_subpath model attributes
    pub base_location: Option<String>,

    // normalized SQL options
    pub adapter_properties: BTreeMap<String, String>,

    // metadata helper
    pub is_transient: bool,
}

impl CatalogRelation {
    pub fn from_model_config_and_catalogs(
        model_config: &Value,
        catalogs: Option<Arc<DbtCatalogs>>,
    ) -> AdapterResult<Self> {
        // Special case hack: a plain string means this is the linked database name.
        // You cannot use a string literal anywhere except drop for this feature
        // this function is designed to be used with a model config

        if model_config.kind() == ValueKind::String {
            let fqn = model_config.as_str().unwrap().trim();
            let db_only = fqn.split('.').next().unwrap_or(fqn).trim();

            return if let Some(cats) = catalogs.as_ref()
                && Self::cld_exists_in_iceberg_rest(cats.mapping(), db_only)
            {
                Self::build_for_cld_only(model_config)
            } else {
                Ok(Self::empty_catalog_relation())
            };
        }

        let model_catalog_name = Self::get_model_config_value(model_config, "catalog_name")
            .and_then(|s| {
                let t = s.trim();
                // [DELIBERATE CHANGE] Serialization sometimes makes model configs parse a none
                // value into Some("none"). Unlikely many users will be naming their catalog names 'none'.
                // TODO: track that down and patch
                if t.eq_ignore_ascii_case("none") {
                    None
                } else {
                    Some(t.to_string())
                }
            });

        match (model_catalog_name.as_deref(), catalogs.as_ref()) {
            // No reconciliation path: only values present on the model config are used.
            // This represents the "legacy" or v1 Iceberg tables/iceberg tables
            // which are Snowflake only and do not use the catalogs.yml.
            (None, _) => Self::build_without_catalogs_yml(model_config),

            // Catalog-driven path: both catalog_name and catalogs.yml need be present
            (Some(catalog_name), None) => Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Model specifies catalog_name '{catalog_name}', but catalogs.yml was not found"
                ),
            )),

            (Some(catalog_name), Some(catalogs)) => {
                Self::build_with_catalogs(model_config, catalogs.mapping(), catalog_name)
            }
        }
    }

    /// Some relations have no configs and attempts incorporate fail.
    ///
    /// This is hack to duplicate core's logic until we have time to architecture a better system.
    fn build_for_cld_only(v: &Value) -> AdapterResult<CatalogRelation> {
        let db_name = v.as_str().unwrap().trim();

        let mut adapter_properties = BTreeMap::new();
        adapter_properties.insert("catalog_linked_database".to_string(), db_name.to_string());

        Ok(CatalogRelation {
            catalog_name: None,
            integration_name: None,
            catalog_type: CatalogType::IcebergRest.as_str().to_string(),
            table_format: "iceberg".to_string(),
            external_volume: None,
            base_location: None,
            adapter_properties,
            is_transient: false,
        })
    }

    /// Build a legacy model configuration into a catalog relation.
    ///
    /// Helper for building a catalog relation, default or iceberg, for model-configured only
    /// iceberg materializations in Snowflake.
    fn build_without_catalogs_yml(model_config: &Value) -> AdapterResult<CatalogRelation> {
        if Self::get_model_adapter_properties(model_config).is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "'adapter_properties' may only be specified to override catalogs.yml and cannot be used in a legacy model config",
            ));
        }

        // Core does not functionally permit a manually specified catalog_type in a model config.
        // Prompt the user to adopt catalogs.yml. [DELIBERATE CHANGE]: Core only ignores this silently.
        // This should be an impossible field by YAML strict mode.
        if Self::get_model_config_value(model_config, "catalog_type").is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "catalog_type may only be specified in catalog entries of catalogs.yml",
            ));
        }

        let transient_spec = Self::get_model_config_value(model_config, "transient");
        let transient_parsed = transient_spec
            .as_ref()
            .map(|s| s.eq_ignore_ascii_case("true"));

        match Self::get_model_config_value(model_config, "table_format") {
            // ===========================================================
            // table_format unspecified so assumed 'default' (legacy path)
            // ===========================================================
            None => {
                let external_volume = Self::get_model_config_value(model_config, "external_volume");
                let base_location_root =
                    Self::get_model_config_value(model_config, "base_location_root");
                let base_location_subpath =
                    Self::get_model_config_value(model_config, "base_location_subpath");

                if external_volume.is_some()
                    || base_location_root.is_some()
                    || base_location_subpath.is_some()
                {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        LEGACY_CONFIG_ICEBERG_ATTRIBUTE_ERR,
                    ));
                }

                Ok(CatalogRelation {
                    catalog_name: None,
                    integration_name: None,
                    table_format: DEFAULT_TABLE_FORMAT.to_string(),
                    catalog_type: SNOWFLAKE_RELATION_STORE.to_string(),
                    external_volume: None,
                    base_location: None,
                    adapter_properties: BTreeMap::new(),
                    is_transient: transient_parsed.unwrap_or(true),
                })
            }

            // ====================================
            // table_format='default' (legacy path)
            // ====================================
            Some(table_format) if table_format.eq_ignore_ascii_case(DEFAULT_TABLE_FORMAT) => {
                let external_volume = Self::get_model_config_value(model_config, "external_volume");
                let base_location_root =
                    Self::get_model_config_value(model_config, "base_location_root");
                let base_location_subpath =
                    Self::get_model_config_value(model_config, "base_location_subpath");

                if external_volume.is_some()
                    || base_location_root.is_some()
                    || base_location_subpath.is_some()
                {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        LEGACY_CONFIG_ICEBERG_ATTRIBUTE_ERR,
                    ));
                }

                Ok(CatalogRelation {
                    catalog_name: None,
                    integration_name: None,
                    table_format: DEFAULT_TABLE_FORMAT.to_string(),
                    catalog_type: SNOWFLAKE_RELATION_STORE.to_string(),
                    external_volume: None,
                    base_location: None,
                    adapter_properties: BTreeMap::new(),
                    is_transient: transient_parsed.unwrap_or(true),
                })
            }

            // ====================================
            // table_format='iceberg' (legacy path)
            // ====================================
            Some(table_format) if table_format.eq_ignore_ascii_case(ICEBERG_TABLE_FORMAT) => {
                if transient_spec.is_some() {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        "transient may not be specified for ICEBERG catalogs. Snowflake built-in catalog DDL does not support transient ICEBERG tables.",
                    ));
                }

                let external_volume = Self::get_model_config_value(model_config, "external_volume");
                let base_location_root =
                    Self::get_model_config_value(model_config, "base_location_root");
                let base_location_subpath =
                    Self::get_model_config_value(model_config, "base_location_subpath");

                let schema = Self::get_model_config_value(model_config, "schema");
                let identifier = Self::get_model_config_value(model_config, "identifier");

                let base_location = Self::build_base_location(
                    &base_location_root,
                    &base_location_subpath,
                    &schema,
                    &identifier,
                );

                Ok(CatalogRelation {
                    catalog_name: None,
                    integration_name: None,
                    table_format,
                    catalog_type: ICEBERG_BUILT_IN_CATALOG.to_string(),
                    external_volume,
                    base_location: Some(base_location),
                    adapter_properties: BTreeMap::new(),
                    is_transient: false, // always FALSE for ICEBERG
                })
            }

            // ======================
            // any other table_format
            // ======================
            Some(table_format) => Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Unsupported table_format='{table_format}'. Must be one of \
                    ({ALLOWED_TABLE_FORMATS_DISPLAY}) case insensitive. \
                     For other table formats, use catalogs.yml write integrations."
                ),
            )),
        }
    }

    /// Helper for building a catalog relation of any type supported in catalogs.yml
    ///
    /// A catalog write integration holds fallback metadata for model materialization DDL.
    /// Any individual model may override the catalog metadata with their own model configs.
    fn build_with_catalogs(
        model_config: &Value,
        catalogs: &YmlMapping,
        catalog_name: &str,
    ) -> AdapterResult<CatalogRelation> {
        let catalog = find_catalog(catalogs, catalog_name).ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("Catalog '{catalog_name}' not found in catalogs.yml"),
            )
        })?;

        // 1) identity: catalog comes from MC; integration is the catalog's active one
        let integration_name = lookup_integration_name(catalogs, catalog_name).unwrap_or_default();

        // 2) write integration lookup (may be None)
        let write_integration = Self::lookup_write_integration(catalog, &integration_name);

        // 3) resolve fields: model > write_integration > default/None

        // === catalog_type logic forbids overrides as Core hardcodes in catalogs.yml
        if Self::get_model_config_value(model_config, "catalog_type").is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "catalog_type may only be specified in write integration entries of catalogs.yml",
            ));
        }

        let raw_catalog_type = Self::yml_str(write_integration, "catalog_type".to_owned())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "catalog_type missing from catalogs.yml (should be impossible by schema)",
                )
            })?;

        let catalog_type = CatalogType::parse_strict(&raw_catalog_type)
            .map_err(|e| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!("Invalid catalog_type '{raw_catalog_type}': {e}"),
                )
            })?
            .as_str();

        let table_format = Self::get_model_config_value(model_config, "table_format")
            .or_else(|| Self::yml_str(write_integration, "table_format".to_string()))
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!("Missing required table_format for catalog '{catalog_name}'"),
                )
            })?;

        if !ALLOWED_CATALOG_TYPE_FORMATS
            .iter()
            .any(|a| table_format.eq_ignore_ascii_case(a))
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Unsupported table_format '{table_format}' in catalog '{catalog_name}'. \
                     Must be one of ({ALLOWED_TABLE_FORMATS_DISPLAY}) case insensitive."
                ),
            ));
        }

        // === Build up the external volume
        let external_volume = Self::get_model_config_value(model_config, "external_volume")
            .or_else(|| Self::yml_str(write_integration, "external_volume".to_string()));

        // === Build up base location
        let base_location_root = Self::get_model_config_value(model_config, "base_location_root")
            .or_else(|| Self::yml_str(write_integration, "base_location_root".to_string()));

        let base_location_subpath =
            Self::get_model_config_value(model_config, "base_location_subpath")
                .or_else(|| Self::yml_str(write_integration, "base_location_subpath".to_string()));

        let schema = Self::get_model_config_value(model_config, "schema");
        let identifier = Self::get_model_config_value(model_config, "identifier");

        let base_location = Self::build_base_location(
            &base_location_root,
            &base_location_subpath,
            &schema,
            &identifier,
        );

        // 4) adapter_properties from YAML write_integration.adapter_properties and model config overrides
        let adapter_properties = Self::merged_adapter_properties(model_config, write_integration);

        // 5) transient handling
        let transient_spec = Self::get_model_config_value(model_config, "transient");

        if table_format.eq_ignore_ascii_case(ICEBERG_TABLE_FORMAT) && transient_spec.is_some() {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "transient may not be specified for ICEBERG catalogs. Snowflake built-in catalog DDL does not support transient ICEBERG tables.",
            ));
        }

        Ok(CatalogRelation {
            catalog_name: Some(catalog_name.to_string()),
            integration_name: Some(integration_name),
            catalog_type: catalog_type.to_string(),
            table_format,
            external_volume,
            base_location: Some(base_location),
            adapter_properties,
            is_transient: false, // catalogs.yml hardcoded to iceberg table_format => always false
        })
    }

    // [DELIBERATE CHANGE] Core always has schema and identifier in model config,
    // but we do not apparently. This can subtly change location paths in external volumes.
    // https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-snowflake/src/dbt/adapters/snowflake/parse_model.py#L34
    fn build_base_location(
        root: &Option<String>,
        subpath: &Option<String>,
        schema: &Option<String>,
        identifier: &Option<String>,
    ) -> String {
        // default prefix if not provided
        // see core: https://github.com/dbt-labs/dbt-adapters/blob/80b505709373d0eb027ad0311b16f09c8a4b9bad/dbt-snowflake/src/dbt/adapters/snowflake/parse_model.py#L40
        let prefix = root
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("_dbt");

        let mut parts = vec![prefix.to_string()];

        if let Some(s) = schema.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            parts.push(s.to_string());
        }
        // https://github.com/dbt-labs/dbt-adapters/blob/80b505709373d0eb027ad0311b16f09c8a4b9bad/dbt-snowflake/src/dbt/adapters/snowflake/parse_model.py#L41C5-L41C57
        if let Some(id) = identifier
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            parts.push(id.to_string());
        }
        if let Some(sp) = subpath.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            parts.push(sp.to_string());
        }

        parts.join("/")
    }

    /// Build the effective `adapter_properties` by combining values from
    /// - catalogs.yml `adapter_properties` (base set), and
    /// - model config values (which override when present).
    ///
    /// The precedence is:
    ///     model_config > catalogs.yml
    fn merged_adapter_properties(
        model_config: &Value,
        write_integration: Option<&YmlMapping>,
    ) -> BTreeMap<String, String> {
        let mut merged = BTreeMap::new();

        // 1) seed from catalogs.yml
        if let Some(YmlValue::Mapping(adapter_props, _)) =
            write_integration.and_then(|m| m.get(key("adapter_properties".to_string())))
        {
            for (k, v) in adapter_props {
                if let Some(name) = k.as_str()
                    && let Some(s) = Self::yaml_scalar_to_string(v)
                {
                    merged.insert(name.to_string(), s);
                }
            }
        }

        // 2) overlay model adapter_properties if present
        if let Some(model_props) = Self::get_model_adapter_properties(model_config) {
            for (k, v) in model_props {
                merged.insert(k, v);
            }
        }

        merged
    }

    fn yaml_scalar_to_string(v: &YmlValue) -> Option<String> {
        if let Some(b) = v.as_bool() {
            return Some(if b { "true".into() } else { "false".into() });
        }
        if let Some(s) = v.as_str() {
            return Some(s.to_owned());
        }
        if let Some(i) = v.as_i64() {
            return Some(i.to_string());
        }
        if let Some(u) = v.as_u64() {
            return Some(u.to_string());
        }
        debug_assert!(false, "unexpected YAML scalar: {v:?}");
        None
    }

    //
    // === Value Extractors
    //

    // [DELIBERATE CHANGE]: serialization can sometimes serialize None into Some("none")
    // which is not how core reads values in.
    fn get_model_config_value(model_config: &Value, key: &str) -> Option<String> {
        match model_config.get_attr(key) {
            Ok(v) if !v.is_undefined() => {
                let s = v.to_string();
                if s.is_empty() || s.eq_ignore_ascii_case("none") {
                    None
                } else {
                    Some(s)
                }
            }
            _ => None,
        }
    }

    fn get_model_adapter_properties(model_config: &Value) -> Option<BTreeMap<String, String>> {
        if let Ok(adapter_properties_val) = model_config.get_attr("adapter_properties") {
            if adapter_properties_val.is_undefined() {
                return None;
            }

            let mut map = BTreeMap::new();
            if let Ok(keys) = adapter_properties_val.try_iter() {
                for key in keys {
                    let key_str = key
                        .as_str()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| key.to_string());
                    if let Ok(val) = adapter_properties_val.get_item(&key) {
                        let val_str = val
                            .as_str()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| val.to_string());
                        map.insert(key_str, val_str);
                    }
                }
            }

            Some(map)
        } else {
            None
        }
    }

    fn yml_str(m: Option<&YmlMapping>, k: String) -> Option<String> {
        m.and_then(|mm| mm.get(key(k)))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    #[inline]
    fn lookup_write_integration<'a>(
        catalog: &'a YmlMapping,
        integration_name: &str,
    ) -> Option<&'a YmlMapping> {
        let seq = catalog
            .get(key("write_integrations".to_string()))?
            .as_sequence()?;
        seq.iter().filter_map(|v| v.as_mapping()).find(|m| {
            m.get(key("name".to_string()))
                .or_else(|| m.get(key("integration_name".to_string())))
                .and_then(|v| v.as_str())
                .map(|s| s == integration_name)
                .unwrap_or(false)
        })
    }

    // === Value extraction helpers
    fn map_opt_str(v: Option<String>) -> Value {
        match v.as_deref().map(|s| s.trim()).filter(|t| !t.is_empty()) {
            Some(t) => Value::from(t),
            None => Value::from(()),
        }
    }

    // plain String fields (always defined, but still check empty)
    fn map_str_val(v: &str) -> Value {
        Value::from(v)
    }

    // === from adapter_properties
    fn map_properties_str(m: &BTreeMap<String, String>, k: &str) -> Value {
        match m.get(k).map(|s| s.trim()).filter(|t| !t.is_empty()) {
            Some(t) => Value::from(t),
            None => Value::from(()),
        }
    }

    fn map_properties_bool(m: &BTreeMap<String, String>, k: &str) -> Value {
        match m.get(k) {
            Some(s) => Value::from(s.trim().eq_ignore_ascii_case("true")),
            None => Value::from(()),
        }
    }

    fn map_properties_u32(m: &BTreeMap<String, String>, k: &str) -> Value {
        match m.get(k).and_then(|s| s.trim().parse::<u32>().ok()) {
            Some(n) => Value::from(n as i64),
            None => Value::from(()),
        }
    }

    // === begin HACK
    /// Returns true if `db_name` appears under any write_integration whose
    /// `catalog_type` is `iceberg_rest` and whose
    /// `adapter_properties.catalog_linked_database` equals `db_name`.
    fn cld_exists_in_iceberg_rest(catalogs: &YmlMapping, db_name: &str) -> bool {
        let Some(seq) = catalogs
            .get(key("catalogs".to_string()))
            .and_then(|v| v.as_sequence())
        else {
            return false;
        };

        for cat in seq.iter().filter_map(|v| v.as_mapping()) {
            let Some(write_integrations) = cat
                .get(key("write_integrations".to_string()))
                .and_then(|v| v.as_sequence())
            else {
                continue;
            };
            for write_integration in write_integrations.iter().filter_map(|v| v.as_mapping()) {
                if let Some(ct) = write_integration
                    .get(key("catalog_type".into()))
                    .and_then(|v| v.as_str())
                    && !ct.eq_ignore_ascii_case("iceberg_rest")
                {
                    continue;
                }
                let adapter_properties = write_integration
                    .get(key("adapter_properties".to_string()))
                    .and_then(|v| v.as_mapping());
                if let Some(cld) = adapter_properties
                    .and_then(|m| m.get(key("catalog_linked_database".to_string())))
                    .and_then(|v| v.as_str())
                    && cld.eq_ignore_ascii_case(db_name)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Build an "empty" catalog relation: everything None/empty, falling back
    /// to the INFO_SCHEMA store and DEFAULT table format.
    pub fn empty_catalog_relation() -> Self {
        CatalogRelation {
            catalog_name: None,
            integration_name: None,
            catalog_type: SNOWFLAKE_RELATION_STORE.to_string(),
            table_format: DEFAULT_TABLE_FORMAT.to_string(),
            external_volume: None,
            base_location: None,
            adapter_properties: BTreeMap::new(),
            is_transient: true, // default transient for DEFAULT table format
        }
    }

    // === end HACK
}

#[inline]
fn key(key: String) -> YmlValue {
    YmlValue::String(key, Span::default())
}

fn find_catalog<'a>(catalogs: &'a YmlMapping, catalog_name: &str) -> Option<&'a YmlMapping> {
    let seq = catalogs.get(key("catalogs".to_string()))?.as_sequence()?;
    seq.iter().filter_map(|v| v.as_mapping()).find(|m| {
        // match on name or catalog_name
        let n1 = m.get(key("name".to_string())).and_then(|v| v.as_str());
        let n2 = m
            .get(key("catalog_name".to_string()))
            .and_then(|v| v.as_str());
        // backwards compatbility measure for dbt snowflake only
        // see: https://github.com/dbt-labs/dbt-adapters/pull/1134
        let n3 = m.get(key("catalog".to_string())).and_then(|v| v.as_str());
        n1 == Some(catalog_name) || n2 == Some(catalog_name) || n3 == Some(catalog_name)
    })
}

fn lookup_integration_name(catalogs: &YmlMapping, catalog_name: &str) -> Option<String> {
    let cat = find_catalog(catalogs, catalog_name)?;
    cat.get(key("active_write_integration".to_string()))?
        .as_str()
        .map(|s| s.to_string())
}

impl Object for CatalogRelation {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        Some(match key.as_str()? {
            // identity / routing
            "catalog_name" => Self::map_opt_str(self.catalog_name.clone()),
            "integration_name" => Self::map_opt_str(self.integration_name.clone()),

            // required for any catalog relation
            "catalog_type" => Self::map_str_val(self.catalog_type.as_str()),
            "table_format" => Self::map_str_val(self.table_format.as_str()),

            // common optional
            "external_volume" => Self::map_opt_str(self.external_volume.clone()),
            "base_location" => Self::map_opt_str(self.base_location.clone()),

            // expose full map
            "adapter_properties" => Value::from_serialize(self.adapter_properties.clone()),

            // === Adapter properties

            // all via adapter_properties
            "max_data_extension_time_in_days" => Self::map_properties_u32(
                &self.adapter_properties,
                "max_data_extension_time_in_days",
            ),

            // BUILT_IN
            "change_tracking" => {
                Self::map_properties_bool(&self.adapter_properties, "change_tracking")
            }
            "data_retention_time_in_days" => {
                Self::map_properties_u32(&self.adapter_properties, "data_retention_time_in_days")
            }
            "storage_serialization_policy" => {
                Self::map_properties_str(&self.adapter_properties, "storage_serialization_policy")
            }

            // REST
            "auto_refresh" => Self::map_properties_bool(&self.adapter_properties, "auto_refresh"),
            "catalog_linked_database" => {
                Self::map_properties_str(&self.adapter_properties, "catalog_linked_database")
            }
            "target_file_size" => {
                Self::map_properties_str(&self.adapter_properties, "target_file_size")
            }

            // HELPER
            "is_transient" => Value::from(self.is_transient),

            _ => Value::from(()),
        })
    }

    fn render(self: &Arc<Self>, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CatalogRelation(catalog={}, integration={}, type={}, format={})",
            self.catalog_name.as_deref().unwrap_or("<none>"),
            self.integration_name.as_deref().unwrap_or("<none>"),
            self.catalog_type,
            self.table_format
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::Value as JVal;
    use serde_json::json;

    fn model(v: serde_json::Value) -> JVal {
        JVal::from_serialize(v)
    }

    fn s(s: &str) -> YmlValue {
        YmlValue::String(s.to_owned(), Span::default())
    }
    fn boolv(b: bool) -> YmlValue {
        YmlValue::Bool(b, Span::default())
    }
    fn i64v(n: i64) -> YmlValue {
        YmlValue::Number(n.into(), Span::default())
    }
    fn u64v(n: u64) -> YmlValue {
        YmlValue::Number(n.into(), Span::default())
    }
    fn mapping(entries: &[(&str, YmlValue)]) -> YmlMapping {
        let mut m = YmlMapping::new();
        for (k, v) in entries {
            m.insert(s(k), v.clone());
        }
        m
    }
    fn map(entries: &[(&str, YmlValue)]) -> YmlValue {
        let mut m = YmlMapping::new();
        for (k, v) in entries {
            m.insert(s(k), v.clone());
        }
        YmlValue::Mapping(m, Span::default())
    }
    fn seq(items: &[YmlValue]) -> YmlValue {
        YmlValue::Sequence(items.to_vec(), Span::default())
    }

    /// Build a valid catalogs.yml mapping for a single catalog/integration.
    fn catalogs_yaml_one(
        catalog_name: &str,
        win: &str,
        catalog_type: &str,
        table_format: &str,
        extra_integration_fields: &[(&str, YmlValue)],
    ) -> YmlMapping {
        let mut wi = mapping(&[
            ("name", s(win)),
            ("catalog_type", s(catalog_type)),
            ("table_format", s(table_format)),
        ]);
        for (k, v) in extra_integration_fields {
            wi.insert(s(k), v.clone());
        }
        let cat = mapping(&[
            ("name", s(catalog_name)),
            ("active_write_integration", s(win)),
            (
                "write_integrations",
                seq(&[YmlValue::Mapping(wi, Span::default())]),
            ),
        ]);
        mapping(&[("catalogs", seq(&[YmlValue::Mapping(cat, Span::default())]))])
    }

    //
    // --- legacy config (no catalogs.yml) ---
    //

    #[test]
    fn legacy_default_implied_ok_and_forbids_external_and_base_location_fields() {
        // default implied
        let m = model(json!({ "schema": "S", "identifier": "I" }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert_eq!(r.table_format, DEFAULT_TABLE_FORMAT);
        assert_eq!(r.catalog_type, SNOWFLAKE_RELATION_STORE);
        assert!(r.external_volume.is_none());
        assert!(r.base_location.is_none());
        assert!(r.adapter_properties.is_empty());

        // forbidden on DEFAULT (implied)
        for (k, v) in [
            ("external_volume", "EV"),
            ("base_location_root", "root"),
            ("base_location_subpath", "sub"),
        ] {
            let m = model(json!({ k: v }));
            let err = CatalogRelation::build_without_catalogs_yml(&m).unwrap_err();
            assert!(format!("{err}").contains("not able to be specified on table_format=default"));
        }
    }

    #[test]
    fn legacy_default_explicit_ok_and_forbids_externals() {
        let m = model(json!({ "table_format": "DEFAULT" }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert_eq!(r.table_format, DEFAULT_TABLE_FORMAT);
        assert_eq!(r.catalog_type, SNOWFLAKE_RELATION_STORE);

        let m = model(json!({ "table_format": "DEFAULT", "external_volume": "EV" }));
        let err = CatalogRelation::build_without_catalogs_yml(&m).unwrap_err();
        assert!(format!("{err}").contains("not able to be specified on table_format=default"));
    }

    #[test]
    fn legacy_iceberg_sets_built_in_and_synthesizes_base_location() {
        let m = model(json!({
            "table_format": "ICEBERG",
            "external_volume": "EV",
            "base_location_root": "_root",
            "base_location_subpath": "sub",
            "schema": "SCH",
            "identifier": "ID"
        }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert_eq!(r.catalog_type, ICEBERG_BUILT_IN_CATALOG);
        assert_eq!(r.table_format, "ICEBERG");
        assert_eq!(r.external_volume.as_deref(), Some("EV"));
        assert_eq!(r.base_location.as_deref(), Some("_root/SCH/ID/sub"));
    }

    #[test]
    fn legacy_only_default_or_iceberg_allowed() {
        let m = model(json!({ "table_format": "PARQUET" }));
        let err = CatalogRelation::build_without_catalogs_yml(&m).unwrap_err();
        assert!(format!("{err}").contains("Unsupported table_format='PARQUET'"));
        assert!(format!("{err}").contains(ALLOWED_TABLE_FORMATS_DISPLAY));
    }

    #[test]
    fn legacy_catalog_type_forbidden_at_model_level() {
        let m = model(json!({ "catalog_type": "BUILT_IN" }));
        let err = CatalogRelation::build_without_catalogs_yml(&m).unwrap_err();
        assert!(
            format!("{err}")
                .contains("catalog_type may only be specified in catalog entries of catalogs.yml")
        );
    }

    #[test]
    fn legacy_adapter_properties_blocked_and_transient_ignored() {
        // adapter_properties blocked
        let m = model(json!({ "adapter_properties": { "x": "y" } }));
        let err = CatalogRelation::build_without_catalogs_yml(&m).unwrap_err();
        assert!(format!("{err}").contains("'adapter_properties' may only be specified"));

        // transient is ignored (no error, no effect)
        let m = model(json!({ "transient": true }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert_eq!(r.table_format, DEFAULT_TABLE_FORMAT);
        assert!(r.adapter_properties.is_empty());
    }

    //
    // --- base location ---
    //

    #[test]
    fn base_location_defaults_and_order() {
        assert_eq!(
            CatalogRelation::build_base_location(&None, &None, &None, &None),
            "_dbt"
        );
        assert_eq!(
            CatalogRelation::build_base_location(&None, &None, &Some("S".into()), &None),
            "_dbt/S"
        );
        assert_eq!(
            CatalogRelation::build_base_location(
                &None,
                &None,
                &Some("S".into()),
                &Some("I".into())
            ),
            "_dbt/S/I"
        );
        assert_eq!(
            CatalogRelation::build_base_location(
                &Some("_root".into()),
                &Some("sub".into()),
                &Some("S".into()),
                &Some("I".into())
            ),
            "_root/S/I/sub"
        );
    }

    //
    // --- from_model_config_and_catalogs orchestration ---
    //

    #[test]
    fn from_model_no_catalog_name_uses_legacy_path() {
        let m = model(json!({}));
        let r = CatalogRelation::from_model_config_and_catalogs(&m, None).unwrap();
        assert_eq!(r.table_format, DEFAULT_TABLE_FORMAT);
        assert_eq!(r.catalog_type, SNOWFLAKE_RELATION_STORE);
    }

    #[test]
    fn from_model_catalog_name_without_catalogs_errors() {
        let m = model(json!({ "catalog_name": "CAT" }));
        let err = CatalogRelation::from_model_config_and_catalogs(&m, None).unwrap_err();
        assert!(format!("{err}").contains("catalog_name 'CAT'"));
        assert!(format!("{err}").contains("catalogs.yml was not found"));
    }

    #[test]
    fn from_model_catalog_name_string_none_is_treated_as_absent() {
        // "none" (any case) treated as not provided -> legacy
        let m = model(json!({ "catalog_name": "None" }));
        let r = CatalogRelation::from_model_config_and_catalogs(&m, None).unwrap();
        assert_eq!(r.table_format, DEFAULT_TABLE_FORMAT);
        assert!(r.catalog_name.is_none());
    }

    //
    // --- catalogs.yml reconciliation ---
    //

    #[test]
    fn catalogs_reconciliation_model_overrides_and_merging() {
        let cats = catalogs_yaml_one(
            "CAT",
            "WIN",
            "BUILT_IN",
            "ICEBERG",
            &[
                ("external_volume", s("EV_YAML")),
                ("base_location_root", s("_root_yaml")),
                ("base_location_subpath", s("sub_yaml")),
                (
                    "adapter_properties",
                    map(&[
                        ("change_tracking", boolv(true)),
                        ("target_file_size", u64v(128)),
                        ("storage_serialization_policy", s("SNAPPY")),
                    ]),
                ),
            ],
        );

        let m = model(json!({
            "catalog_name": "CAT",
            "table_format": "ICEBERG",
            "schema": "S",
            "identifier": "I",
            "external_volume": "EV_MODEL",
            "base_location_subpath": "sub_model",
            "adapter_properties": { "storage_serialization_policy": "ZSTD" }
        }));

        let r = CatalogRelation::build_with_catalogs(&m, &cats, "CAT").unwrap();
        assert_eq!(r.catalog_name.as_deref(), Some("CAT"));
        assert_eq!(r.integration_name.as_deref(), Some("WIN"));
        assert_eq!(r.catalog_type, "BUILT_IN");
        assert_eq!(r.table_format, "ICEBERG");

        // precedence: model > catalogs.yml
        assert_eq!(r.external_volume.as_deref(), Some("EV_MODEL"));
        assert_eq!(r.base_location.as_deref(), Some("_root_yaml/S/I/sub_model"));

        // merged adapter_properties; model override wins
        assert_eq!(
            r.adapter_properties
                .get("change_tracking")
                .map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(
            r.adapter_properties
                .get("target_file_size")
                .map(|s| s.as_str()),
            Some("128")
        );
        assert_eq!(
            r.adapter_properties
                .get("storage_serialization_policy")
                .map(|s| s.as_str()),
            Some("ZSTD")
        );
    }

    #[test]
    fn catalogs_iceberg_flow_is_respected() {
        let cats = catalogs_yaml_one(
            "CAT",
            "WIN",
            "BUILT_IN",
            "ICEBERG",
            &[
                ("external_volume", s("EV")),
                ("base_location_root", s("_root")),
                ("base_location_subpath", s("sub")),
            ],
        );
        let m = model(json!({ "catalog_name": "CAT", "schema": "S", "identifier": "I" }));

        let r = CatalogRelation::build_with_catalogs(&m, &cats, "CAT").unwrap();
        assert_eq!(r.catalog_type, "BUILT_IN");
        assert_eq!(r.table_format, "ICEBERG");
        assert_eq!(r.external_volume.as_deref(), Some("EV"));
        assert_eq!(r.base_location.as_deref(), Some("_root/S/I/sub"));
    }

    #[test]
    fn catalogs_bad_table_format_in_model_override_is_rejected() {
        let cats = catalogs_yaml_one("CAT", "WIN", "BUILT_IN", "DEFAULT", &[]);
        let m = model(json!({ "catalog_name": "CAT", "table_format": "FANCY" }));
        let err = CatalogRelation::build_with_catalogs(&m, &cats, "CAT").unwrap_err();
        assert!(format!("{err}").contains("Unsupported table_format 'FANCY'"));
        assert!(format!("{err}").contains(ALLOWED_TABLE_FORMATS_DISPLAY));
    }

    #[test]
    fn catalogs_model_cannot_override_catalog_type() {
        let cats = catalogs_yaml_one("CAT", "WIN", "INFO_SCHEMA", "DEFAULT", &[]);
        let m = model(json!({ "catalog_name": "CAT", "catalog_type": "BUILT_IN" }));
        let err = CatalogRelation::build_with_catalogs(&m, &cats, "CAT").unwrap_err();
        assert!(format!("{err}").contains(
            "catalog_type may only be specified in write integration entries of catalogs.yml"
        ));
    }

    #[test]
    fn model_root_override_trims() {
        let bl = CatalogRelation::build_base_location(
            &Some("   root_with_spaces   ".into()),
            &None,
            &Some("S".into()),
            &Some("I".into()),
        );
        assert_eq!(bl, "root_with_spaces/S/I");
    }

    #[test]
    fn yaml_scalar_normalization_bool_i64_u64() {
        assert_eq!(
            CatalogRelation::yaml_scalar_to_string(&boolv(true)),
            Some("true".into())
        );
        assert_eq!(
            CatalogRelation::yaml_scalar_to_string(&i64v(-5)),
            Some("-5".into())
        );
        assert_eq!(
            CatalogRelation::yaml_scalar_to_string(&u64v(42)),
            Some("42".into())
        );
    }

    #[test]
    fn fallback_base_location_defaults_to_dbt() {
        // no root/subpath in model or yaml
        let bl = CatalogRelation::build_base_location(
            &None,
            &None,
            &Some("S".into()),
            &Some("I".into()),
        );
        assert_eq!(bl, "_dbt/S/I");
    }

    //
    // --- is transient reconciliation ---
    //
    #[test]
    fn legacy_default_transient_unspecified_defaults_true() {
        let m = model(json!({ "table_format": "DEFAULT" }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert!(r.is_transient);
    }

    #[test]
    fn legacy_default_transient_false_explicit() {
        let m = model(json!({ "table_format": "DEFAULT", "transient": false }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert!(!r.is_transient);
    }

    #[test]
    fn legacy_default_transient_true_explicit() {
        let m = model(json!({ "table_format": "DEFAULT", "transient": true }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert!(r.is_transient);
    }

    #[test]
    fn legacy_iceberg_any_transient_specified_is_error() {
        let m = model(json!({ "table_format": "ICEBERG", "transient": false }));
        let err = CatalogRelation::build_without_catalogs_yml(&m).unwrap_err();
        assert!(format!("{err}").contains("transient may not be specified for ICEBERG"));
    }

    #[test]
    fn legacy_iceberg_unspecified_transient_defaults_false() {
        let m = model(json!({ "table_format": "ICEBERG" }));
        let r = CatalogRelation::build_without_catalogs_yml(&m).unwrap();
        assert!(!r.is_transient);
    }

    #[test]
    fn catalogs_iceberg_unspecified_transient_defaults_false() {
        let cats = catalogs_yaml_one("CAT", "WIN", "BUILT_IN", "ICEBERG", &[]);
        let m = model(json!({ "catalog_name": "CAT" }));
        let r = CatalogRelation::build_with_catalogs(&m, &cats, "CAT").unwrap();
        assert!(!r.is_transient);
    }

    #[test]
    fn catalogs_iceberg_any_transient_specified_is_error() {
        let cats = catalogs_yaml_one("CAT", "WIN", "BUILT_IN", "ICEBERG", &[]);
        let m = model(json!({ "catalog_name": "CAT", "transient": true }));
        let err = CatalogRelation::build_with_catalogs(&m, &cats, "CAT").unwrap_err();
        assert!(format!("{err}").contains("transient may not be specified for ICEBERG"));
    }
}
