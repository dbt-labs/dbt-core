use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::serde::yaml_to_fs_error;
use dbt_schemas::schemas::{dbt_catalogs::DbtCatalogs, validate_catalogs};
use dbt_serde_yaml as yml;
use std::path::Path;
use std::sync::{Arc, RwLock};

static CATALOGS: RwLock<Option<Arc<DbtCatalogs>>> = RwLock::new(None);

/// Reader: returns a read guard to loaded catalogs.yml if present
pub fn fetch_catalogs() -> Option<Arc<DbtCatalogs>> {
    match CATALOGS.read() {
        Ok(g) => g.as_ref().cloned(),
        Err(p) => p.into_inner().as_ref().cloned(),
    }
}

/// Load <project_root>/catalogs.yml, validate, and return a validated mapping holder.
pub fn load_catalogs(text: &str, path: &Path) -> FsResult<()> {
    let validated = do_load_catalogs(text, path)?;
    let mut write_guard = match CATALOGS.write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *write_guard = Some(Arc::new(validated));
    Ok(())
}

pub fn do_load_catalogs(text: &str, path: &Path) -> FsResult<DbtCatalogs> {
    let text_yml: yml::Value = yml::from_str(text).map_err(|e| yaml_to_fs_error(e, Some(path)))?;

    let repr = match text_yml {
        yml::Value::Mapping(mapping, _) => mapping,
        _ => {
            return Err(fs_err!(
                code => ErrorCode::InvalidConfig,
                loc  => path.to_path_buf(),
                "Top-level of '{}' must be a YAML mapping", path.display()
            ));
        }
    };

    let catalogs = DbtCatalogs { repr };
    let view = catalogs.view()?;
    validate_catalogs(&view, path)?;
    Ok(catalogs)
}

#[cfg(test)]
mod tests_pure {
    use super::*;
    use std::path::Path;

    fn good_yaml() -> &'static str {
        r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: built_in
        table_format: iceberg
        external_volume: ev
"#
    }

    fn bad_yaml_missing_cld_rest_writer() -> &'static str {
        r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: iceberg_rest
        table_format: iceberg
        adapter_properties:
          target_file_size: "64MB"
"#
    }

    #[test]
    fn build_catalogs_validates_and_returns_holder() {
        let path = Path::new("<test>/catalogs.yml");
        let holder = do_load_catalogs(good_yaml(), path).expect("should parse and validate");
        let view = holder.view().expect("view parses");
        assert_eq!(view.catalogs.len(), 1);
        assert_eq!(view.catalogs[0].catalog_name, "c1");
    }

    #[test]
    fn build_catalogs_surfaces_validation_error() {
        let path = Path::new("<test>/catalogs.yml");
        let err = do_load_catalogs(bad_yaml_missing_cld_rest_writer(), path).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains(
                "'catalog_linked_database' is required and cannot be blank for Iceberg REST"
            ),
            "got: {msg}",
        );
    }

    #[test]
    fn build_catalogs_rejects_non_mapping_top_level() {
        let path = Path::new("<test>/catalogs.yml");
        let err = do_load_catalogs("---\n- not a mapping\n", path).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("Top-level of '<test>/catalogs.yml' must be a YAML mapping")
                || msg.contains("Top-level of"),
            "got: {msg}",
        );
    }

    #[test]
    fn no_top_level_catalogs_key_error() {
        let path = Path::new("<test>/catalogs.yml");
        let err = do_load_catalogs("not_catalogs: []\n", path).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("Unknown key 'not_catalogs' in top-level"),
            "got: {msg}",
        );
    }
}

#[cfg(test)]
mod tests_global {
    use super::*;
    use std::path::Path;

    fn good_yaml() -> &'static str {
        r#"
catalogs:
  - name: c1
    active_write_integration: i1
    write_integrations:
      - name: i1
        catalog_type: built_in
        table_format: iceberg
        external_volume: ev
"#
    }

    #[test]
    fn load_catalogs_sets_global_and_fetches() {
        let path = Path::new("<test>/catalogs.yml");

        load_catalogs(good_yaml(), path).expect("global init should succeed");
        let holder = fetch_catalogs().expect("global should be initialized");
        let view = holder.view().expect("view parses");
        assert_eq!(view.catalogs.len(), 1);
        assert_eq!(view.catalogs[0].catalog_name, "c1");
    }

    #[test]
    fn load_catalogs_sets_global_unity() {
        let yaml = r#"
catalogs:
  - name: uc
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: unity
        table_format: iceberg
        file_format: delta
"#;
        let path = Path::new("<test>/catalogs.yml");
        load_catalogs(yaml, path).expect("global init should succeed for unity");
        let holder = fetch_catalogs().expect("global should be initialized");
        let view = holder.view().expect("view parses");
        assert_eq!(view.catalogs.len(), 1);
        assert_eq!(view.catalogs[0].catalog_name, "uc");
    }

    #[test]
    fn load_catalogs_surfaces_hms_adapter_props_error_globally() {
        let yaml = r#"
catalogs:
  - name: hms
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: hive_metastore
        table_format: default
        file_format: hudi
        adapter_properties:
          location_root: /mnt/not_allowed
"#;
        let path = Path::new("<test>/catalogs.yml");
        let err = load_catalogs(yaml, path).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains(
                "Unknown key 'adapter_properties' in write_integration(Databricks hive_metastore)"
            ),
            "got: {msg}",
        );
    }

    #[test]
    fn databricks_unity_valid_minimal_via_loader() {
        let yaml = r#"
catalogs:
  - name: uc
    active_write_integration: i
    write_integrations:
      - name: i
        catalog_type: unity
        table_format: iceberg
        file_format: delta
"#;
        let path = Path::new("<test>/catalogs.yml");
        let holder = do_load_catalogs(yaml, path).expect("unity minimal should parse+validate");
        let view = holder.view().unwrap();
        assert_eq!(view.catalogs.len(), 1);
        assert_eq!(view.catalogs[0].active_write_integration, "i");
    }
}
