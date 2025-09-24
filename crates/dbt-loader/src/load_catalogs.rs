use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::{dbt_catalogs::DbtCatalogs, validate_catalogs};
use dbt_serde_yaml as yml;
use once_cell::sync::OnceCell;
use std::path::Path;

static CATALOGS: OnceCell<DbtCatalogs> = OnceCell::new();

#[inline]
pub fn fetch_catalogs() -> Option<&'static DbtCatalogs> {
    CATALOGS.get()
}

/// Load <project_root>/catalogs.yml, validate, and return a validated mapping holder.
///
/// Synchronizes CATALOGS state by explicitly taking first arrival as source of truth
/// No-op after first invocation
pub fn load_catalogs(text: &str, dbt_catalogs_path: &Path) -> FsResult<()> {
    CATALOGS.get_or_try_init(|| do_load_catalogs(text, dbt_catalogs_path))?;
    Ok(())
}

pub fn do_load_catalogs(text: &str, path: &Path) -> FsResult<DbtCatalogs> {
    let text_yml: yml::Value =
        yml::from_str(text).map_err(|e| dbt_jinja_utils::serde::yaml_to_fs_error(e, Some(path)))?;

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

    fn bad_yaml_missing_ev_rest_writer() -> &'static str {
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
        let err = do_load_catalogs(bad_yaml_missing_ev_rest_writer(), path).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("'external_volume' required for REST write-support"),
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
}
