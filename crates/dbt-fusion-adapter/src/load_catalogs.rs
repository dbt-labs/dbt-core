use dbt_common::{ErrorCode, FsResult, fs_err};
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
pub fn load_catalogs(text_yml: yml::Value, path: &Path) -> FsResult<()> {
    let validated = do_load_catalogs(text_yml, path)?;
    let mut write_guard = match CATALOGS.write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *write_guard = Some(Arc::new(validated));
    Ok(())
}

pub fn do_load_catalogs(text_yml: yml::Value, path: &Path) -> FsResult<DbtCatalogs> {
    let _guard = yml::with_filename(Some(path.to_path_buf()));

    let (repr, span) = match text_yml {
        yml::Value::Mapping(mapping, span) => (mapping, span),
        _ => {
            return Err(fs_err!(
                code => ErrorCode::InvalidConfig,
                loc  => path.to_path_buf(),
                "Top-level of '{}' must be a YAML mapping", path.display()
            ));
        }
    };

    let catalogs = DbtCatalogs { repr, span };
    let view = catalogs.view()?;
    validate_catalogs(&view, path)?;
    Ok(catalogs)
}
