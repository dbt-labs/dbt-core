use dbt_common::io_utils::StatusReporter;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::warn_error_options::project_flags_get_value;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::{
    dbt_catalogs::DbtCatalogs, dbt_catalogs_v2::validate_catalogs_v2, validate_catalogs,
};
use dbt_yaml as yml;
use std::path::Path;
use std::sync::{Arc, RwLock};

const CATALOGS_V2_DISCUSSION_URL: &str = "https://github.com/dbt-labs/dbt-core/discussions/12723";

static CATALOGS: RwLock<Option<Arc<DbtCatalogs>>> = RwLock::new(None);
static USE_CATALOGS_V2: RwLock<bool> = RwLock::new(false);

/// Reader: returns a read guard to loaded catalogs.yml if present
pub fn fetch_catalogs() -> Option<Arc<DbtCatalogs>> {
    match CATALOGS.read() {
        Ok(g) => g.as_ref().cloned(),
        Err(p) => p.into_inner().as_ref().cloned(),
    }
}

/// Reader: returns whether the use_catalogs_v2 behavior flag was set at load time.
pub fn fetch_use_catalogs_v2() -> bool {
    match USE_CATALOGS_V2.read() {
        Ok(g) => *g,
        Err(p) => *p.into_inner(),
    }
}

/// Load <project_root>/catalogs.yml, validate, and return a validated mapping holder.
pub fn load_catalogs(
    text_yml: yml::Value,
    path: &Path,
    project_flags: Option<&yml::Value>,
    status_reporter: Option<&Arc<dyn StatusReporter>>,
) -> FsResult<()> {
    let validated = do_load_catalogs(text_yml, path, project_flags, status_reporter)?;
    let mut write_guard = match CATALOGS.write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *write_guard = Some(Arc::new(validated));
    Ok(())
}

pub fn do_load_catalogs(
    text_yml: yml::Value,
    path: &Path,
    project_flags: Option<&yml::Value>,
    status_reporter: Option<&Arc<dyn StatusReporter>>,
) -> FsResult<DbtCatalogs> {
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

    let catalogs = DbtCatalogs::new(repr, span);
    // TODO: remove v1 after discussion/product alignment (see CATALOGS_V2_DISCUSSION_URL)
    if project_flags
        .and_then(|f| project_flags_get_value(f, "use_catalogs_v2"))
        .and_then(yml::Value::as_bool)
        .unwrap_or(false)
    {
        *match USE_CATALOGS_V2.write() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        } = true;
        emit_warn_log_message(
            ErrorCode::NotYetSupportedOption,
            format!(
                "catalogs.yml v2 schema validation is experimental, not officially supported yet, and its spec is liable to change. See {CATALOGS_V2_DISCUSSION_URL}"
            ),
            status_reporter,
        );
        let view = catalogs.view_v2()?;
        validate_catalogs_v2(&view, path)?;
    } else {
        let view = catalogs.view()?;
        validate_catalogs(&view, path)?;
    }
    Ok(catalogs)
}
