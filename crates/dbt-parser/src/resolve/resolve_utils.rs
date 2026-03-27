use std::path::Path;

use dbt_common::ErrorCode;
use dbt_common::error::FsError;
use dbt_common::fs_err;

/// Returns an error for resource names derived from filenames that contain spaces.
/// dbt does not allow spaces in resource names — this mirrors dbt-core's
/// `check_for_spaces_in_resource_names` validation.
pub(crate) fn err_resource_name_has_spaces(name: &str, path: &Path) -> Box<FsError> {
    fs_err!(
        code => ErrorCode::SchemaError,
        loc => path.to_path_buf(),
        "Resource name '{}' contains spaces. Resource names cannot contain spaces. \
         Rename '{}' to remove any spaces.",
        name,
        path.display()
    )
}
