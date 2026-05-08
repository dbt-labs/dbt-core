use std::path::Path;

use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::error::FsError;
use dbt_common::fs_err;
use dbt_common::io_args::ComputeArg;

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

/// Validates the merged `compute` config on a node. Currently only `Remote` is supported;
/// other variants are rejected with a clear error so users see the constraint at parse time
/// rather than mid-build. The set of accepted values will widen as local-compute support
/// for additional node types stabilizes.
pub(crate) fn validate_compute(compute: Option<ComputeArg>, path: &Path) -> FsResult<()> {
    match compute {
        None | Some(ComputeArg::Remote) => Ok(()),
        Some(other) => Err(fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => path.to_path_buf(),
            "compute config currently only accepts 'remote'; got '{other}'",
        )),
    }
}
