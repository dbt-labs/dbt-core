//! This module contains the functions for initializing the Jinja environment for the load phase.

use std::{collections::BTreeMap, str::FromStr as _, sync::Arc};

use chrono::DateTime;
use chrono_tz::Tz;
use dbt_common::{
    ErrorCode, FsResult, adapter::AdapterType, cancellation::CancellationToken, fs_err,
    io_args::IoArgs,
};
use dbt_fusion_adapter::parse::adapter::create_parse_adapter;
use dbt_schemas::{
    dbt_utils::resolve_package_quoting,
    schemas::profiles::{DbConfig, TargetContext},
};
use minijinja::value::Value as MinijinjaValue;
use minijinja_contrib::modules::{py_datetime::datetime::PyDateTime, pytz::PytzTimezone};

use crate::{
    environment_builder::JinjaEnvBuilder, jinja_environment::JinjaEnv,
    phases::utils::build_target_context_map,
};

/// Initialize load_profile jinja environment
pub fn initialize_load_profile_jinja_environment() -> JinjaEnv {
    JinjaEnvBuilder::new().build()
}

/// Initialize a Jinja environment for the load phase.
#[allow(clippy::too_many_arguments)]
pub fn initialize_load_jinja_environment(
    profile: &str,
    target: &str,
    adapter_type: &str,
    db_config: DbConfig,
    run_started_at: DateTime<Tz>,
    flags: &BTreeMap<String, minijinja::Value>,
    io_args: IoArgs,
    token: CancellationToken,
) -> FsResult<JinjaEnv> {
    let target_context = TargetContext::try_from(db_config)
        .map_err(|e| fs_err!(ErrorCode::InvalidConfig, "{}", &e))?;
    let target_context = Arc::new(build_target_context_map(profile, target, target_context));
    let globals = BTreeMap::from([
        (
            "run_started_at".to_string(),
            MinijinjaValue::from_object(PyDateTime::new_aware(
                run_started_at,
                Some(PytzTimezone::new(Tz::UTC)),
            )),
        ),
        (
            "target".to_string(),
            MinijinjaValue::from_serialize(target_context),
        ),
        ("flags".to_string(), MinijinjaValue::from_serialize(flags)),
    ]);

    let adapter_type = AdapterType::from_str(adapter_type).map_err(|_| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Unknown or unsupported adapter type '{adapter_type}'",
        )
    })?;

    let package_quoting = resolve_package_quoting(None, adapter_type);

    Ok(JinjaEnvBuilder::new()
        .with_adapter(create_parse_adapter(adapter_type, package_quoting, token)?)
        .with_root_package("dbt".to_string())
        .with_io_args(io_args)
        .with_globals(globals)
        .build())
}
