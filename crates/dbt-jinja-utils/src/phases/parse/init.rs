//! This module contains the functions for initializing the Jinja environment for the parse phase.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use chrono::DateTime;
use chrono_tz::Tz;
use dbt_common::{fs_err, io_args::IoArgs, ErrorCode, FsResult};
use dbt_fusion_adapter::parse::adapter::create_parse_adapter;
use dbt_schemas::{
    schemas::{
        common::DbtQuoting,
        profiles::{DbConfig, TargetContext},
    },
    state::DbtVars,
};
use minijinja::{
    dispatch_object::THREAD_LOCAL_DEPENDENCIES, macro_unit::MacroUnit,
    value::Value as MinijinjaValue, UndefinedBehavior,
};
use minijinja_contrib::modules::{py_datetime::datetime::PyDateTime, pytz::PytzTimezone};

use crate::{
    environment_builder::{JinjaEnvironmentBuilder, MacroUnitsWrapper},
    flags::Flags,
    functions::ConfiguredVar,
    invocation_args::InvocationArgs,
    jinja_environment::JinjaEnvironment,
    phases::utils::build_target_context_map,
};

/// Initialize a Jinja environment for the parse phase.
#[allow(clippy::too_many_arguments)]
pub fn initialize_parse_jinja_environment(
    project_name: &str,
    profile: &str,
    target: &str,
    adapter_type: &str,
    db_config: &DbConfig,
    package_quoting: DbtQuoting,
    macro_units: BTreeMap<String, Vec<MacroUnit>>,
    vars: BTreeMap<String, BTreeMap<String, DbtVars>>,
    cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
    flags: BTreeMap<String, MinijinjaValue>,
    run_started_at: DateTime<Tz>,
    invocation_args: &InvocationArgs,
    all_package_names: BTreeSet<String>,
    io_args: IoArgs,
) -> FsResult<JinjaEnvironment<'static>> {
    // Set the thread local dependencies
    if THREAD_LOCAL_DEPENDENCIES.get().is_none() {
        THREAD_LOCAL_DEPENDENCIES
            .set(Mutex::new(all_package_names))
            .unwrap();
    }
    let target_context = TargetContext::try_from(db_config.clone())
        .map_err(|e| fs_err!(ErrorCode::InvalidConfig, "{}", &e))?;
    let target_context = Arc::new(build_target_context_map(profile, target, target_context));

    let mut prj_flags = Flags::from_project_flags(flags);
    let inv_flags = Flags::from_invocation_args(invocation_args.to_dict());
    let joined_flags = prj_flags.join(inv_flags);

    let invocation_args_dict = Arc::new(joined_flags.to_dict());

    let globals = BTreeMap::from([
        (
            "project_name".to_string(),
            MinijinjaValue::from(project_name),
        ),
        (
            "run_started_at".to_string(),
            MinijinjaValue::from_object(PyDateTime::new_aware(
                run_started_at,
                Some(PytzTimezone::new(Tz::UTC)),
            )),
        ),
        (
            "target".to_string(),
            MinijinjaValue::from_serialize(target_context.clone()),
        ),
        (
            "env".to_string(),
            MinijinjaValue::from_serialize(target_context),
        ),
        (
            "flags".to_string(),
            MinijinjaValue::from_object(joined_flags),
        ),
        (
            "invocation_args_dict".to_string(),
            MinijinjaValue::from_serialize(invocation_args_dict),
        ),
        (
            "invocation_id".to_string(),
            MinijinjaValue::from_serialize(invocation_args.invocation_id.to_string()),
        ),
        (
            "var".to_string(),
            MinijinjaValue::from_object(ConfiguredVar::new(vars, cli_vars)),
        ),
        (
            "database".to_string(),
            MinijinjaValue::from(db_config.get_database()),
        ),
        (
            "schema".to_string(),
            MinijinjaValue::from(db_config.get_schema()),
        ),
    ]);

    let mut env = JinjaEnvironmentBuilder::new()
        .with_adapter(create_parse_adapter(adapter_type, package_quoting)?)
        .with_root_package(project_name.to_string())
        .with_globals(globals)
        .with_io_args(io_args)
        .try_with_macros(MacroUnitsWrapper::new(macro_units))?
        .build();
    env.set_undefined_behavior(UndefinedBehavior::Dbt);
    Ok(env)
}
