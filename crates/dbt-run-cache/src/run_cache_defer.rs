//! dbt State auto-deferral: synthesizes profile-target defer nodes from the
//! dbt State service config when no manifest-backed defer state is in play.
//!
//! The synthesized nodes mimic previous-state defer nodes — cloned from the
//! current project and rewritten as if resolved against the configured
//! `defer_to_target` profile target — so the standard defer pipeline can treat
//! them identically.

use std::{collections::BTreeMap, sync::Arc};

use crate::service_config::RunCacheServiceConfig;
use dbt_common::{
    ErrorCode, FsResult, fs_err,
    io_args::{EvalArgs, FsCommand},
    tracing::emit::{emit_trace_log_message, emit_warn_log_message},
    warn_error_options::WarnErrorOptions,
};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv, phases::build_compile_and_run_base_context,
    register_base_functions,
};
use dbt_parser::utils::{RelationComponents, update_node_relation_components};
use dbt_profile::{
    ProfileEnvironment, ProfileError, ResolvedProfile, find_profiles_path, resolve_with_env,
};
use dbt_schemas::{
    schemas::{
        DbtFunction, DbtModel, DbtSeed, DbtSnapshot, InternalDbtNodeAttributes, Nodes,
        common::DbtMaterialization,
        profiles::{DbConfig, Execute, TargetContext},
        serde::yaml_to_fs_error,
    },
    state::ResolverState,
};

use minijinja::Value;

pub struct RunCacheProfileResolver;

impl RunCacheProfileResolver {
    /// Builds manifest-like defer nodes for dbt State auto-deferral without
    /// loading a prior manifest artifact.
    ///
    /// The returned nodes are cloned from the current project and rewritten as
    /// if they were resolved against the configured `defer_to_target` profile
    /// target.
    /// Existing defer machinery can then treat them like previous-state nodes
    /// when resolving unselected upstreams.
    pub fn synthesize_defer_nodes(
        arg: &EvalArgs,
        resolved_state: &ResolverState,
        jinja_env: &JinjaEnv,
    ) -> FsResult<Option<Nodes>> {
        let Some(auto_defer) = run_cache_auto_defer_config(
            arg,
            &resolved_state.dbt_profile.target,
            resolved_state.dbt_profile.defer_to_target.as_deref(),
        ) else {
            return Ok(None);
        };

        let db_config = match resolve_run_cache_defer_target_profile(
            arg,
            &resolved_state.dbt_profile.profile,
            &auto_defer.defer_to_target,
        ) {
            Ok(db_config) => db_config,
            Err(ProfileError::Yaml { source, path }) => {
                return Err(yaml_to_fs_error(source, Some(&path)));
            }
            Err(err) => {
                let defer_to = &auto_defer.defer_to_target;
                emit_trace_log_message(|| {
                    format!("dbt State auto-deferral could not resolve target '{defer_to}': {err}")
                });
                return Ok(None);
            }
        };

        let defer_jinja_env = jinja_env_for_run_cache_target(
            arg,
            jinja_env,
            &resolved_state.dbt_profile.profile,
            &auto_defer.defer_to_target,
            db_config.clone(),
        )?;

        let defer_base_context = run_cache_defer_base_context(resolved_state, jinja_env);

        let mut defer_nodes = resolved_state.nodes.deep_clone();
        update_run_cache_defer_nodes(
            &mut defer_nodes,
            &defer_jinja_env,
            &defer_base_context,
            resolved_state,
            &db_config,
        )?;

        Ok(Some(defer_nodes))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct RunCacheAutoDeferConfig {
    defer_to_target: String,
}

fn run_cache_auto_defer_config(
    arg: &EvalArgs,
    active_target: &str,
    profile_defer_to_target: Option<&str>,
) -> Option<RunCacheAutoDeferConfig> {
    if !run_cache_auto_defer_requested(
        arg,
        RunCacheServiceConfig::is_explicitly_requested_from_env(),
    ) {
        return None;
    }

    let config = match RunCacheServiceConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                format!(
                    "dbt State auto-deferral config failed: {err}; continuing without synthesized defer state"
                ),
                None,
            );
            return None;
        }
    };

    let defer_to_target = select_run_cache_defer_to_target(profile_defer_to_target, &config);

    if !config.enabled || active_target == defer_to_target {
        return None;
    }

    Some(RunCacheAutoDeferConfig { defer_to_target })
}

fn select_run_cache_defer_to_target(
    profile_defer_to_target: Option<&str>,
    config: &RunCacheServiceConfig,
) -> String {
    profile_defer_to_target
        .filter(|target| !target.is_empty())
        .unwrap_or(&config.defer_to)
        .to_string()
}

fn run_cache_auto_defer_requested(arg: &EvalArgs, env_requested: bool) -> bool {
    if !run_cache_auto_defer_command(arg.command) {
        return false;
    }
    if Execute::from_compute_flag(arg.local_execution_backend) != Execute::Remote {
        return false;
    }

    // Respect an explicit --no-defer. The clap layer defaults EvalArgs.defer
    // to true when no flag is passed, so `arg.defer == true` cannot tell us
    // the user opted in — but `arg.defer == false` only ever comes from
    // --no-defer, so it is a reliable opt-out signal.
    if !arg.defer {
        return false;
    }
    arg.run_cache_service || env_requested
}

fn run_cache_auto_defer_command(command: FsCommand) -> bool {
    matches!(
        command,
        FsCommand::Compile
            | FsCommand::Run
            | FsCommand::Build
            | FsCommand::Test
            | FsCommand::Seed
            | FsCommand::Snapshot
            | FsCommand::Show
    )
}

fn resolve_run_cache_defer_target_profile(
    arg: &EvalArgs,
    profile_name: &str,
    defer_to_target: &str,
) -> Result<DbConfig, ProfileError> {
    let profile_path = find_profiles_path(arg.profiles_dir.as_deref())?;
    let mut penv = ProfileEnvironment::new(arg.vars.clone());
    register_base_functions(&mut penv.env, arg.io.clone(), WarnErrorOptions::default());
    let resolved: ResolvedProfile =
        resolve_with_env(&penv, &profile_path, profile_name, Some(defer_to_target))?;

    let credentials_value =
        dbt_yaml::Value::Mapping(resolved.credentials, dbt_yaml::Span::default());
    dbt_yaml::from_value(credentials_value).map_err(|source| ProfileError::Yaml {
        source,
        path: profile_path,
    })
}

fn jinja_env_for_run_cache_target(
    arg: &EvalArgs,
    jinja_env: &JinjaEnv,
    profile_name: &str,
    target_name: &str,
    db_config: DbConfig,
) -> FsResult<JinjaEnv> {
    if db_config.has_removed_execute_field() {
        emit_warn_log_message(
            ErrorCode::DeprecatedOption,
            "The `execute:` field in profiles.yml is no longer supported and will be ignored. \
             Use the `--compute inline|sidecar|service|remote` CLI flag instead. \
             Please remove `execute:` from your profile.",
            arg.io.status_reporter.as_ref(),
        );
    }

    let database = db_config.get_database().cloned();
    let schema = db_config.get_schema().cloned();
    let target_context = TargetContext::try_from(db_config)
        .map_err(|e| fs_err!(ErrorCode::InvalidConfig, "{}", e))?;
    let target_context = run_cache_target_context_map(profile_name, target_name, target_context);
    let target_context = Arc::new(target_context);

    let mut defer_jinja_env = jinja_env.clone();
    defer_jinja_env
        .env
        .add_global("target", Value::from_serialize(target_context.clone()));
    defer_jinja_env
        .env
        .add_global("env", Value::from_serialize(target_context));
    defer_jinja_env
        .env
        .add_global("database", Value::from(database));
    defer_jinja_env
        .env
        .add_global("schema", Value::from(schema));
    Ok(defer_jinja_env)
}

fn run_cache_target_context_map(
    profile_name: &str,
    target_name: &str,
    target_context: TargetContext,
) -> BTreeMap<String, Value> {
    let target_context_value =
        dbt_yaml::to_value(&target_context).expect("TargetContext should serialize to YAML");
    let mut target_context_map: BTreeMap<String, Value> =
        dbt_yaml::from_value(target_context_value).expect("TargetContext should convert to Jinja");
    target_context_map.insert("profile_name".to_string(), Value::from(profile_name));
    target_context_map.insert("name".to_string(), Value::from(target_name));
    target_context_map.insert("target_name".to_string(), Value::from(target_name));
    target_context_map
}

fn run_cache_defer_base_context(
    resolved_state: &ResolverState,
    jinja_env: &JinjaEnv,
) -> BTreeMap<String, Value> {
    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|registry| registry.keys().map(|key| key.to_string()).collect())
        .unwrap_or_default();

    build_compile_and_run_base_context(
        resolved_state.node_resolver.clone(),
        &resolved_state.root_project_name,
        &resolved_state.nodes,
        resolved_state.defer_nodes.as_ref(),
        resolved_state.runtime_config.clone(),
        namespace_keys,
    )
}

/// Rewrites cloned current-project nodes so they can stand in for
/// previous-state defer nodes at the dbt State `defer_to` target.
///
/// Each cacheable node gets relation components rendered with the target
/// profile's database/schema/Jinja context, which lets standard defer handling
/// resolve unselected upstreams to that target. Ephemeral and inline models are
/// removed because they do not materialize to warehouse relations.
fn update_run_cache_defer_nodes(
    defer_nodes: &mut Nodes,
    jinja_env: &JinjaEnv,
    base_context: &BTreeMap<String, Value>,
    resolved_state: &ResolverState,
    db_config: &DbConfig,
) -> FsResult<()> {
    let default_database = db_config.get_database_or_default();
    let default_schema = db_config
        .get_schema()
        .map(String::as_str)
        .unwrap_or("public")
        .to_string();

    for model in defer_nodes.models.values_mut() {
        update_run_cache_defer_model(
            Arc::make_mut(model),
            jinja_env,
            base_context,
            resolved_state,
            &default_database,
            &default_schema,
        )?;
    }
    for seed in defer_nodes.seeds.values_mut() {
        update_run_cache_defer_seed(
            Arc::make_mut(seed),
            jinja_env,
            base_context,
            resolved_state,
            &default_database,
            &default_schema,
        )?;
    }
    for snapshot in defer_nodes.snapshots.values_mut() {
        update_run_cache_defer_snapshot(
            Arc::make_mut(snapshot),
            jinja_env,
            base_context,
            resolved_state,
            &default_database,
            &default_schema,
        )?;
    }
    for function in defer_nodes.functions.values_mut() {
        update_run_cache_defer_function(
            Arc::make_mut(function),
            jinja_env,
            base_context,
            resolved_state,
            &default_database,
            &default_schema,
        )?;
    }

    defer_nodes.models.retain(|_, model| {
        model.__base_attr__.materialized != DbtMaterialization::Ephemeral
            && model.__base_attr__.materialized != DbtMaterialization::Inline
    });
    Ok(())
}

fn update_run_cache_defer_model(
    model: &mut DbtModel,
    jinja_env: &JinjaEnv,
    base_context: &BTreeMap<String, Value>,
    resolved_state: &ResolverState,
    default_database: &str,
    default_schema: &str,
) -> FsResult<()> {
    set_run_cache_default_relation_target(model, default_database, default_schema);
    let components = RelationComponents {
        database: model
            .deprecated_config
            .database
            .clone()
            .into_inner()
            .unwrap_or(None),
        schema: model
            .deprecated_config
            .schema
            .clone()
            .into_inner()
            .unwrap_or(None),
        alias: model.deprecated_config.alias.clone(),
        store_failures: None,
    };
    let package_name = model.__common_attr__.package_name.clone();
    update_node_relation_components(
        model,
        jinja_env,
        &resolved_state.root_project_name,
        &package_name,
        base_context,
        &components,
        resolved_state.adapter_type,
    )
}

fn update_run_cache_defer_seed(
    seed: &mut DbtSeed,
    jinja_env: &JinjaEnv,
    base_context: &BTreeMap<String, Value>,
    resolved_state: &ResolverState,
    default_database: &str,
    default_schema: &str,
) -> FsResult<()> {
    set_run_cache_default_relation_target(seed, default_database, default_schema);
    let components = RelationComponents {
        database: seed.deprecated_config.database.clone(),
        schema: seed.deprecated_config.schema.clone(),
        alias: seed.deprecated_config.alias.clone(),
        store_failures: None,
    };
    let package_name = seed.__common_attr__.package_name.clone();
    update_node_relation_components(
        seed,
        jinja_env,
        &resolved_state.root_project_name,
        &package_name,
        base_context,
        &components,
        resolved_state.adapter_type,
    )
}

fn update_run_cache_defer_snapshot(
    snapshot: &mut DbtSnapshot,
    jinja_env: &JinjaEnv,
    base_context: &BTreeMap<String, Value>,
    resolved_state: &ResolverState,
    default_database: &str,
    default_schema: &str,
) -> FsResult<()> {
    set_run_cache_default_relation_target(snapshot, default_database, default_schema);
    let components = RelationComponents {
        database: snapshot
            .deprecated_config
            .target_database
            .clone()
            .or_else(|| snapshot.deprecated_config.database.clone()),
        schema: snapshot
            .deprecated_config
            .target_schema
            .clone()
            .or_else(|| snapshot.deprecated_config.schema.clone()),
        alias: snapshot.deprecated_config.alias.clone(),
        store_failures: None,
    };
    let package_name = snapshot.__common_attr__.package_name.clone();
    update_node_relation_components(
        snapshot,
        jinja_env,
        &resolved_state.root_project_name,
        &package_name,
        base_context,
        &components,
        resolved_state.adapter_type,
    )
}

fn update_run_cache_defer_function(
    function: &mut DbtFunction,
    jinja_env: &JinjaEnv,
    base_context: &BTreeMap<String, Value>,
    resolved_state: &ResolverState,
    default_database: &str,
    default_schema: &str,
) -> FsResult<()> {
    set_run_cache_default_relation_target(function, default_database, default_schema);
    let components = RelationComponents {
        database: function
            .deprecated_config
            .database
            .clone()
            .into_inner()
            .unwrap_or(None),
        schema: function
            .deprecated_config
            .schema
            .clone()
            .into_inner()
            .unwrap_or(None),
        alias: function.deprecated_config.alias.clone(),
        store_failures: None,
    };
    let package_name = function.__common_attr__.package_name.clone();
    update_node_relation_components(
        function,
        jinja_env,
        &resolved_state.root_project_name,
        &package_name,
        base_context,
        &components,
        resolved_state.adapter_type,
    )
}

fn set_run_cache_default_relation_target(
    node: &mut dyn InternalDbtNodeAttributes,
    default_database: &str,
    default_schema: &str,
) {
    let base = node.base_mut();
    base.database = default_database.to_string();
    base.schema = default_schema.to_string();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_defer_to_target_overrides_legacy_env_config() {
        let mut config = RunCacheServiceConfig::disabled();
        config.defer_to = "legacy_prod".to_string();

        assert_eq!(
            select_run_cache_defer_to_target(Some("prod"), &config),
            "prod"
        );
    }

    #[test]
    fn legacy_defer_to_remains_fallback() {
        let mut config = RunCacheServiceConfig::disabled();
        config.defer_to = "legacy_prod".to_string();

        assert_eq!(
            select_run_cache_defer_to_target(None, &config),
            "legacy_prod"
        );
    }
}
