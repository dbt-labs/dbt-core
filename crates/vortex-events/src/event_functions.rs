use dashmap::DashMap;
use dbt_common::{
    FsResult,
    stats::{NodeStatus, Stat},
};
use dbt_env::env::InternalEnv;
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_schemas::schemas::{
    InternalDbtNodeAttributes,
    manifest::{DbtManifest, DbtNode},
};
use proto_rust::v1::events::fusion::CloudInvocation;
use proto_rust::v1::public::events::fusion::{
    AdapterInfo, AdapterInfoV2, Invocation, InvocationEnv, PackageInstall, ResourceCounts, RunModel,
};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use uuid::Uuid;

use vortex_client::client::{log_proto, log_proto_and_shutdown};

pub fn invocation_start_event(
    invocation_id: &Uuid,
    root_project_name: &str,
    profile_path: Option<&Path>,
    command: String,
) {
    let env = InternalEnv::global();
    // Some commands don't load dbt_project
    let project_id = if !root_project_name.is_empty() {
        format!("{:x}", md5::compute(root_project_name))
    } else {
        "".to_string()
    };
    // Create Invocation start message
    let message = Invocation {
        //  REQUIRED invocation_id - globally unique identifier
        invocation_id: invocation_id.to_string(),
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        //  progress - start/end
        progress: "start".to_string(),
        //  version - dbt version
        version: env.invocation_config().dbt_version.clone(),
        //  project_id - MD5 hash of the project name
        project_id,
        //  user_id - UUID generated to identify a unique user (~/.dbt/.user.yml)
        user_id: profile_path.map(get_user_id).unwrap_or("".to_string()),
        //  command - full string of the command that was run
        command,
        //  result_type - ok/error.
        //  only provided on invocation_end
        result_type: "".to_string(),
        //  git_commit_sha - SHA of the git commit of the dbt project being run
        git_commit_sha: "".to_string(),
        //  enrichment - toggle enrichment of message by vortex
        enrichment: None,
    };

    let _ = log_proto(message);

    let dbt_inv_env = env.invocation_config().environment.clone();
    // dbt-core: core/dbt/tracking.py::get_dbt_env_context
    let message = InvocationEnv {
        // REQUIRED invocation_id - globally unique identifier
        invocation_id: invocation_id.to_string(),
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        // This is a string that indicates the environment in which the invocation is
        environment: dbt_inv_env.clone(),
        // This field is a toggle to enable enrichment of the message by the Vortex service.
        enrichment: None,
    };

    let _ = log_proto(message);

    if dbt_inv_env != *"manual" {
        cloud_invocation_event(invocation_id);
    }
}

pub fn build_result_string(result: &FsResult<i32>) -> String {
    // result is set to Ok(1) for error and Ok(0) for success
    match result {
        Ok(1) => "error",
        Ok(0) => "ok",
        _ => "unknown",
    }
    .to_string()
}

/// Logs the `Invocation` event and shuts down the Vortex client.
pub fn invocation_end_event(invocation_id: String, result_string: String, shutdown: bool) {
    let env = InternalEnv::global();
    // Create Invocation start message
    let message = Invocation {
        //  REQUIRED invocation_id - globally unique identifier
        invocation_id,
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        //  progress - start/end
        progress: "end".to_string(),
        //  version - dbt version
        version: env.invocation_config().dbt_version.clone(),
        //  project_id - MD5 hash of the project name
        project_id: "".to_string(),
        //  user_id - UUID generated to identify a unique user (~/.dbt/.user.yml)
        user_id: "".to_string(),
        //  command - full string of the command that was run
        command: "".to_string(),
        //  result_type - ok/error.
        //  only provided on invocation_end
        result_type: result_string,
        //  git_commit_sha - SHA of the git commit of the dbt project being run
        git_commit_sha: "".to_string(),
        //  enrichment - toggle enrichment of message by vortex
        enrichment: None,
    };

    if shutdown {
        #[allow(unused_variables)]
        let _ = log_proto_and_shutdown(message).map_err(|e| {
            #[cfg(debug_assertions)]
            {
                use vortex_client::client::ProducerError;
                match e {
                    ProducerError::DevModeError(e) => eprintln!("{e}"),
                    ProducerError::SendError(e) => eprintln!("{e}"),
                    ProducerError::ShutdownError(e) => panic!("{e:?}"),
                }
            }
        });
    } else {
        let _ = log_proto(message);
    }
}

/// In dbt-core, this is in core/dbt/task/run.py::track_model_run
pub fn run_model_event(
    invocation_id: String,
    run_stats: &DashMap<String, Stat>,
    node: &dyn InternalDbtNodeAttributes,
    maybe_incremental_strategy: Option<String>,
    is_contract_enforced: bool,
    has_group: bool,
) {
    let unique_id = node.unique_id();
    if !run_stats.contains_key(&unique_id) {
        // This got called for a seed, which didn't have stats.
        // There might be a better way of doing this...
        return;
    }
    let stat = run_stats.get(&unique_id).unwrap();
    let access = node
        .get_access()
        .as_ref()
        .map_or_else(|| "".to_string(), |a| a.to_string());

    let hashed_contents = node
        .common()
        .raw_code
        .as_ref()
        .map_or_else(|| "".to_string(), |a| format!("{:x}", md5::compute(a)));

    let mut skipped = false;
    let mut skipped_reason = "".to_string();
    match stat.status {
        NodeStatus::Succeeded => {}
        NodeStatus::Errored => {}
        NodeStatus::SkippedUpstreamReused => {
            skipped = true;
            skipped_reason = "upstream_reused".to_string();
        }
        NodeStatus::SkippedUpstreamFailed => {
            skipped = true;
            skipped_reason = "upstream_failed".to_string();
        }
        NodeStatus::ReusedNoChanges => {
            skipped = true;
            skipped_reason = "reused_no_changes".to_string();
        }
        NodeStatus::ReusedStillFresh => {
            skipped = true;
            skipped_reason = "reused_still_fresh".to_string();
        }
        NodeStatus::NoOp => {
            skipped = true;
            skipped_reason = "no-op".to_string();
        }
    }

    let resource_type = node.resource_type().to_string();

    let message = RunModel {
        // REQUIRED invocation_id - globally unique identifier
        invocation_id,
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        // Numerical index of the model being run in this invocation
        index: 0,
        // Total number of models being run in this invocation
        total: 0,
        // the time it took for a model to execute in seconds
        execution_time: stat.get_duration().as_secs_f64(),
        // success or failure status of that model's run
        run_status: stat.result_status_string(),
        // whether or not the model was skipped
        run_skipped: skipped,
        // the materialization strategy used for that model
        model_materialization: node.materialized().to_string(),
        // the incremental strategy used for that model (ex. append, merge, etc.)
        model_incremental_strategy: maybe_incremental_strategy.unwrap_or_default(),
        // unique identifier for the model
        model_id: format!("{:x}", md5::compute(unique_id)),
        // MD5 hash of the model's contents
        hashed_contents,
        // the language used to write the model (ex. sql, python)
        // TODO: if other languages are supported, this will need to change
        language: "sql".to_string(),
        // whether or not the model is in a group
        has_group,
        // whether or not the model is contract enforced
        contract_enforced: is_contract_enforced,
        // the access level of the model (ex. public, private, etc.)
        access,
        // whether or not the model is versioned
        versioned: node.is_versioned(),
        // A reason for why the model was skipped. cost_avoidance/upstream_failed
        run_skipped_reason: skipped_reason,
        // A globally unique ID that is emitted at each instance of an individual
        run_model_id: uuid::Uuid::new_v4().to_string(),
        //  enrichment - toggle enrichment of message by vortex
        enrichment: None,
        // The resource type of the node (model, test, etc.)
        resource_type,
    };

    let _ = log_proto(message);
}

/// core/dbt/task/deps.py::track_package_install
pub fn package_install_event(invocation_id: String, name: String, version: String, source: String) {
    let message = PackageInstall {
        // REQUIRED invocation_id - globally unique identifier
        invocation_id,
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        // plain string name of a package that was installed. This is often the same
        // or similar to the git repository name of that package.
        name,
        // plain string source of the package. This is also referred to as the
        // installation method in our internal analytics. This is based on the syntax
        // used in packages.yml file.
        source,
        // either a semantic version of the package (if installed through the hub) or
        // a git commit hash (if installed through git).
        version,
        // This field is a toggle to enable enrichment of the message by the Vortex service.
        enrichment: None,
    };

    let _ = log_proto(message);
}

/// dbt-core core/dbt/compilation.py::print_compile_stats, track_resource_counts
pub fn resource_counts_event(args: InvocationArgs, manifest: &DbtManifest) {
    let mut model_count = 0;
    let mut seed_count = 0;
    let mut data_test_count = 0;
    let mut snapshot_count = 0;
    let mut operation_count = 0;
    let mut analysis_count = 0;
    for (_, node) in manifest.nodes.iter() {
        match node {
            DbtNode::Model(_) => model_count += 1,
            DbtNode::Seed(_) => seed_count += 1,
            DbtNode::Test(_) => data_test_count += 1,
            DbtNode::Snapshot(_) => snapshot_count += 1,
            DbtNode::Operation(_) => operation_count += 1,
            DbtNode::Analysis(_) => analysis_count += 1,
        }
    }

    let source_count = manifest.sources.len() as i32;
    let macro_count = manifest.macros.len() as i32;
    let group_count = manifest.groups.len() as i32;
    let unit_test_count = manifest.unit_tests.len() as i32;
    let exposure_count = manifest.exposures.len() as i32;

    // to-be-implemented
    let metric_count = 0;
    let semantic_model_count = 0;
    let saved_query_count = 0;

    let message = ResourceCounts {
        // REQUIRED invocation_id - globally unique identifier
        invocation_id: args.invocation_id.to_string(),
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        // total count of models in the project.
        models: model_count,
        // total count of data tests (originally just tests) in the project.
        tests: data_test_count,
        // total count of snapshots in the project.
        snapshots: snapshot_count,
        // total count of analysis queries in the project.
        analyses: analysis_count,
        // total count of macros in the project.
        macros: macro_count,
        // total count of operations in the project.
        operations: operation_count,
        // total count of seeds in the project.
        seeds: seed_count,
        // total count of sources in the project.
        sources: source_count,
        // total count of exposures in the project.
        exposures: exposure_count,
        // total count of metrics in the project.
        metrics: metric_count,
        // total count of groups in the project.
        groups: group_count,
        // total count of unit tests in the project.
        unit_tests: unit_test_count,
        // total count of semantic models in the project.
        semantic_models: semantic_model_count,
        // total count of saved queries in the project.
        saved_queries: saved_query_count,
        // This field is a toggle to enable enrichment of the message by the Vortex service.
        enrichment: None,
    };

    let _ = log_proto(message);
}

pub fn cloud_invocation_event(invocation_id: &Uuid) {
    let env = InternalEnv::global();
    let message = CloudInvocation {
        // REQUIRED invocation_id - globally unique identifier
        invocation_id: invocation_id.to_string(),
        // REQUIRED Globally unique account identifier in which the invocation is run.
        // Comes from the DBT_CLOUD_ACCOUNT_IDENTIFIER environment variable.
        // e.g. act_0g9JY6ZTSUNAQPG6WvLNYYdmYHW
        dbt_cloud_account_identifier: env.invocation_config().account_identifier.clone(),
        // REQUIRED Comes from the DBT_CLOUD_PROJECT_ID environment variable. e.g. 672
        dbt_cloud_project_id: env.invocation_config().project_id.clone(),
        // REQUIRED Unique identifier of the environment within the tenant.
        // Comes from the DBT_CLOUD_ENVIRONMENT_ID environment variable. e.g. 2
        dbt_cloud_environment_id: env.invocation_config().environment_id.clone(),
        // Unique identifier of the job within the account identifier.
        // Comes from the DBT_CLOUD_JOB_ID environment variable.
        // e.g. 0
        dbt_cloud_job_id: env.invocation_config().job_id.clone(),
        // This field is a toggle to enable enrichment of the message by the Vortex service.
        enrichment: None,
    };

    let _ = log_proto(message);
}

pub fn adapter_info_event(invocation_id: String, adapter_type: String, adapter_unique_id: String) {
    let message = AdapterInfo {
        // REQUIRED invocation_id - globally unique identifier
        invocation_id,
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        // adapter_type is the plain string name for the dbt adapter that's used by the
        // project. Examples could include: "bigquery", "snowflake", "redshift", "postgres", etc.
        adapter_type,
        // adapter_unique_id is the unique identifier of a project's warehouse credentials.
        // For supported warehouses, we create an MD5 hash of the connection string.
        // The specific string varies per adapter_type.
        adapter_unique_id,
        // This field is a toggle to enable enrichment of the message by the Vortex service.
        enrichment: None,
    };

    let _ = log_proto(message);
}

/// Not yet implemented, all stubbed
pub fn adapter_info_v2_event() {
    let message = AdapterInfoV2 {
        // REQUIRED  event_id - unique identifier for this event (uuid)
        event_id: uuid::Uuid::new_v4().to_string(),
        // A foreign key to the RunModel message that was emitted at each instance
        // of an individual model being run.
        run_model_id: "".to_string(),
        // This reflects the adapter name used when they ran a given model.
        adapter_name: "".to_string(),
        // This reflects the simplified semantic version of an adapter that was used
        // when they ran a given model. ex. 1.9.0
        base_adapter_version: "".to_string(),
        // This reflects the full adapter version used when they ran a given model.
        adapter_version: "".to_string(),
        // This is a flexible key-value pair that can be used to store any additional
        // model adapter information. Today this is used to store two pieces of
        // information: the model_adapter_type (the adapter_name that was used for
        // that specific model) and the model_adapter_table_format (Iceberg or
        // something else).
        model_adapter_details: HashMap::new(),
        // This field is a toggle to enable enrichment of the message by the Vortex service.
        enrichment: None,
    };

    let _ = log_proto(message);
}

/// This looks for or creates a .user.yml file in the same directory
/// as the profiles.yml file, which stores a uuid user_id.
pub fn get_user_id(profile_path: &Path) -> String {
    let profiles_dir = profile_path.parent();
    if let Some(profiles_dir) = profiles_dir {
        let cookie_path = profiles_dir.join(".user.yml");
        if cookie_path.exists() {
            match fs::read_to_string(&cookie_path) {
                Ok(contents) => match dbt_serde_yaml::from_str::<String>(&contents) {
                    Ok(user_id) => user_id,
                    Err(_) => set_user_cookie(profiles_dir, &cookie_path),
                },
                Err(_) => set_user_cookie(profiles_dir, &cookie_path),
            }
        } else {
            set_user_cookie(profiles_dir, &cookie_path)
        }
    } else {
        "".to_string()
    }
}

fn set_user_cookie(profiles_dir: &Path, cookie_path: &Path) -> String {
    let user_id = uuid::Uuid::new_v4().to_string();
    let profiles_file = profiles_dir.join("profiles.yml");
    // Check if profiles_dir is empty (current directory) or exists
    if (profiles_dir.as_os_str().is_empty() || profiles_dir.exists()) && profiles_file.exists() {
        if let Ok(yaml) = dbt_serde_yaml::to_string(&user_id) {
            let _ = fs::write(cookie_path, yaml);
        }
    }
    // even if we weren't able to store the user_id in the .user.yml file, return it anyway
    user_id
}
