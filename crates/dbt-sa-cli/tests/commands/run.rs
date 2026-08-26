use dbt_common::{FsResult, current_function_name};
use dbt_test_utils::random_schema;
use dbt_test_utils::task::{HydrateProfilesTask, ProjectEnv, TaskSeq};

use crate::common::TaskSeqExt;

#[tokio::test(flavor = "multi_thread")]
async fn run_duckdb_seed_and_model() -> FsResult<()> {
    let env = ProjectEnv::immutable_sa("tests/data/duckdb_seed_and_model")?;
    TaskSeq::new(current_function_name!())
        .fs_sa("seed")
        .fs_sa("run --select m1")
        .fs_sa("run --select m2")
        .fs_sa("show --select m2")
        .execute_in(&env)
        .await?;
    Ok(())
}

// Live warehouse tests.
//
// These connect to a real warehouse (no recording) using the shared
// `fusion_tests` profile that `xtask` hydrates into `~/.dbt/profiles.yml` from
// CI credentials. They are gated by the `live_warehouse` name: excluded from the
// default test filter and run by the Linux `cargo xtask ci` job where
// credentials are present (or locally via `cargo xtask test --all <name>`). Each
// run uses a randomized schema for isolation; the schema is normalized back
// before golden comparison.

async fn live_warehouse_seed_and_model(target: &str, name: &str) -> FsResult<()> {
    let env = ProjectEnv::immutable_sa("tests/data/live_seed_and_model")?;
    let schema = random_schema("fusion_tests_schema");

    let mut seq = TaskSeq::new(name);
    seq.task(Box::new(HydrateProfilesTask {
        schema,
        target: target.to_string(),
    }));
    seq.fs_sa(format!("seed --target {target}"))
        .fs_sa(format!("run --select m1 --target {target}"))
        .fs_sa(format!("run --select m2 --target {target}"))
        .fs_sa(format!("show --select m2 --target {target}"));
    seq.execute_in(&env).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn live_warehouse_snowflake_seed_and_model() -> FsResult<()> {
    live_warehouse_seed_and_model("snowflake", current_function_name!()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn live_warehouse_redshift_seed_and_model() -> FsResult<()> {
    live_warehouse_seed_and_model("redshift", current_function_name!()).await
}
