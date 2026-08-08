use dbt_common::{FsResult, current_function_name};
use dbt_test_utils::task::{ProjectEnv, TaskSeq};

use crate::common::TaskSeqExt;

// `compile` only runs dialect-specific static analysis / SQL rendering; it does
// not open a warehouse connection, so these run locally with no recording.

#[tokio::test]
async fn compile_snowflake() -> FsResult<()> {
    let env = ProjectEnv::immutable_sa("tests/data/snowflake_compile")?;
    TaskSeq::new(current_function_name!())
        .fs_sa("compile")
        .execute_in(&env)
        .await?;
    Ok(())
}

#[tokio::test]
async fn compile_redshift() -> FsResult<()> {
    let env = ProjectEnv::immutable_sa("tests/data/redshift_compile")?;
    TaskSeq::new(current_function_name!())
        .fs_sa("compile")
        .execute_in(&env)
        .await?;
    Ok(())
}
