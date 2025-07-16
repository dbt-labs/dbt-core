use dbt_common::{FsResult, current_function_name};
use dbt_test_utils::task::{
    AssertDirExistsTask, AssertFileContainsTask, AssertFileExistsTask, ProjectEnv, TaskSeq,
};

#[tokio::test]
async fn tasks_file_contains() -> FsResult<()> {
    let root = env!("CARGO_MANIFEST_DIR");
    let env = ProjectEnv::immutable_from(root, "tests/data/hello")?;

    TaskSeq::new(current_function_name!())
        .task(Box::new(AssertFileContainsTask::new(
            "profiles.yml",
            "datafusion",
        )))
        .execute_in(&env)
        .await?;

    Ok(())
}

#[tokio::test]
async fn tasks_file_exists() -> FsResult<()> {
    let root = env!("CARGO_MANIFEST_DIR");
    let env = ProjectEnv::immutable_from(root, "tests/data/hello")?;

    TaskSeq::new(current_function_name!())
        .task(Box::new(AssertFileExistsTask::new("profiles.yml")))
        .execute_in(&env)
        .await?;

    Ok(())
}

#[tokio::test]
async fn tasks_dir_exists() -> FsResult<()> {
    let root = env!("CARGO_MANIFEST_DIR");
    let env = ProjectEnv::immutable_from(root, "tests/data/hello")?;

    TaskSeq::new(current_function_name!())
        .task(Box::new(AssertDirExistsTask::new("models")))
        .execute_in(&env)
        .await?;

    Ok(())
}
