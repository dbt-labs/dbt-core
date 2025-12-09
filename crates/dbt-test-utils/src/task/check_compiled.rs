use crate::task::{ProjectEnv, Task, TestEnv, TestResult, utils::iter_files_recursively};
use async_trait::async_trait;
use dbt_common::{constants::DBT_COMPILED_DIR_NAME, stdfs};
use dbt_test_primitives::is_update_golden_files_mode;

pub struct CheckCompiledFiles {}

#[async_trait]
impl Task for CheckCompiledFiles {
    async fn run(&self, _: &ProjectEnv, test_env: &TestEnv, _task_index: usize) -> TestResult<()> {
        iter_files_recursively(
            test_env
                .temp_dir
                .join("target")
                .join(DBT_COMPILED_DIR_NAME)
                .join("models")
                .as_path(),
            &|abs_path| {
                if abs_path
                    .as_os_str()
                    .to_str()
                    .unwrap_or_default()
                    .ends_with(".sql")
                {
                    let actual = stdfs::read_to_string(abs_path)?;
                    let path =
                        stdfs::diff_paths(abs_path, test_env.temp_dir.join("target")).unwrap();
                    let golden_path = test_env.golden_dir.join(path);
                    if is_update_golden_files_mode() {
                        if let Some(parent_dir) = golden_path.parent() {
                            stdfs::create_dir_all(parent_dir)?;
                        }
                        stdfs::write(golden_path, actual).unwrap();
                    } else {
                        let expected = stdfs::read_to_string(&golden_path).unwrap();
                        assert_eq!(actual, expected);
                    }
                }

                Ok(())
            },
        )
        .await?;

        Ok(())
    }
}
