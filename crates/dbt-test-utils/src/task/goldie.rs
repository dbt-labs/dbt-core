use crate::task::utils::relative_to_git_root;

use super::{
    task_seq::CommandFn,
    utils::{
        maybe_normalize_schema_name, maybe_normalize_slashes, maybe_normalize_time,
        normalize_version,
    },
    ProjectEnv, TestEnv,
};
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use dbt_test_primitives::is_update_golden_files_mode;

use dbt_common::{
    err,
    stdfs::{self},
    ErrorCode, FsResult,
};

// Snowflake prompt for our REPL
static SNOWFLAKE_PROMPT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d+\(snowflake\[local\].*").unwrap());

fn postprocess_actual(content: String, sort_output: bool) -> String {
    let res = [
        filter_lines,
        maybe_normalize_schema_name,
        maybe_normalize_time,
        normalize_version,
    ]
    .iter()
    .fold(content, |acc, transform| transform(acc));

    if sort_output {
        sort_lines(res)
    } else {
        res
    }
}

fn postprocess_golden(content: String, sort_output: bool) -> String {
    let res = [
        maybe_normalize_slashes,
        maybe_normalize_schema_name,
        normalize_version,
    ]
    .iter()
    .fold(content, |acc, transform| transform(acc));

    if sort_output {
        sort_lines(res)
    } else {
        res
    }
}

fn assert_output(channel: &str, actual: String, goldie_path: &Path, sort_output: bool) {
    let goldie_exists = goldie_path.exists();
    let golden = if goldie_exists {
        stdfs::read_to_string(goldie_path).unwrap_or_else(|_| {
            panic!(
                "cannot read golden {} from {}",
                channel,
                goldie_path.display()
            )
        })
    } else {
        "".to_string()
    };
    let golden = postprocess_golden(golden, sort_output);
    let actual = maybe_normalize_slashes(actual);

    if goldie_exists && golden == actual {
        return;
    }

    let relative_golden_path =
        relative_to_git_root(goldie_path).unwrap_or_else(|| goldie_path.to_path_buf());
    let original_filename = if !goldie_exists {
        "/dev/null".to_string()
    } else {
        PathBuf::from("i")
            .join(&relative_golden_path)
            .to_string_lossy()
            .to_string()
    };
    let modified_filename = PathBuf::from("w")
        .join(&relative_golden_path)
        .to_string_lossy()
        .to_string();

    let patch = diffy::DiffOptions::new()
        .set_original_filename(original_filename)
        .set_modified_filename(modified_filename)
        .create_patch(&golden, &actual);

    eprintln!("{patch}");
    panic!(
        "Output of {channel} does not match golden file. See diff above. \
        To accept this output as golden file, open a terminal in the root of the git repository and run: \
          `git apply -` \
        then copy-paste the diff above into the terminal and press Ctrl+D.\
        (Note: if you're copy-pasting from the Github web UI, run `sed 's/^    //' | git apply -` instead) \
        ",
    )
}

pub struct CompareEnv {
    pub project_dir: PathBuf,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
    pub goldie_stdout_path: PathBuf,
    pub goldie_stderr_path: PathBuf,
}

pub fn create_compare_env(
    name: &str,
    project_env: &ProjectEnv,
    test_env: &TestEnv,
    task_index: usize,
) -> CompareEnv {
    // inputs are read from here
    let project_dir = &project_env.absolute_project_dir;
    // golden files are read from here
    let golden_dir = &test_env.golden_dir;

    // Prepare stdout and stderr
    let task_suffix = if task_index > 0 {
        format!("_{task_index}")
    } else {
        "".to_string()
    };

    let stdout_path = test_env
        .temp_dir
        .join(format!("{name}{task_suffix}.stdout"));
    let stderr_path = test_env
        .temp_dir
        .join(format!("{name}{task_suffix}.stderr"));
    let goldie_stdout_path = golden_dir.join(format!("{name}{task_suffix}.stdout"));
    let goldie_stderr_path = golden_dir.join(format!("{name}{task_suffix}.stderr"));

    CompareEnv {
        project_dir: project_dir.clone(),
        stdout_path,
        stderr_path,
        goldie_stdout_path,
        goldie_stderr_path,
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_and_compare(
    // name of the task used to create file names (this is usually test name)
    name: &str,
    // command to execute as a vector
    cmd_vec: &[String],
    project_env: &ProjectEnv,
    test_env: &TestEnv,
    task_index: usize,
    // the actual function that will execute the given command after
    // necessary/common preparation
    sort_output: bool,
    exe: Arc<CommandFn>,
) -> FsResult<()> {
    let compare_env = create_compare_env(name, project_env, test_env, task_index);

    let stdout_file = stdfs::File::create(&compare_env.stdout_path)?;
    let stderr_file = stdfs::File::create_with_read_write(&compare_env.stderr_path)?;

    let _res = exe(
        cmd_vec.to_vec(),
        compare_env.project_dir,
        stdout_file,
        stderr_file,
    )
    .await;
    let is_update = is_update_golden_files_mode();
    compare_or_update(
        is_update,
        sort_output,
        compare_env.stderr_path,
        compare_env.goldie_stderr_path,
        compare_env.stdout_path,
        compare_env.goldie_stdout_path,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn compare_or_update(
    is_update: bool,
    sort_output: bool,
    stderr_path: PathBuf,
    goldie_stderr_path: PathBuf,
    stdout_path: PathBuf,
    goldie_stdout_path: PathBuf,
) -> FsResult<()> {
    // Check that stdout and stderr exist
    if !stdout_path.exists() {
        return err!(
            ErrorCode::IoError,
            "stdout file does not exist: {}",
            stdout_path.display()
        );
    }
    if !stderr_path.exists() {
        return err!(
            ErrorCode::IoError,
            "stderr file does not exist: {}",
            stderr_path.display()
        );
    }

    let stdout_content = stdfs::read_to_string(&stdout_path)?;
    let stdout_content = postprocess_actual(stdout_content, sort_output);
    let stderr_content = stdfs::read_to_string(&stderr_path)?;
    let stderr_content = postprocess_actual(stderr_content, sort_output);

    if is_update {
        // Copy stdout and stderr to goldie_stdout and goldie_stderr Note: we
        // can't use move here because the source and target files may not be on
        // the same filesystem
        stdfs::write(&goldie_stdout_path, stdout_content)?;
        stdfs::write(&goldie_stderr_path, stderr_content)?;
    } else {
        // Compare the generated files to the golden files
        assert_output("stderr", stderr_content, &goldie_stderr_path, sort_output);
        assert_output("stdout", stdout_content, &goldie_stdout_path, sort_output);
    }
    Ok(())
}

fn sort_lines(content: String) -> String {
    content.lines().sorted().collect::<Vec<_>>().join("\n")
}

fn filter_lines_internal(content: String, in_emacs: bool) -> String {
    const KNOWN_NOISE: &[&str] = &[" has been running for over", "last updated"];

    let mut res = content
        .lines()
        .filter_map(|line| {
            if KNOWN_NOISE.iter().any(|noise| line.contains(noise)) {
                // Remove known noise lines entirely
                None
            } else if in_emacs && SNOWFLAKE_PROMPT.is_match(line) {
                // In Emacs we need to filter our REPL prompt.
                Some("")
            } else {
                // Keep other lines unchanged
                Some(line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    if content.ends_with('\n') {
        res.push('\n');
    }
    res
}

fn filter_lines(content: String) -> String {
    filter_lines_internal(content, env::var("INSIDE_EMACS").is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_lines() {
        let lines = filter_lines("abc \n has been running for over \n 123".to_string());
        assert_eq!("abc \n 123", lines);
    }

    #[test]
    fn test_filter_repl_prompt() {
        let lines = filter_lines_internal("abc \n0(snowflake[local])> \n 123".to_string(), true);
        assert_eq!("abc \n\n 123", lines);
    }
}
