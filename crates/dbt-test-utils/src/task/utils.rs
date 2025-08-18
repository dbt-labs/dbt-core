use super::TestResult;
use clap::Parser;
use dbt_common::{
    cancellation::{CancellationToken, never_cancels},
    logging::dbt_compat_log::LogEntry,
};
use std::{
    fs::File,
    future::Future,
    io::Read,
    path::{Component, Path, PathBuf},
};

use once_cell::sync::Lazy;
use regex::Regex;

use dbt_common::{
    FsResult,
    io_args::SystemArgs,
    stdfs::{self},
    tokiofs, unexpected_err,
};

// Pre-compiled regex patterns for optimal performance
static SCHEMA_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)fusion_tests_schema__[a-zA-Z0-9_]*").unwrap());
static ISO_TIMESTAMP_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z").unwrap());
static TIME_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b\d{2}:\d{2}:\d{2}\b").unwrap());
static BRACKETED_DURATION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[\s*\d+(?:\.\d+)?s\s*\]").unwrap());
static IN_DURATION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Matches: "in 1s", "in 500ms", "in 1s 298ms", "in 2m 10s", etc.
    Regex::new(r"\bin\s+\d+(?:\.\d+)?(?:ns|us|μs|µs|ms|s|m|h)(?:\s+\d+(?:\.\d+)?(?:ns|us|μs|µs|ms|s|m|h))*\b")
        .unwrap()
});
static MULTI_UNIT_DURATION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Matches sequences of 2+ duration tokens (e.g., "32ms 101us", "4s 703ms 195us 939ns")
    Regex::new(r"\b\d+(?:\.\d+)?(?:ns|us|μs|µs|ms|s|m|h)(?:\s+\d+(?:\.\d+)?(?:ns|us|μs|µs|ms|s|m|h)){1,}\b").unwrap()
});
static AGE_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bage:\s*\d+").unwrap());

/// Copies a directory and its contents, excluding .gitignored files.
pub fn copy_dir_non_ignored(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> FsResult<()> {
    stdfs::create_dir_all(&dst)?;
    for entry in ignore::WalkBuilder::new(src.as_ref())
        .hidden(false)
        .follow_links(true)
        .git_global(false)
        .ignore(false)
        .build()
    {
        let Ok(entry) = entry else {
            return unexpected_err!(
                "Failed to read entry in directory: {}",
                src.as_ref().display()
            );
        };

        let relative_path = entry
            .path()
            .strip_prefix(src.as_ref())
            .expect("entry path should be relative to source");
        let target_path = dst.as_ref().join(relative_path);

        let is_dir = entry.file_type().is_some_and(|ft| ft.is_dir());
        if is_dir {
            stdfs::create_dir_all(&target_path)?;
        } else {
            stdfs::copy(entry.path(), &target_path)?;
        }
    }
    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn redirect_buffer_to_stdin(buffer_content: &str) -> TestResult<File> {
    use std::io::{Seek as _, Write};
    use std::os::fd::AsRawFd as _;

    // Create a buffer with the desired content.
    let mut temp_file = tempfile::tempfile()?;
    temp_file.write_all(buffer_content.as_bytes())?;
    temp_file.seek(std::io::SeekFrom::Start(0))?;

    unsafe {
        // Close the original stdin.
        libc::close(0);

        // Duplicate the buffer's file descriptor to stdin (0).
        libc::dup2(temp_file.as_raw_fd(), 0);
    }

    Ok(temp_file)
}

#[cfg(target_os = "windows")]
pub fn redirect_buffer_to_stdin(buffer_content: &str) -> TestResult<File> {
    use std::io::{Seek, Write};
    use std::os::windows::io::AsRawHandle;

    use winapi::um::processenv::SetStdHandle;
    use winapi::um::winbase::STD_INPUT_HANDLE;

    // Create a temporary file and write the buffer content to it
    let mut temp_file = tempfile::tempfile()?;
    temp_file.write_all(buffer_content.as_bytes())?;
    temp_file.seek(std::io::SeekFrom::Start(0))?;

    // Get the raw handle of the temporary file
    let raw_handle = temp_file.as_raw_handle();

    // Redirect the standard input to the temporary file
    let success = unsafe { SetStdHandle(STD_INPUT_HANDLE, raw_handle as _) };
    if success == 0 {
        return Err("Failed to redirect stdin".into());
    }

    // Return the temporary file to keep it open for the duration of the redirection
    Ok(temp_file)
}

/// Iterates over file paths in the directory and subdirectories,
/// invoking a handler for each file.
pub async fn iter_files_recursively<'a, F>(root: &'a Path, handler: &'a mut F) -> TestResult<()>
where
    F: FnMut(&Path) -> TestResult<()> + Send,
{
    let mut read_dir = tokiofs::read_dir(root).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            // Recursively handle files in subdirectories.
            Box::pin(iter_files_recursively(&path, handler)).await?;
        } else {
            // Invoke the handler for each file.
            handler(&path)?;
        }
    }

    Ok(())
}

pub fn check_set_user_env_var() {
    if std::env::var("USER").is_err() {
        // set_var is generally disallowed but intentional here
        let run_id = std::env::var("GITHUB_RUN_ID").unwrap_or_else(|_| "0000000".to_string());
        let run_number = std::env::var("GITHUB_RUN_NUMBER").unwrap_or_else(|_| "0".to_string());
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("USER", format!("{run_id}x{run_number}"));
        }
    }
}

/// Collapse mktemp-style segments like /.tmpabcdef -> /.tmpXXXXXX
pub fn maybe_normalize_tmp_paths(output: String) -> String {
    let re = Regex::new(r"/\.tmp[0-9A-Za-z_-]+").unwrap();
    re.replace_all(&output, "/.tmpXXXXXX").to_string()
}

/// On Windows, this normalizes forward/backward slashes to '|' so as to ignore
/// the difference in path separators.
///
/// On other platforms, this is a no-op.
pub fn maybe_normalize_slashes(output: String) -> String {
    #[cfg(windows)]
    {
        output.replace("\\", "|").replace("/", "|")
    }
    #[cfg(not(windows))]
    {
        output
    }
}

pub fn maybe_normalize_schema_name(output: String) -> String {
    // Use pre-compiled regex to replace schema patterns like "fusion_tests_schema__alex"
    // with "fusion_tests_schema__replaced" without breaking duration patterns like "44.65s"
    SCHEMA_PATTERN
        .replace_all(&output, "fusion_tests_schema__replaced")
        .to_string()
}

pub fn maybe_normalize_time(output: String) -> String {
    let mut result = output;

    // Replace ISO 8601 timestamps like "2025-05-27T22:38:47.667Z" and "2017-09-01T00:00:00Z"
    result = ISO_TIMESTAMP_PATTERN
        .replace_all(&result, "YYYY-MM-DDTHH:MM:SS.sssZ")
        .to_string();

    // Replace time formats like "15:39:21"
    result = TIME_PATTERN.replace_all(&result, "HH:MM:SS").to_string();

    // Replace bracketed duration formats like "[ 44.65s]" with "[000.00s]"
    result = BRACKETED_DURATION_PATTERN
        .replace_all(&result, "[000.00s]")
        .to_string();

    // Replace trailing "in ..." duration phrases with a stable token
    result = IN_DURATION_PATTERN
        .replace_all(&result, "in duration")
        .to_string();

    // Replace multi-unit duration sequences like "32ms 101us 694ns" with a stable token
    result = MULTI_UNIT_DURATION_PATTERN
        .replace_all(&result, "duration")
        .to_string();

    // Replace age patterns like "age: 244165330" with normalized value
    result = AGE_PATTERN
        .replace_all(&result, "age: NORMALIZED")
        .to_string();

    result
}

pub fn normalize_version(output: String) -> String {
    output.replace(
        format!("dbt-fusion {}", env!("CARGO_PKG_VERSION")).as_str(),
        "dbt-fusion ",
    )
}

/// Strips the full test name of the crate name and returns the test name.
pub fn strip_full_test_name(full_test_name: &str) -> String {
    full_test_name
        .split("::")
        .last()
        .expect("Fully qualified test name should contain `::`")
        .to_string()
}

/// Strips the leading relative path from a path.
pub fn strip_leading_relative(path: &Path) -> &Path {
    let mut components = path.components();

    // Skip over any leading CurDir (`./`) or ParentDir (`../`)
    while let Some(c) = components.clone().next() {
        match c {
            Component::CurDir | Component::ParentDir => {
                components.next(); // discard
            }
            _ => break,
        }
    }

    components.as_path()
}

// Util function to execute fusion commands in tests
pub async fn exec_fs<Fut, P: Parser>(
    cmd_vec: Vec<String>,
    project_dir: PathBuf,
    stdout_file: File,
    stderr_file: File,
    execute_fs: impl FnOnce(SystemArgs, P, CancellationToken) -> Fut,
    from_lib: impl FnOnce(&P) -> SystemArgs,
) -> FsResult<i32>
where
    Fut: Future<Output = FsResult<i32>>,
{
    let token = never_cancels();
    // Check if project_dir has a .env.conformance file
    // NOTE: this has to be done before we parse Cli
    let conformance_file = project_dir.join(".env.conformance");
    if conformance_file.exists() {
        // if so, load it
        dotenv::from_path(conformance_file).unwrap();
    }

    // Redirect stdout and stderr to the specified files until the end of this
    // scope, at which point the original stdout and stderr will be restored and
    // the files will be closed.
    let _stdout = with_redirected_stdout(stdout_file);
    let _stderr = with_redirected_stderr(stderr_file);

    let cli = P::parse_from(cmd_vec);
    let arg = from_lib(&cli);

    execute_fs(arg, cli, token).await
}

/// The purpose of this guard is two fold:
/// 1. it holds the file handle open for the duration of the redirection
/// 2. it restores the original stdout/stderr file descriptors when dropped
///
/// Restoring the original file descriptors is necessary to allow printing to
/// terminal code (e.g. `dbg!/println!`) to still function in test cases -- if
/// we don't restore here, then terminal output will be disabled after the first
/// time `exec_fs` gets called, which would be surprising for the test author.
struct FdRedirectionGuard {
    file: File,
    #[cfg(not(target_os = "windows"))]
    target_fd: std::os::unix::io::RawFd,
    #[cfg(not(target_os = "windows"))]
    original_fd: std::os::unix::io::RawFd,
    #[cfg(target_os = "windows")]
    target_fd: usize, // Windows uses HANDLE, but we store it as usize for simplicity
    #[cfg(target_os = "windows")]
    original_fd: usize, // Windows uses HANDLE, but we store it as usize for simplicity
}

impl Drop for FdRedirectionGuard {
    fn drop(&mut self) {
        #[cfg(not(target_os = "windows"))]
        unsafe {
            // Restore the original stdout
            libc::dup2(self.original_fd, self.target_fd);
        }
        #[cfg(target_os = "windows")]
        unsafe {
            use winapi::um::processenv::SetStdHandle;

            // Restore the original stdout
            SetStdHandle(self.target_fd as _, self.original_fd as _);
        }

        // In the testing framework, the self.file is created from a temp file,
        // so we need to read the content and print it to ensure that the errors are not sunken
        let is_stderr = {
            #[cfg(not(target_os = "windows"))]
            {
                self.target_fd == libc::STDERR_FILENO
            }
            #[cfg(target_os = "windows")]
            {
                use winapi::um::winbase::STD_ERROR_HANDLE;
                self.target_fd == STD_ERROR_HANDLE as usize
            }
        };

        if is_stderr {
            let mut content = String::new();
            use std::io::Seek;
            let _ = self.file.seek(std::io::SeekFrom::Start(0));
            let _ = self.file.read_to_string(&mut content);
            eprintln!("{content}");
        }
        // self._file will be closed automatically after this point
    }
}

#[cfg(target_os = "windows")]
/// Redirects stdout to `file`. Returns a scope guard that restores the original
/// stdout on drop.
fn with_redirected_stdout(file: File) -> FdRedirectionGuard {
    use std::os::windows::io::AsRawHandle as _;
    use winapi::um::processenv::SetStdHandle;
    use winapi::um::winbase::STD_OUTPUT_HANDLE;

    let original_fd = unsafe { winapi::um::processenv::GetStdHandle(STD_OUTPUT_HANDLE) as usize };

    let raw_handle = file.as_raw_handle();

    let success = unsafe { SetStdHandle(STD_OUTPUT_HANDLE, raw_handle as _) };
    if success == 0 {
        panic!("Failed to redirect stdout");
    }

    FdRedirectionGuard {
        file,
        target_fd: STD_OUTPUT_HANDLE as usize,
        original_fd,
    }
}

#[cfg(target_os = "windows")]
/// Redirects stderr to `file`. Returns a scope guard that restores the original
/// stderr on drop.
fn with_redirected_stderr(file: File) -> FdRedirectionGuard {
    use std::os::windows::io::AsRawHandle as _;
    use winapi::um::processenv::SetStdHandle;
    use winapi::um::winbase::STD_ERROR_HANDLE;

    let original_fd = unsafe { winapi::um::processenv::GetStdHandle(STD_ERROR_HANDLE) as usize };

    let raw_handle = file.as_raw_handle();

    let success = unsafe { SetStdHandle(STD_ERROR_HANDLE, raw_handle as _) };
    if success == 0 {
        panic!("Failed to redirect stderr");
    }

    FdRedirectionGuard {
        file,
        target_fd: STD_ERROR_HANDLE as usize,
        original_fd,
    }
}

#[cfg(not(target_os = "windows"))]
/// Redirects stdout to `file`. Returns a scope guard that restores the original
/// stdout on drop.
fn with_redirected_stdout(file: File) -> FdRedirectionGuard {
    use std::os::fd::AsRawFd as _;

    let original_fd = unsafe { libc::dup(libc::STDOUT_FILENO) };

    unsafe {
        // Redirect stdout to the file
        libc::dup2(file.as_raw_fd(), libc::STDOUT_FILENO);
    }

    FdRedirectionGuard {
        file,
        target_fd: libc::STDOUT_FILENO,
        original_fd,
    }
}

#[cfg(not(target_os = "windows"))]
/// Redirects stderr to `file`. Returns a scope guard that restores the original
/// stderr on drop.
fn with_redirected_stderr(file: File) -> FdRedirectionGuard {
    use std::os::fd::AsRawFd as _;

    let original_fd = unsafe { libc::dup(libc::STDERR_FILENO) };

    unsafe {
        // Redirect stderr to the file
        libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO);
    }

    FdRedirectionGuard {
        file,
        target_fd: libc::STDERR_FILENO,
        original_fd,
    }
}

/// The name of the git directory
pub const GIT_DIR: &str = ".git";

/// Given an absolute path, returns the relative path to the git root directory if it exists.
pub fn relative_to_git_root(path: &Path) -> Option<PathBuf> {
    let mut current = path;
    while let Some(parent) = current.parent() {
        if parent.join(GIT_DIR).is_dir() {
            return path.strip_prefix(parent).ok().map(|p| p.to_path_buf());
        }
        current = parent;
    }
    None
}

pub fn assert_str_in_log_messages(logs: &[LogEntry], search_str: &str) -> FsResult<()> {
    if logs.iter().any(|log| {
        log.info
            .as_ref()
            .map(|info| info.msg.as_str())
            .unwrap_or("")
            .contains(search_str)
    }) {
        Ok(())
    } else {
        panic!("Log message containing '{search_str}' not found");
    }
}
