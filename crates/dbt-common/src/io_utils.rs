use crate::stdfs::File;
use crate::{err, fs_err, stdfs::canonicalize, ErrorCode, FsError, FsResult};
use pathdiff::diff_paths;
use regex::Regex;
use std::{
    any::Any,
    env,
    ffi::OsStr,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

/// A trait for reporting status messages and errors that occur during execution.
/// This is primarily used in LSP mode to report errors and progress to the client.
pub trait StatusReporter: Any + Send + Sync {
    /// Called when an error occurs to collect it for later processing
    fn collect_error(&self, error: &FsError);
    fn collect_warning(&self, warning: &FsError);
    /// Called to show progress in the UI
    fn show_progress(&self, action: &str, target: &str, description: Option<&str>);
}

fn find_token_positions_in_file(file: &Path, token: &str) -> FsResult<Vec<(usize, usize, usize)>> {
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    let mut positions = Vec::new();
    let mut pos = 0;
    for (line_number, line) in reader.lines().enumerate() {
        let line = line?;
        let column_number = 0;
        let remaining_line = &line[..];

        if let Some(index) = remaining_line.find(token) {
            let start_column = column_number + index;
            // let end_column = start_column + token.len();
            positions.push((line_number + 1, start_column + 1, pos + index));
            // remaining_line = &remaining_line[(index + token.len())..];
            // column_number += index + token.len();
        }
        pos += line.len() + 1; // TODO: 1 means the newline character, maybe \r\n
    }

    Ok(positions)
}

pub fn find_locations(token: &str, file: &Path) -> FsResult<Option<(usize, usize, usize)>> {
    let output = None;
    // the token might be a dotted name, first try to find the full name, then the last part
    let tokens = token.split('.').collect::<Vec<&str>>();
    let full_and_last = if tokens.len() > 1 {
        vec![
            token.to_owned(),
            tokens.last().unwrap().to_owned().to_string(),
        ]
    } else {
        vec![token.to_owned()]
    };
    for token in full_and_last {
        let positions = find_token_positions_in_file(file, &token)?;
        if !positions.is_empty() {
            if let Some((line, column, index)) = positions.into_iter().next() {
                return Ok(Some((line, column, index)));
            }
        }
    }
    Ok(output)
}

pub fn find_enclosed_substring(msg: &str, re: &Regex) -> Option<String> {
    if let Some(captures) = re.captures(msg) {
        if let Some(substring) = captures.get(1) {
            return Some(substring.as_str().to_string());
        }
    }
    None
}

/// Reads the contents of a file as a string.
pub fn try_read_yml_to_str(path: &Path) -> FsResult<String> {
    let mut file = File::open(path).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Cannot open file {}: {}",
            path.display(),
            e
        )
    })?;
    let mut data = String::new();
    file.read_to_string(&mut data).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Cannot read file {}: {}",
            path.display(),
            e
        )
    })?;
    Ok(data)
}

pub fn determine_project_dir(inputs: &[String], project_file: &str) -> FsResult<PathBuf> {
    // start the search at
    // - the current directory (the default) or
    // - the directory of the first sql file
    // - the directory of the first workspace.sdf file
    let mut search_start = env::current_dir()?;

    if let Some(input) = inputs.iter().next() {
        let input_path = Path::new(&input);
        if input_path.is_file()
            && (is_allowed_extension(input_path)
                || input_path.file_name() == Some(OsStr::new(project_file)))
        {
            match canonicalize(input_path) {
                Ok(path_buf) => {
                    search_start = path_buf.parent().unwrap().to_path_buf();
                }
                Err(_) => {
                    return err!(
                        ErrorCode::IoError,
                        "Input file '{input}' not found; make sure that it exists under the provided path"
                    );
                }
            }
        } else if input_path.is_dir() {
            match canonicalize(input_path) {
                Ok(path_buf) => {
                    search_start = path_buf;
                }
                Err(_) => {
                    return err!(
                        ErrorCode::IoError,
                        "Input directory '{input}' not found; make sure that it exists under the provided path"
                    );
                }
            }
        }
    }

    let working_dir = find_path(&search_start, Path::new(project_file));
    match working_dir {
        None => {
            if search_start == env::current_dir()? {
                // check whether the inputs had a path among them, then that is the problem
                if !inputs.is_empty() {
                    err!(
                        ErrorCode::IoError,
                        "Invalid value '{}' for <TARGETS>: Please pass a path that points to or into a dbt project directory", inputs[0]
                    )
                } else {
                    err!(
                        ErrorCode::IoError,
                        "The current directory is not a dbt project directory; cd into it or pass a <path> to it via --project-dir <path>"                )
                }
            } else {
                let relative_path =
                    diff_paths(search_start, env::current_dir()?).unwrap_or(env::current_dir()?);
                err!(
                    ErrorCode::IoError,
                    "Invalid value '{}' for <TARGETS>: Please pass a path that points to or into a dbt project directory", relative_path.display()
                )
            }
        }
        Some(working_dir) => Ok(working_dir),
    }
}

pub fn find_path(starting_directory: &Path, file: &Path) -> Option<PathBuf> {
    if let Some(path) = find_file(starting_directory, file) {
        let mut tmp: PathBuf = path;
        tmp.pop();
        Some(tmp)
    } else {
        None
    }
}
pub fn find_file(starting_directory: &Path, file: &Path) -> Option<PathBuf> {
    let mut path: PathBuf = starting_directory.into();
    loop {
        path.push(file);
        if path.is_file() {
            break canonicalize(path).ok();
        }
        if !(path.pop() && path.pop()) {
            // remove file && remove parent
            break None;
        }
    }
}
pub const YML_EXT: &str = "yml";
pub const SQL_EXT: &str = "sql";
pub const JSON_EXT: &str = "json";

pub fn is_allowed_extension(input_path: &Path) -> bool {
    let extension = input_path.extension().unwrap();
    extension == SQL_EXT || extension == JSON_EXT || extension == YML_EXT
}

pub fn and_n_others(n: usize, items: &[impl ToString]) -> String {
    if items.len() > n {
        format!(
            "{} and {} others",
            items
                .iter()
                .take(n)
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            items.len() - n
        )
    } else {
        items
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}
