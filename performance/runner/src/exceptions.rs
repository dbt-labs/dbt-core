use std::io;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

// Custom IO Error messages for the IO errors we encounter.
// New constructors should be added to wrap any new IO errors.
// The desired output of these errors is tested below.
#[derive(Debug, Error)]
pub enum IOError {
    #[error("ReadErr: The file cannot be read.\nFilepath: {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    ReadErr(PathBuf, Option<io::Error>),
    #[error("WriteErr: The file cannot be written to.\nFilepath: {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    WriteErr(PathBuf, Option<io::Error>),
    #[error("MissingFilenameErr: The path provided does not specify a file.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    MissingFilenameErr(PathBuf),
    #[error("FilenameNotUnicodeErr: The filename is not expressible in unicode. Consider renaming the file.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    FilenameNotUnicodeErr(PathBuf),
    #[error("BadFileContentsErr: Check that the file exists and is readable.\nFilepath: {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    BadFileContentsErr(PathBuf, Option<io::Error>),
    #[error("CommandErr: System command failed to run.\nOriginating Exception: {}", .0.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    CommandErr(Option<io::Error>),
    #[error("CannotRerreateTempDirErr: attempted to delete and recreate temp dir at path {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1)]
    CannotRecreateTempDirErr(PathBuf, io::Error),
}

// Custom Error messages for the error states we could encounter
// in any of the subcommands, and are not prevented at compile time.
// New constructors should be added for any new error situations that
// come up. The desired output of these errors is tested below.
// TODO make the tests up-to-date with the newly added constructors
#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("VersionParseFail: Error parsing input `{}`. Must be in the format \"major.minor.patch\" where each component is an integer.", .0)]
    VersionParseFail(String),
    #[error("MetricParseFail: Error parsing input `{}`. Must be in the format \"metricname___projectname\" with no file extensions.", .0)]
    MetricParseFail(String),
    #[error("BadJSONErr: JSON in file cannot be deserialized as expected.\nFilepath: {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    BadJSONErr(PathBuf, Option<serde_json::Error>),
    #[error("SerializationErr: Object cannot be serialized as expected.\nOriginating Exception: {}", .0)]
    SerializationErr(serde_json::Error),
    #[error("{}", .0)]
    RunnerIOError(IOError),
    #[error("HyperfineNonZeroExitCode: Hyperfine child process exited with non-zero exit code: {}", .0)]
    HyperfineNonZeroExitCode(i32),
    #[error("BaselineWithNoModelsErr: Cannot create a baseline from zero models.")]
    BaselineWithNoModelsErr(),
}

impl From<IOError> for RunnerError {
    fn from(item: IOError) -> Self {
        RunnerError::RunnerIOError(item)
    }
}

// Tests for exceptions
#[cfg(test)]
mod tests {
    use super::*;

    // Tests the output fo io error messages. There should be at least one per enum constructor.
    #[test]
    fn test_io_error_messages() {
        let pairs = vec![
            (
                IOError::ReadErr(Path::new("dummy/path/file.json").to_path_buf(), None),
                r#"ReadErr: The file cannot be read.
Filepath: dummy/path/file.json
Originating Exception: None"#,
            ),
            (
                IOError::MissingFilenameErr(Path::new("dummy/path/no_file/").to_path_buf()),
                r#"MissingFilenameErr: The path provided does not specify a file.
Filepath: dummy/path/no_file/"#,
            ),
            (
                IOError::FilenameNotUnicodeErr(Path::new("dummy/path/no_file/").to_path_buf()),
                r#"FilenameNotUnicodeErr: The filename is not expressible in unicode. Consider renaming the file.
Filepath: dummy/path/no_file/"#,
            ),
            (
                IOError::BadFileContentsErr(
                    Path::new("dummy/path/filenotexist.json").to_path_buf(),
                    None,
                ),
                r#"BadFileContentsErr: Check that the file exists and is readable.
Filepath: dummy/path/filenotexist.json
Originating Exception: None"#,
            ),
            (
                IOError::CommandErr(None),
                r#"CommandErr: System command failed to run.
Originating Exception: None"#,
            ),
        ];

        for (err, msg) in pairs {
            assert_eq!(format!("{}", err), msg)
        }
    }

    // Tests the output fo calculate error messages. There should be at least one per enum constructor.
    #[test]
    fn test_calculate_error_messages() {
        let pairs = vec![
            (
                RunnerError::BadJSONErr(Path::new("dummy/path/file.json").to_path_buf(), None),
                r#"BadJSONErr: JSON in file cannot be deserialized as expected.
Filepath: dummy/path/file.json
Originating Exception: None"#,
            ),
            (
                RunnerError::BadJSONErr(Path::new("dummy/path/file.json").to_path_buf(), None),
                r#"BadJSONErr: JSON in file cannot be deserialized as expected.
Filepath: dummy/path/file.json
Originating Exception: None"#,
            ),
        ];

        for (err, msg) in pairs {
            assert_eq!(format!("{}", err), msg)
        }
    }
}
