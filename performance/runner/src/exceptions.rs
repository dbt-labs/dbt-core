use crate::types::Metric;
use std::io;
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
    #[error("CannotRecreateTempDirErr: attempted to delete and recreate temp dir at path {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1)]
    CannotRecreateTempDirErr(PathBuf, io::Error),
    #[error("BadFilestemError: failed to read the filestem from path {}", .0.to_string_lossy().into_owned())]
    BadFilestemError(PathBuf),
    #[error("ReadIterErr: While traversing a directory, an error occured.\nDirectory: {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    ReadIterErr(PathBuf, Option<io::Error>),
}

// Custom Error messages for the error states we could encounter
// in any of the subcommands, and are not prevented at compile time.
// New constructors should be added for any new error situations that
// come up. The desired output of these errors is tested below.
#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("VersionParseFail: Error parsing input `{}`. Must be in the format \"major.minor.patch\" where each component is an integer.", .0)]
    VersionParseFail(String),
    #[error("MetricParseFail: Error parsing input `{}`. Must be in the format \"metricname___projectname\" with no file extensions.", .0)]
    MetricParseFail(String),
    #[error("BadJSONErr: JSON in file cannot be deserialized as expected.\nRaw json: {}\nOriginating Exception: {}", .0, .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    BadJSONErr(String, Option<serde_json::Error>),
    #[error("SerializationErr: Object cannot be serialized as expected.\nOriginating Exception: {}", .0)]
    SerializationErr(serde_json::Error),
    #[error("{}", .0)]
    RunnerIOError(IOError),
    #[error("HyperfineNonZeroExitCode: Hyperfine child process exited with non-zero exit code: {}", .0)]
    HyperfineNonZeroExitCode(i32),
    #[error("NoVersionedBaselineData: There was no versioned data in the following directory: {}\n expected structure like <baseline-dir>/<sem-ver-dir>/metric.json", .0.to_string_lossy().into_owned())]
    NoVersionedBaselineData(PathBuf),
    #[error("BaselineMetricNotSampled: The metric {} on project {} was included in the baseline comparison but was not sampled.", .0.name, .0.project_name)]
    BaselineMetricNotSampled(Metric),
}

impl From<IOError> for RunnerError {
    fn from(item: IOError) -> Self {
        RunnerError::RunnerIOError(item)
    }
}
