use crate::calculate::*;
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
    #[error("MissingFilenameErr: The path provided does not specify a file.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    MissingFilenameErr(PathBuf),
    #[error("FilenameNotUnicodeErr: The filename is not expressible in unicode. Consider renaming the file.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    FilenameNotUnicodeErr(PathBuf),
    #[error("BadFileContentsErr: Check that the file exists and is readable.\nFilepath: {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    BadFileContentsErr(PathBuf, Option<io::Error>),
    #[error("CommandErr: System command failed to run.\nOriginating Exception: {}", .0.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    CommandErr(Option<io::Error>),
}

// Custom Error messages for the error states we could encounter
// during calculation, and are not prevented at compile time. New
// constructors should be added for any new error situations that
// come up. The desired output of these errors is tested below.
#[derive(Debug, Error)]
pub enum CalculateError {
    #[error("BadJSONErr: JSON in file cannot be deserialized as expected.\nFilepath: {}\nOriginating Exception: {}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    BadJSONErr(PathBuf, Option<serde_json::Error>),
    #[error("{}", .0)]
    CalculateIOError(IOError),
    #[error("NoResultsErr: The results directory has no json files in it.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    NoResultsErr(PathBuf),
    #[error("OddResultsCountErr: The results directory has an odd number of results in it. Expected an even number.\nFile Count: {}\nFilepath: {}", .0, .1.to_string_lossy().into_owned())]
    OddResultsCountErr(usize, PathBuf),
    #[error("BadGroupSizeErr: Expected two results per group, one for each branch-project pair.\nCount: {}\nGroup: {:?}", .0, .1.into_iter().map(|group| (&group.version[..], &group.run[..])).collect::<Vec<(&str, &str)>>())]
    BadGroupSizeErr(usize, Vec<MeasurementGroup>),
    #[error("BadBranchNameErr: Branch names must be 'baseline' and 'dev'.\nFound: {}, {}", .0, .1)]
    BadBranchNameErr(String, String),
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
                CalculateError::BadJSONErr(Path::new("dummy/path/file.json").to_path_buf(), None),
                r#"BadJSONErr: JSON in file cannot be deserialized as expected.
Filepath: dummy/path/file.json
Originating Exception: None"#,
            ),
            (
                CalculateError::BadJSONErr(Path::new("dummy/path/file.json").to_path_buf(), None),
                r#"BadJSONErr: JSON in file cannot be deserialized as expected.
Filepath: dummy/path/file.json
Originating Exception: None"#,
            ),
            (
                CalculateError::NoResultsErr(Path::new("dummy/path/no_file/").to_path_buf()),
                r#"NoResultsErr: The results directory has no json files in it.
Filepath: dummy/path/no_file/"#,
            ),
            (
                CalculateError::OddResultsCountErr(
                    3,
                    Path::new("dummy/path/no_file/").to_path_buf(),
                ),
                r#"OddResultsCountErr: The results directory has an odd number of results in it. Expected an even number.
File Count: 3
Filepath: dummy/path/no_file/"#,
            ),
            (
                CalculateError::BadGroupSizeErr(
                    1,
                    vec![MeasurementGroup {
                        version: "dev".to_owned(),
                        run: "some command".to_owned(),
                        measurement: Measurement {
                            command: "some command".to_owned(),
                            mean: 1.0,
                            stddev: 1.0,
                            median: 1.0,
                            user: 1.0,
                            system: 1.0,
                            min: 1.0,
                            max: 1.0,
                            times: vec![1.0, 1.1, 0.9, 1.0, 1.1, 0.9, 1.1],
                        },
                    }],
                ),
                r#"BadGroupSizeErr: Expected two results per group, one for each branch-project pair.
Count: 1
Group: [("dev", "some command")]"#,
            ),
            (
                CalculateError::BadBranchNameErr("boop".to_owned(), "noop".to_owned()),
                r#"BadBranchNameErr: Branch names must be 'baseline' and 'dev'.
Found: boop, noop"#,
            ),
        ];

        for (err, msg) in pairs {
            assert_eq!(format!("{}", err), msg)
        }
    }
}
