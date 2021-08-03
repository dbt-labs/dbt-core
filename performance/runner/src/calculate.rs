use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::{fs, io};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Measurement {
    pub command: String,
    pub mean: f64,
    pub stddev: f64,
    pub median: f64,
    pub user: f64,
    pub system: f64,
    pub min: f64,
    pub max: f64,
    pub times: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Measurements {
    pub results: Vec<Measurement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Data {
    pub threshold: f64,
    pub difference: f64,
    pub baseline: f64,
    pub dev: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Calculation {
    pub metric: String,
    pub regression: bool,
    pub data: Data,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeasurementGroup {
    pub version: String,
    pub run: String,
    pub measurement: Measurement,
}

#[derive(Error, Debug)]
pub enum TestError {
    #[error("BadJSONErr: JSON in file cannot be deserialized as expected.\nFilepath: {}\nOriginating Exception:{}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    BadJSONErr(PathBuf, Option<serde_json::Error>),
    #[error("ReadErr: The file cannot be read.\nFilepath: {}\nOriginating Exception:{}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    ReadErr(PathBuf, Option<io::Error>),
    #[error("MissingFilenameErr: The path provided does not specify a file.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    MissingFilenameErr(PathBuf),
    #[error("FilenameNotUnicodeErr: The filename is not expressible in unicode. Consider renaming the file.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    FilenameNotUnicodeErr(PathBuf),
    #[error("BadFileContentsErr: Check that the file exists and is readable.\nFilepath: {}\nOriginating Exception:{}", .0.to_string_lossy().into_owned(), .1.as_ref().map_or("None".to_owned(), |e| format!("{}", e)))]
    BadFileContentsErr(PathBuf, Option<io::Error>),
    #[error("NoResultsErr: The results directory has no json files in it.\nFilepath: {}", .0.to_string_lossy().into_owned())]
    NoResultsErr(PathBuf),
    #[error("OddResultsCountErr: The results directory has an odd number of results in it. Expected an even number.\nFile Count: {}\nFilepath: {}", .0, .1.to_string_lossy().into_owned())]
    OddResultsCountErr(usize, PathBuf),
    #[error("BadGroupSizeErr: Expected two results per group, one for each branch-project pair.\nCount: {}\nGroup: {:?}", .0, .1.into_iter().map(|group| (&group.version[..], &group.run[..])).collect::<Vec<(&str, &str)>>())]
    BadGroupSizeErr(usize, Vec<MeasurementGroup>),
    #[error("BadBranchNameErr: Branch names must be 'baseline' and 'dev'.\nFound: {}, {}", .0, .1)]
    BadBranchNameErr(String, String),
}

fn calculate(metric: &str, dev: &Measurement, baseline: &Measurement) -> Vec<Calculation> {
    let median_threshold = 1.05; // 5% regression threshold
    let median_difference = dev.median / baseline.median;

    let stddev_threshold = 1.05; // 5% regression threshold
    let stddev_difference = dev.stddev / baseline.stddev;

    vec![
        Calculation {
            metric: ["median", metric].join("_"),
            regression: median_difference > median_threshold,
            data: Data {
                threshold: median_threshold,
                difference: median_difference,
                baseline: baseline.median.clone(),
                dev: dev.median.clone(),
            }
        },
        Calculation {
            metric: ["stddev", metric].join("_"),
            regression: stddev_difference > stddev_threshold,
            data: Data {
                threshold: stddev_threshold,
                difference: stddev_difference,
                baseline: baseline.stddev.clone(),
                dev: dev.stddev.clone(),
            }
        },
    ]
}

// given a directory, read all files in the directory and return each
// filename with the deserialized json contents of that file
fn measurements_from_files(
    results_directory: &Path,
) -> Result<Vec<(PathBuf, Measurements)>, TestError> {
    let result_files = fs::read_dir(results_directory)
        .or_else(|e| Err(TestError::ReadErr(results_directory.to_path_buf(), Some(e))))?;

    result_files
        .into_iter()
        .map(|f| f.unwrap().path())
        .filter(|filename| {
            filename
                .extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext.ends_with("json"))
        })
        .map(|filename| {
            fs::read_to_string(&filename)
                .or_else(|e| Err(TestError::BadFileContentsErr(filename.clone(), Some(e))))
                .and_then(|contents| {
                    serde_json::from_str::<Measurements>(&contents)
                        .or_else(|e| Err(TestError::BadJSONErr(filename.clone(), Some(e))))
                })
                .map(|m| (filename, m))
        })
        .collect()
}

// given a list of filename-measurement pairs, detect any regressions by grouping
// measurements together by filename
fn calculate_regressions(
    measurements: &[(PathBuf, Measurement)],
) -> Result<Vec<Calculation>, TestError> {
    let mut measurement_groups: Vec<MeasurementGroup> = measurements
        .into_iter()
        .map(|(p, m)| {
            p.file_name()
                .ok_or_else(|| TestError::MissingFilenameErr(p.to_path_buf()))
                .and_then(|name| {
                    name.to_str()
                        .ok_or_else(|| TestError::FilenameNotUnicodeErr(p.to_path_buf()))
                })
                .map(|name| {
                    let parts: Vec<&str> = name.split("_").collect();
                    MeasurementGroup {
                        version: parts[0].to_owned(),
                        run: parts[1..].join("_"),
                        measurement: m.clone(),
                    }
                })
        })
        .collect::<Result<Vec<MeasurementGroup>, TestError>>()?;

    measurement_groups.sort_by(|x, y| (&x.run, &x.version).cmp(&(&y.run, &y.version)));

    // locking up mutation
    let sorted_measurement_groups = measurement_groups;

    let calculations: Vec<Calculation> = sorted_measurement_groups
        .into_iter()
        .group_by(|x| x.run.clone())
        .into_iter()
        .map(|(_, g)| {
            let mut groups: Vec<MeasurementGroup> = g.collect();
            groups.sort_by(|x, y| x.version.cmp(&y.version));

            match groups.len() {
                2 => {
                    let dev = &groups[0];
                    let baseline = &groups[1];
                    
                    if dev.version == "dev" && baseline.version == "baseline" {
                        calculate(&dev.run, &dev.measurement, &baseline.measurement).into_iter().map(Ok).collect()
                    } else {
                        vec![Err(TestError::BadBranchNameErr(baseline.version.clone(), dev.version.clone()))]
                    }
                },
                i => vec![Err(TestError::BadGroupSizeErr(i, groups))],
            }
        })
        .flatten()
        .collect::<Result<Vec<Calculation>, TestError>>()?;

    Ok(calculations)
}

pub fn regressions(results_directory: &PathBuf) -> Result<Vec<Calculation>, TestError> {
    measurements_from_files(Path::new(&results_directory)).and_then(|v| {
        // exit early with an Err if there are no results to process
        match v.len() {
            0 => Err(TestError::NoResultsErr(results_directory.clone())),
            i if i % 2 == 1 => Err(TestError::OddResultsCountErr(i, results_directory.clone())),
            _ => Ok(()),
        }?;

        let v_next: Vec<(PathBuf, Measurement)> = v
            .into_iter()
            // the way we're running these, the files will each contain exactly one measurement, hence `results[0]`
            .map(|(p, ms)| (p, ms.results[0].clone()))
            .collect();

        calculate_regressions(&v_next)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_messages() {
        let pairs = vec![
            (
                TestError::BadJSONErr(Path::new("dummy/path/file.json").to_path_buf(), None),
                r#"BadJSONErr: JSON in file cannot be deserialized as expected.
Filepath: dummy/path/file.json
Originating Exception:None"#
            ),
            (
                TestError::ReadErr(Path::new("dummy/path/file.json").to_path_buf(), None),
                r#"ReadErr: The file cannot be read.
Filepath: dummy/path/file.json
Originating Exception:None"#
            ),
            (
                TestError::MissingFilenameErr(Path::new("dummy/path/no_file/").to_path_buf()),
                r#"MissingFilenameErr: The path provided does not specify a file.
Filepath: dummy/path/no_file/"#
            ),
            (
                TestError::FilenameNotUnicodeErr(Path::new("dummy/path/no_file/").to_path_buf()),
                r#"FilenameNotUnicodeErr: The filename is not expressible in unicode. Consider renaming the file.
Filepath: dummy/path/no_file/"#
            ),
            (
                TestError::BadFileContentsErr(Path::new("dummy/path/filenotexist.json").to_path_buf(), None),
                r#"BadFileContentsErr: Check that the file exists and is readable.
Filepath: dummy/path/filenotexist.json
Originating Exception:None"#
            ),
            (
                TestError::NoResultsErr(Path::new("dummy/path/no_file/").to_path_buf()),
                r#"NoResultsErr: The results directory has no json files in it.
Filepath: dummy/path/no_file/"#
            ),
            (
                TestError::OddResultsCountErr(3, Path::new("dummy/path/no_file/").to_path_buf()),
                r#"OddResultsCountErr: The results directory has an odd number of results in it. Expected an even number.
File Count: 3
Filepath: dummy/path/no_file/"#
            ),
            (
                TestError::BadGroupSizeErr(1, vec![MeasurementGroup {
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
                    }
                }]),
                r#"BadGroupSizeErr: Expected two results per group, one for each branch-project pair.
Count: 1
Group: [("dev", "some command")]"#
            ),
            (
                TestError::BadBranchNameErr("boop".to_owned(), "noop".to_owned()).to_path_buf(),
                r#"BadBranchNameErr: Branch names must be 'baseline' and 'dev'.
Found: boop, noop"#
            ),


            
        ];

        for (err, msg) in pairs {
            assert_eq!(format!("{}", err), msg)
        }
    }

}
