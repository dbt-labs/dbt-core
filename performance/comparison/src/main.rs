use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::{env, fs, io};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Measurement {
    command: String,
    mean: f64,
    stddev: f64,
    median: f64,
    user: f64,
    system: f64,
    min: f64,
    max: f64,
    times: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Measurements {
    results: Vec<Measurement>,
}

#[derive(Debug, Clone, PartialEq)]
struct Regression {
    threshold: f64,
    difference: f64,
    baseline: Measurement,
    latest: Measurement,
}

#[derive(Debug, Clone, PartialEq)]
struct MeasurementGroup {
    version: String,
    run: String,
    measurement: Measurement,
}

#[derive(Error, Debug)]
enum TestError {
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
}

fn regression(baseline: &Measurement, latest: &Measurement) -> Option<Regression> {
    let threshold = 1.05; // 5% regression threshold
    let difference = latest.median / baseline.median;
    if difference > threshold {
        Some(Regression {
            threshold,
            difference,
            baseline: baseline.clone(),
            latest: latest.clone(),
        })
    } else {
        None
    }
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
            println!("{:?}", filename);
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
fn detect_regressions(
    measurements: &[(PathBuf, Measurement)],
) -> Result<Vec<Regression>, TestError> {
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
                        run: parts[1..].join(""),
                        measurement: m.clone(),
                    }
                })
        })
        .collect::<Result<Vec<MeasurementGroup>, TestError>>()?;

    measurement_groups.sort_by(|x, y| (&x.run, &x.version).cmp(&(&y.run, &y.version)));

    // locking up mutation
    let sorted_measurement_groups = measurement_groups;

    let x: Vec<Regression> = sorted_measurement_groups
        .into_iter()
        .group_by(|x| (x.run.clone(), x.version.clone()))
        .into_iter()
        .map(|(_, group)| {
            let g: Vec<MeasurementGroup> = group.collect();
            regression(&g[0].measurement, &g[1].measurement)
        })
        .flatten()
        .collect();

    Ok(x)
}

fn main() {
    // TODO args lib
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("please provide the results directory");
        exit(1);
    }
    let results_directory = &args[1];

    let regressions = measurements_from_files(Path::new(&results_directory)).and_then(|v| {
        let v_next: Vec<(PathBuf, Measurement)> = v
            .into_iter()
            // the way we're running these, the files will each contain exactly one measurement, hence `results[0]`
            .map(|(p, ms)| (p, ms.results[0].clone()))
            .collect();

        detect_regressions(&v_next)
    });

    match regressions {
        Err(e) => panic!("{}", e),
        Ok(rs) => match rs[..] {
            [] => println!("congrats! no regressions"),
            _ => {
                for r in rs {
                    println!("{:?}", r);
                }
                println!("the above regressions were found.");
                exit(1)
            }
        },
    }
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
        ];

        for (err, msg) in pairs {
            assert_eq!(format!("{}", err), msg)
        }
    }

}
