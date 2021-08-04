use crate::exceptions::{CalculateError, IOError};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::fs;
use std::fs::DirEntry;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Data {
    pub threshold: f64,
    pub difference: f64,
    pub baseline: f64,
    pub dev: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
                baseline: baseline.median,
                dev: dev.median,
            }
        },
        Calculation {
            metric: ["stddev", metric].join("_"),
            regression: stddev_difference > stddev_threshold,
            data: Data {
                threshold: stddev_threshold,
                difference: stddev_difference,
                baseline: baseline.stddev,
                dev: dev.stddev,
            }
        },
    ]
}

// given a directory, read all files in the directory and return each
// filename with the deserialized json contents of that file
fn measurements_from_files(
    results_directory: &Path,
) -> Result<Vec<(PathBuf, Measurements)>, CalculateError> {
    let result_files = fs::read_dir(results_directory)
        .or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
        .or_else(|e| Err(CalculateError::CalculateIOError(e)))?;

    result_files
        .into_iter()
        .map(|entry| {
            let ent: DirEntry = entry.or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))?;

            Ok(ent.path())
        })
        .collect::<Result<Vec<PathBuf>, CalculateError>>()?
        .into_iter()
        .filter(|path| {
            path
                .extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext.ends_with("json"))
        })
        .map(|path| {
            fs::read_to_string(&path)
                .or_else(|e| Err(IOError::BadFileContentsErr(path.clone(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))
                .and_then(|contents| {
                    serde_json::from_str::<Measurements>(&contents)
                        .or_else(|e| Err(CalculateError::BadJSONErr(path.clone(), Some(e))))
                })
                .map(|m| (path, m))
        })
        .collect()
}

// given a list of filename-measurement pairs, detect any regressions by grouping
// measurements together by filename
fn calculate_regressions(
    measurements: &[(PathBuf, Measurement)],
) -> Result<Vec<Calculation>, CalculateError> {
    let mut measurement_groups: Vec<MeasurementGroup> = measurements
        .into_iter()
        .map(|(p, m)| {
            p.file_name()
                .ok_or_else(|| IOError::MissingFilenameErr(p.to_path_buf()))
                .and_then(|name| {
                    name.to_str()
                        .ok_or_else(|| IOError::FilenameNotUnicodeErr(p.to_path_buf()))
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
        .collect::<Result<Vec<MeasurementGroup>, IOError>>()
        .or_else(|e| Err(CalculateError::CalculateIOError(e)))?;

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
                    let dev = &groups[1];
                    let baseline = &groups[0];
                    
                    if dev.version == "dev" && baseline.version == "baseline" {
                        Ok(calculate(&dev.run, &dev.measurement, &baseline.measurement))
                    } else {
                        Err(CalculateError::BadBranchNameErr(baseline.version.clone(), dev.version.clone()))
                    }
                },
                i => Err(CalculateError::BadGroupSizeErr(i, groups)),
            }
        })
        .collect::<Result<Vec<Vec<Calculation>>, CalculateError>>()?
        .concat();

    Ok(calculations)
}

pub fn regressions(results_directory: &PathBuf) -> Result<Vec<Calculation>, CalculateError> {
    measurements_from_files(Path::new(&results_directory)).and_then(|v| {
        // exit early with an Err if there are no results to process
        match v.len() {
            0 => Err(CalculateError::NoResultsErr(results_directory.clone())),
            i if i % 2 == 1 => Err(CalculateError::OddResultsCountErr(i, results_directory.clone())),
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
