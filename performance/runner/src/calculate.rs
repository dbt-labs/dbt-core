use crate::exceptions::{CalculateError, IOError};
use chrono::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};

// This type exactly matches the type of array elements
// from hyperfine's output. Deriving `Serialize` and `Deserialize`
// gives us read and write capabilities via json_serde.
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

// This type exactly matches the type of hyperfine's output.
// Deriving `Serialize` and `Deserialize` gives us read and
// write capabilities via json_serde.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Measurements {
    pub results: Vec<Measurement>,
}

// Output data from a comparison between runs on the baseline
// and dev branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Data {
    pub threshold: f64,
    pub difference: f64,
    pub baseline: f64,
    pub dev: f64,
}

// The full output from a comparison between runs on the baseline
// and dev branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Calculation {
    pub metric: String,
    pub regression: bool,
    pub ts: DateTime<Utc>,
    pub data: Data,
}

// A type to describe which measurement we are working with. This
// information is parsed from the filename of hyperfine's output.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasurementGroup {
    pub version: String,
    pub run: String,
    pub measurement: Measurement,
}

// Given two measurements, return all the calculations. Calculations are
// flagged as regressions or not regressions.
fn calculate(metric: &str, dev: &Measurement, baseline: &Measurement) -> Vec<Calculation> {
    // choosing the current timestamp for all calculations to be the same.
    // this timestamp is not from the time of measurement becuase hyperfine
    // controls that. Since calculation is run directly after, this is fine.
    let ts = Utc::now();

    let median_threshold = 1.05; // 5% regression threshold
    let median_difference = dev.median / baseline.median;

    let stddev_threshold = 1.20; // 20% regression threshold
    let stddev_difference = dev.stddev / baseline.stddev;

    vec![
        Calculation {
            metric: ["median", metric].join("_"),
            regression: median_difference > median_threshold,
            ts: ts,
            data: Data {
                threshold: median_threshold,
                difference: median_difference,
                baseline: baseline.median,
                dev: dev.median,
            },
        },
        Calculation {
            metric: ["stddev", metric].join("_"),
            regression: stddev_difference > stddev_threshold,
            ts: ts,
            data: Data {
                threshold: stddev_threshold,
                difference: stddev_difference,
                baseline: baseline.stddev,
                dev: dev.stddev,
            },
        },
    ]
}

// Given a directory, read all files in the directory and return each
// filename with the deserialized json contents of that file.
fn measurements_from_files(
    results_directory: &Path,
) -> Result<Vec<(PathBuf, Measurements)>, CalculateError> {
    fs::read_dir(results_directory)
        .or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
        .or_else(|e| Err(CalculateError::CalculateIOError(e)))?
        .into_iter()
        .map(|entry| {
            let ent: DirEntry = entry
                .or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))?;

            Ok(ent.path())
        })
        .collect::<Result<Vec<PathBuf>, CalculateError>>()?
        .iter()
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext.ends_with("json"))
        })
        .map(|path| {
            fs::read_to_string(path)
                .or_else(|e| Err(IOError::BadFileContentsErr(path.clone(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))
                .and_then(|contents| {
                    serde_json::from_str::<Measurements>(&contents)
                        .or_else(|e| Err(CalculateError::BadJSONErr(path.clone(), Some(e))))
                })
                .map(|m| (path.clone(), m))
        })
        .collect()
}

// Given a list of filename-measurement pairs, detect any regressions by grouping
// measurements together by filename.
fn calculate_regressions(
    measurements: &[(&PathBuf, &Measurement)],
) -> Result<Vec<Calculation>, CalculateError> {
    /*
        Strategy of this function body:
        1. [Measurement] -> [MeasurementGroup]
        2. Sort the MeasurementGroups
        3. Group the MeasurementGroups by "run"
        4. Call `calculate` with the two resulting Measurements as input
    */

    let mut measurement_groups: Vec<MeasurementGroup> = measurements
        .iter()
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
                        measurement: (*m).clone(),
                    }
                })
        })
        .collect::<Result<Vec<MeasurementGroup>, IOError>>()
        .or_else(|e| Err(CalculateError::CalculateIOError(e)))?;

    measurement_groups.sort_by(|x, y| (&x.run, &x.version).cmp(&(&y.run, &y.version)));

    // locking up mutation
    let sorted_measurement_groups = measurement_groups;

    let calculations: Vec<Calculation> = sorted_measurement_groups
        .iter()
        .group_by(|x| &x.run)
        .into_iter()
        .map(|(_, g)| {
            let mut groups: Vec<&MeasurementGroup> = g.collect();
            groups.sort_by(|x, y| x.version.cmp(&y.version));

            match groups.len() {
                2 => {
                    let dev = &groups[1];
                    let baseline = &groups[0];

                    if dev.version == "dev" && baseline.version == "baseline" {
                        Ok(calculate(&dev.run, &dev.measurement, &baseline.measurement))
                    } else {
                        Err(CalculateError::BadBranchNameErr(
                            baseline.version.clone(),
                            dev.version.clone(),
                        ))
                    }
                }
                i => {
                    let gs: Vec<MeasurementGroup> = groups.into_iter().map(|x| x.clone()).collect();
                    Err(CalculateError::BadGroupSizeErr(i, gs))
                }
            }
        })
        .collect::<Result<Vec<Vec<Calculation>>, CalculateError>>()?
        .concat();

    Ok(calculations)
}

// Top-level function. Given a path for the result directory, call the above
// functions to compare and collect calculations. Calculations include both
// metrics that fall within the threshold and regressions.
pub fn regressions(results_directory: &PathBuf) -> Result<Vec<Calculation>, CalculateError> {
    measurements_from_files(Path::new(&results_directory)).and_then(|v| {
        // exit early with an Err if there are no results to process
        if v.len() <= 0 {
            Err(CalculateError::NoResultsErr(results_directory.clone()))
        // we expect two runs for each project-metric pairing: one for each branch, baseline
        // and dev. An odd result count is unexpected.
        } else if v.len() % 2 == 1 {
            Err(CalculateError::OddResultsCountErr(
                v.len(),
                results_directory.clone(),
            ))
        } else {
            // otherwise, we can do our comparisons
            let measurements = v
                .iter()
                // the way we're running these, the files will each contain exactly one measurement, hence `results[0]`
                .map(|(p, ms)| (p, &ms.results[0]))
                .collect::<Vec<(&PathBuf, &Measurement)>>();

            calculate_regressions(&measurements[..])
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_5_percent_regression() {
        let dev = Measurement {
            command: "some command".to_owned(),
            mean: 1.06,
            stddev: 1.06,
            median: 1.06,
            user: 1.06,
            system: 1.06,
            min: 1.06,
            max: 1.06,
            times: vec![],
        };

        let baseline = Measurement {
            command: "some command".to_owned(),
            mean: 1.00,
            stddev: 1.00,
            median: 1.00,
            user: 1.00,
            system: 1.00,
            min: 1.00,
            max: 1.00,
            times: vec![],
        };

        let calculations = calculate("test_metric", &dev, &baseline);
        let regressions: Vec<&Calculation> =
            calculations.iter().filter(|calc| calc.regression).collect();

        // expect one regression for median
        println!("{:#?}", regressions);
        assert_eq!(regressions.len(), 1);
        assert_eq!(regressions[0].metric, "median_test_metric");
    }
}
