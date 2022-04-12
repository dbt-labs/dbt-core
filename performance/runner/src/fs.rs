use crate::exceptions::{IOError, RunnerError};
use crate::types::*;
use chrono::prelude::*;
use serde::de::DeserializeOwned;
use std::fs::DirEntry;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::str::FromStr;
use std::{cmp, fs};

// TODO these should not be defined here anymore. they need to be split at the github action level.
// To add a new metric to the test suite, simply define it in this list
static METRICS: [HyperfineCmd; 1] = [HyperfineCmd {
    name: "parse",
    prepare: "dbt clean",
    cmd: "dbt parse --no-version-check",
}];

pub fn from_json_files<T: DeserializeOwned>(
    dir: &dyn AsRef<Path>,
) -> Result<Vec<(PathBuf, T)>, RunnerError> {
    let contents = file_contents_from(dir, "json")?;
    let (paths, strings): (Vec<PathBuf>, Vec<String>) = contents.iter().cloned().unzip();
    let ts = map_deserialize(&strings)?;
    Ok(paths.into_iter().zip(ts).collect())
}

// Given a directory, read all files in the directory and return each
// filename with the deserialized json contents of that file.
pub fn map_deserialize<T: DeserializeOwned>(serialized: &[String]) -> Result<Vec<T>, RunnerError> {
    serialized
        .into_iter()
        .map(|contents| {
            serde_json::from_str::<T>(contents)
                .or_else(|e| Err(RunnerError::BadJSONErr(contents.clone(), Some(e))))
        })
        .collect()
}

// gets file contents from a directory for all files with a specific extension.
// great for deserializing with serde.
// `extension` should be everything after the dot. e.g. "json"
pub fn file_contents_from(
    results_directory: &dyn AsRef<Path>,
    extension: &str,
) -> Result<Vec<(PathBuf, String)>, IOError> {
    let entries: Vec<DirEntry> = fs::read_dir(results_directory)
        .or_else(|e| {
            Err(IOError::ReadErr(
                results_directory.as_ref().to_path_buf(),
                Some(e),
            ))
        })?
        .collect::<Result<Vec<DirEntry>, io::Error>>()
        .or_else(|e| {
            Err(IOError::ReadIterErr(
                results_directory.as_ref().to_path_buf(),
                Some(e),
            ))
        })?;

    entries
        .into_iter()
        .filter_map(|entry| {
            let path = entry.path();
            let os_ext = path.extension()?;
            let ext = os_ext.to_str()?;
            if ext.ends_with(extension) {
                Some(path)
            } else {
                None
            }
        })
        .map(|path| {
            fs::read_to_string(&path)
                .map(|contents| (path.clone(), contents))
                .or_else(|e| Err(IOError::BadFileContentsErr(path.clone(), Some(e))))
        })
        .collect()
}

// TODO this should read the commands to run on each project from the project definitions themselves
// not from a hard coded array in this file.
fn get_projects<'a>(
    projects_directory: &dyn AsRef<Path>,
) -> Result<Vec<(PathBuf, String, HyperfineCmd<'a>)>, IOError> {
    let entries = fs::read_dir(projects_directory).or_else(|e| {
        Err(IOError::ReadErr(
            projects_directory.as_ref().to_path_buf(),
            Some(e),
        ))
    })?;

    let results: Vec<(PathBuf, String, HyperfineCmd<'a>)> = entries
        .map(|entry| {
            let path = entry
                .or_else(|e| {
                    Err(IOError::ReadErr(
                        projects_directory.as_ref().to_path_buf(),
                        Some(e),
                    ))
                })?
                .path();

            let project_name: String = path
                .file_name()
                .ok_or_else(|| IOError::MissingFilenameErr(path.clone().to_path_buf()))
                .and_then(|x| {
                    x.to_str()
                        .ok_or_else(|| IOError::FilenameNotUnicodeErr(path.clone().to_path_buf()))
                })?
                .to_owned();

            // each project-metric pair we will run
            // TODO maybe not every command should run on every project. maybe have these defined as files in the project directories?
            let pairs = METRICS
                .iter()
                .map(|metric| (path.clone(), project_name.clone(), metric.clone()))
                .collect::<Vec<(PathBuf, String, HyperfineCmd<'a>)>>();

            Ok(pairs)
        })
        .collect::<Result<Vec<Vec<(PathBuf, String, HyperfineCmd<'a>)>>, IOError>>()?
        .concat();

    Ok(results)
}

// reads directory names under this directory and converts their names to sem-ver versions.
// returns the latest version of the set.
//
// this is used to identify which baseline version we should be targeting to compare samples against.
pub fn latest_version_from(dir: &dyn AsRef<Path>) -> Result<Version, RunnerError> {
    let versions = all_dirs_in(dir)?
        .into_iter()
        // this line is a little opaque but it's just converting OsStr -> String with options along the way.
        .filter_map(|d| {
            d.file_name()
                .and_then(|fname| fname.to_str().map(|x| x.to_string()))
        })
        .map(|fname| Version::from_str(&fname))
        .collect::<Result<Vec<Version>, RunnerError>>()?;

    versions
        .into_iter()
        .reduce(cmp::max)
        .ok_or_else(|| RunnerError::NoVersionedBaselineData(dir.as_ref().to_path_buf()))
}

fn all_dirs_in(dir: &dyn AsRef<Path>) -> Result<Vec<PathBuf>, IOError> {
    Ok(fs::read_dir(dir)
        .or_else(|e| Err(IOError::ReadErr(dir.as_ref().to_path_buf(), Some(e))))?
        .collect::<Result<Vec<DirEntry>, io::Error>>()
        .or_else(|e| Err(IOError::ReadIterErr(dir.as_ref().to_path_buf(), Some(e))))?
        .into_iter()
        .filter_map(|d| {
            let path = d.path();
            if path.is_dir() {
                Some(path)
            } else {
                None
            }
        })
        .collect())
}

// TODO can we call hyperfine as a rust library?
// https://crates.io/crates/hyperfine/1.13.0
fn run_hyperfine(
    run_dir: &dyn AsRef<Path>,
    command: &str,
    prep: &str,
    runs: i32,
    output_file: &dyn AsRef<Path>,
) -> Result<ExitStatus, IOError> {
    Command::new("hyperfine")
        .current_dir(run_dir)
        // warms filesystem caches by running the command first without counting it.
        // alternatively we could clear them before each run
        .arg("--warmup")
        .arg("1")
        // --min-runs defaults to 10
        .arg("--min-runs")
        .arg(runs.to_string())
        .arg("--max-runs")
        .arg(runs.to_string())
        .arg("--prepare")
        .arg(prep)
        .arg(command)
        .arg("--export-json")
        .arg(output_file.as_ref())
        // this prevents hyperfine from capturing dbt's output.
        // Noisy, but good for debugging when tests fail.
        .arg("--show-output")
        .status() // use spawn() here instead for more information
        .or_else(|e| Err(IOError::CommandErr(Some(e))))
}

// Attempt to delete the directory and its contents. If it doesn't exist we'll just recreate it anyway.
fn clear_dir(dir: &dyn AsRef<Path>) -> Result<(), io::Error> {
    match fs::remove_dir_all(dir) {
        // whether it existed or not, create the directory and any missing parent dirs.
        _ => fs::create_dir_all(dir),
    }
}

// deletes the output directory, makes one hyperfine run for each project-metric pair,
// reads in the results, and returns a Sample for each project-metric pair.
pub fn take_samples(
    projects_dir: &dyn AsRef<Path>,
    out_dir: &dyn AsRef<Path>,
) -> Result<Vec<Sample>, RunnerError> {
    clear_dir(out_dir).or_else(|e| {
        Err(IOError::CannotRecreateTempDirErr(
            out_dir.as_ref().to_path_buf(),
            e,
        ))
    })?;

    // using one time stamp for all samples.
    let ts = Utc::now();

    // run hyperfine in serial for each project-metric pair
    for (path, project_name, hcmd) in get_projects(projects_dir)? {
        let metric = Metric {
            name: hcmd.name.to_owned(),
            project_name: project_name.to_owned(),
        };

        let command = format!("{} --profiles-dir ../../project_config/", hcmd.cmd);
        let mut output_file = out_dir.as_ref().to_path_buf();
        output_file.push(metric.filename());

        // TODO we really want one run, not two. Right now the second is discarded. so we might not want to use hyperfine for taking samples.
        let status = run_hyperfine(&path, &command, hcmd.clone().prepare, 2, &output_file)
            .or_else(|e| Err(RunnerError::from(e)))?;

        match status.code() {
            Some(code) if code != 0 => return Err(RunnerError::HyperfineNonZeroExitCode(code)),
            _ => (),
        }
    }

    let samples = from_json_files::<Measurements>(out_dir)?
        .into_iter()
        .map(|(path, measurement)| {
            Sample::from_measurement(
                &path,
                &measurement.results[0], // TODO if its empty it'll panic.
                ts,
            )
        })
        .collect::<Result<Vec<Sample>, RunnerError>>()?;

    Ok(samples)
}

// Calls hyperfine via system command, reads in the results, and writes out Baseline json files.
// Intended to be called after each new version is released.
pub fn model<'a>(
    version: Version,
    projects_directory: &dyn AsRef<Path>,
    out_dir: &dyn AsRef<Path>,
    tmp_dir: &dyn AsRef<Path>,
    n_runs: i32,
) -> Result<Vec<Baseline>, RunnerError> {
    for (path, project_name, hcmd) in get_projects(projects_directory)? {
        let metric = Metric {
            name: hcmd.name.to_owned(),
            project_name: project_name.to_owned(),
        };

        let command = format!("{} --profiles-dir ../../project_config/", hcmd.clone().cmd);
        let mut tmp_file = tmp_dir.as_ref().to_path_buf();
        tmp_file.push(metric.filename());

        let status = run_hyperfine(&path, &command, hcmd.clone().prepare, n_runs, &tmp_file)
            .or_else(|e| Err(RunnerError::from(e)))?;

        match status.code() {
            Some(code) if code != 0 => return Err(RunnerError::HyperfineNonZeroExitCode(code)),
            _ => (),
        }
    }

    // read what hyperfine wrote
    let measurements: Vec<(PathBuf, Measurements)> = from_json_files::<Measurements>(tmp_dir)?;

    // put it in the right format using the same timestamp for every model.
    let now = Utc::now();
    let baselines: Vec<Baseline> = measurements
        .into_iter()
        .map(|m| {
            let (path, measurement) = m;
            from_measurement(version, path, measurement, now)
        })
        .collect::<Result<Vec<Baseline>, RunnerError>>()?;

    // write a file for each baseline measurement
    for model in &baselines {
        // create the correct filename like `/out_dir/1.0.0/parse___2000_models.json`
        let mut out_file = out_dir.as_ref().to_path_buf();
        out_file.push(version.to_string());

        // write out the version directory. ignore errors since if it's already made that's fine.
        match fs::create_dir(out_file.clone()) {
            _ => (),
        };

        // continue creating the correct filename
        out_file.push(model.metric.filename());
        out_file.set_extension("json");

        // get the serialized string
        let s = serde_json::to_string(&model).or_else(|e| Err(RunnerError::SerializationErr(e)))?;

        // TODO writing files in _this function_ isn't the most graceful way organize the code.
        // write the newly modeled baseline to the above path
        fs::write(out_file.clone(), s)
            .or_else(|e| Err(IOError::WriteErr(out_file.clone(), Some(e))))?;
    }

    Ok(baselines)
}

// baseline filenames are expected to encode the metric information
fn from_measurement(
    version: Version,
    path: PathBuf,
    measurements: Measurements,
    // forcing time to be provided so that uniformity of time stamps across a set of baselines is more explicit
    ts: DateTime<Utc>,
) -> Result<Baseline, RunnerError> {
    // `file_name` is boop___proj.json. `file_stem` is boop___proj.
    let filestem = path.file_stem().map_or_else(
        || Err(IOError::BadFilestemError(path.clone())),
        |stem| Ok(stem.to_string_lossy().to_string()),
    )?;

    let metric = Metric::from_str(&filestem)?;

    Ok(Baseline {
        version: version,
        metric: metric,
        ts: ts,
        measurement: measurements.results[0].clone(),
    })
}
