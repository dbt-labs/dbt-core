use crate::exceptions::IOError;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, ExitStatus};

// `Metric` defines a dbt command that we want to measure on both the
// baseline and dev branches.
#[derive(Debug, Clone)]
struct Metric<'a> {
    name: &'a str,
    prepare: &'a str,
    cmd: &'a str,
}

impl Metric<'_> {
    // Returns the proper filename for the hyperfine output for this metric.
    fn outfile(&self, project: &str, branch: &str) -> String {
        [branch, "_", self.name, "_", project, ".json"].join("")
    }
}

// Calls hyperfine via system command, and returns all the exit codes for each hyperfine run.
pub fn measure<'a>(
    projects_directory: &PathBuf,
    dbt_branch: &str,
) -> Result<Vec<ExitStatus>, IOError> {
    /*
        Strategy of this function body:
        1. Read all directory names in `projects_directory`
        2. Pair `n` projects with `m` metrics for a total of n*m pairs
        3. Run hyperfine on each project-metric pair
    */

    // To add a new metric to the test suite, simply define it in this list:
    // TODO: This could be read from a config file in a future version.
    let metrics: Vec<Metric> = vec![Metric {
        name: "parse",
        prepare: "rm -rf target/",
        cmd: "dbt parse --no-version-check",
    }];

    fs::read_dir(projects_directory)
        .or_else(|e| Err(IOError::ReadErr(projects_directory.to_path_buf(), Some(e))))?
        .map(|entry| {
            let path = entry
                .or_else(|e| Err(IOError::ReadErr(projects_directory.to_path_buf(), Some(e))))?
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
            let pairs = metrics
                .iter()
                .map(|metric| (path.clone(), project_name.clone(), metric))
                .collect::<Vec<(PathBuf, String, &Metric<'a>)>>();

            Ok(pairs)
        })
        .collect::<Result<Vec<Vec<(PathBuf, String, &Metric<'a>)>>, IOError>>()?
        .concat()
        .iter()
        // run hyperfine on each pairing
        .map(|(path, project_name, metric)| {
            Command::new("hyperfine")
                .current_dir(path)
                // warms filesystem caches by running the command first without counting it.
                // alternatively we could clear them before each run
                .arg("--warmup")
                .arg("1")
                // --min-runs defaults to 10
                .arg("--min-runs")
                .arg("20")
                .arg("--prepare")
                .arg(metric.prepare)
                .arg([metric.cmd, " --profiles-dir ", "../../project_config/"].join(""))
                .arg("--export-json")
                .arg(["../../results/", &metric.outfile(project_name, dbt_branch)].join(""))
                // this prevents hyperfine from capturing dbt's output.
                // Noisy, but good for debugging when tests fail.
                .arg("--show-output")
                .status() // use spawn() here instead for more information
                .or_else(|e| Err(IOError::CommandErr(Some(e))))
        })
        .collect()
}
