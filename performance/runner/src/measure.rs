extern crate structopt;

use crate::exceptions::IOError;
use std::path::PathBuf;
use std::process::{Command, ExitStatus};
use std::fs;
use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "performance", about = "performance regression testing runner")]
enum Performance {
    #[structopt(name = "measure")]
    Measure {
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        projects_dir: PathBuf,
        #[structopt(short)]
        branch_name: bool,
    },
    #[structopt(name = "compare")]
    Compare {
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        results_dir: PathBuf,
    },
}

#[derive(Debug, Clone)]
struct Metric<'a> {
    name: &'a str,
    prepare: &'a str,
    cmd: &'a str,
}

impl Metric<'_> {
    fn outfile(&self, project: &str, branch: &str) -> String {
        [branch, "_", self.name, "_", project, ".json"].join("")
    }
}

// calls hyperfine via system command, and returns result of runs
pub fn measure(projects_directory: &PathBuf, dbt_branch: &str) -> Result<Vec<ExitStatus>, IOError> {
    // to add a new metric to the test suite, simply define it in this list:
    // TODO read from some config file?
    let metrics: Vec<Metric> = vec![
        Metric {
            name: "parse",
            prepare: "rm -rf target/",
            cmd: "dbt parse --no-version-check",
        },
    ];

    // run hyperfine on each target project in the directory
    fs::read_dir(projects_directory)
        .or_else(|e| Err(IOError::ReadErr(projects_directory.to_path_buf(), Some(e))))?
        .map(|entry| {
            metrics
                .clone()
                .into_iter()
                // for each entry-metric pair
                .map(|metric| {
                    let ent = entry
                        .as_ref()
                        .or_else(|e| Err(IOError::ReadErr(projects_directory.to_path_buf(), None)))?; // TODO change to Some(e)

                    let path: PathBuf = ent.path();
                    let project_name: &str = path
                        .file_name()
                        .ok_or_else(|| IOError::MissingFilenameErr(path.clone().to_path_buf()))
                        .and_then(|x| x.to_str().ok_or_else(|| IOError::FilenameNotUnicodeErr(path.clone().to_path_buf())))?;
                    
                    Command::new("hyperfine")
                        .current_dir(&path)
                        // warms filesystem caches by running the command first without counting it.
                        // alternatively we could clear them before each run
                        .arg("--warmup")
                        .arg("1")
                        .arg("--prepare")
                        .arg(metric.prepare)
                        .arg([metric.cmd, " --profiles-dir ", "../../project_config/"].join(""))
                        .arg("--export-json")
                        .arg(
                            ["../../results/", &metric.outfile(&project_name, &dbt_branch)].join(""),
                        )
                        // this prevents capture dbt output. Noisy, but good for debugging when tests fail.
                        .arg("--show-output")
                        .status() // use spawn() here instead for more information
                        .or_else(|e| Err(IOError::CommandErr(Some(e))))
                })
                .collect::<Vec<Result<ExitStatus, IOError>>>()
        })
        .flatten()
        .collect()
}
