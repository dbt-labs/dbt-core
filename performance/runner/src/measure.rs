extern crate structopt;

use std::path::PathBuf;
use std::process::{Command, ExitStatus};
use std::{fs, io};
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
pub fn measure(projects_directory: &PathBuf, dbt_branch: &str) -> Result<Vec<ExitStatus>, io::Error> {
    // to add a new metric to the test suite, simply define it in this list:
    // TODO read from some config file?
    let metrics: Vec<Metric> = vec![
        Metric {
            name: "parse",
            prepare: "rm -rf target/",
            cmd: "dbt parse --no-version-check",
        },
    ];

    // list out all projects
    let project_entries = fs::read_dir(projects_directory).unwrap();

    project_entries
        .map(|entry| {
            metrics
                .clone()
                .into_iter()
                .map(|metric| {
                    let path = entry.as_ref().unwrap().path();
                    let project_name = &path.file_name().and_then(|x| x.to_str()).unwrap();

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
                            ["../../results/", &metric.outfile(project_name, &dbt_branch)].join(""),
                        )
                        // this prevents capture dbt output. Noisy, but good for debugging when tests fail.
                        .arg("--show-output")
                        .status() // use spawn() here instead for more information
                })
                .collect::<Vec<Result<ExitStatus, io::Error>>>()
        })
        .flatten()
        .collect()
}
