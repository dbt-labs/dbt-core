use std::{env, fs, io};
use std::path::Path;
use std::process::{Command, ExitStatus, exit};


#[derive(Debug, Clone)]
struct Metric<'a> {
    name: &'a str,
    prepare: &'a str,
    cmd: &'a str,
}

impl Metric<'_> {
    fn outfile(&self, project: &str) -> String {
        [self.name, "_", project, ".json"].join("")
    }
}

fn main() {
    // TODO args lib
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("please provide the target projects directory");
        exit(1);
    }
    let projects_directory = &args[1];

    // to add a new metric to the test suite, simply define it in this list:
    let metrics: Vec<Metric> = vec![
        Metric { name:"parse", prepare: "rm -rf target/", cmd: "dbt parse --no-version-check" },
    ];

    // list out all projects
    let path = Path::new(&projects_directory);
    let project_entries = fs::read_dir(path).unwrap();

    let results: Result<Vec<ExitStatus>, io::Error > = project_entries.map(|entry| {
        metrics.clone().into_iter().map(|metric| {
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
                .arg([metric.cmd, " --profiles-dir ", &projects_directory, "/../project_config/"].join(""))
                .arg("--export-json")
                .arg([&projects_directory, "/", &metric.outfile(project_name)].join(""))
                // this prevents capture dbt output. Noisy, but good for debugging when tests fail.
                .arg("--show-output")
                .status() // use spawn() here instead for more information
        }).collect::<Vec<Result<ExitStatus, io::Error>>>()
    }).flatten().collect();

    // only exit with status code 0 if everything ran as expected
    match results {
        // if dispatch of any of the commands failed, panic with that error
        Err(e) => panic!("{:?}", e),
        Ok(statuses) => {
            for status in statuses {
                match status.code() {
                    None => (),
                    Some(0) => (),
                    // if any child command exited with a non zero status code, exit with the same one here.
                    Some(nonzero) => exit(nonzero),
                }
            }
            // everything ran as expected
            exit(0);
        },
    }
}
