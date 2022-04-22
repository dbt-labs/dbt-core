extern crate structopt;

mod calculate;
mod exceptions;
mod fs;
mod types;

use crate::exceptions::RunnerError;
use crate::types::{Calculation, Version};
use std::path::PathBuf;
use structopt::StructOpt;

// This type defines the commandline interface and is generated
// by `derive(StructOpt)`
#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "performance", about = "performance regression testing runner")]
enum Opt {
    #[structopt(name = "model")]
    Model {
        #[structopt(short)]
        version: Version,
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        projects_dir: PathBuf,
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        baselines_dir: PathBuf,
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        tmp_dir: PathBuf,
        #[structopt(short)]
        n_runs: i32,
    },
    #[structopt(name = "sample")]
    Sample {
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        projects_dir: PathBuf,
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        baseline_dir: PathBuf,
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        out_dir: PathBuf,
    },
}

// enables proper useage of exit() in main.
// https://doc.rust-lang.org/std/process/fn.exit.html#examples
//
// This is where all the printing should happen. Exiting happens
// in main, and module functions should only return values.
fn run_app() -> Result<i32, RunnerError> {
    // match what the user inputs from the cli
    match Opt::from_args() {
        // model subcommand
        Opt::Model {
            version,
            projects_dir,
            baselines_dir,
            tmp_dir,
            n_runs,
        } => {
            // note: I tried resolving relative paths here, and I couldn't get it to work.
            // this means the cli requires absolute paths for now.

            // if there are any nonzero exit codes from the hyperfine runs,
            // return the first one. otherwise return zero.
            let baseline = fs::model(version, &projects_dir, &baselines_dir, &tmp_dir, n_runs)?;

            // print the results to the console for viewing in CI
            println!(":: Modeling Results ::");
            let s = serde_json::to_string_pretty(&baseline)
                .or_else(|e| Err(RunnerError::SerializationErr(e)))?;
            println!("{}", s);

            Ok(0)
        }

        // samples performance characteristics from the current commit
        // and compares them against the model for the latest version in this branch
        // prints all sample results and exits with non-zero exit code
        // when a regression is suspected
        Opt::Sample {
            projects_dir,
            baseline_dir,
            out_dir,
        } => {
            // get all the calculations or gracefully show the user an exception
            let calculations = calculate::regressions(&baseline_dir, &projects_dir, &out_dir)?;

            // print all calculations to stdout so they can be easily debugged
            // via CI.
            println!(":: All Calculations ::\n");
            for c in &calculations {
                println!("{:#?}\n", c);
            }

            // filter for regressions
            let regressions: Vec<&Calculation> =
                calculations.iter().filter(|c| c.regression).collect();

            // return a non-zero exit code if there are regressions
            match regressions[..] {
                [] => {
                    println!("congrats! no regressions :)");
                    Ok(0)
                }
                _ => {
                    // print all calculations to stdout so they can be easily
                    // debugged via CI.
                    println!(":: Regressions Found ::\n");
                    for r in regressions {
                        println!("{:#?}\n", r);
                    }
                    Ok(1)
                }
            }
        }
    }
}

fn main() {
    std::process::exit(match run_app() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{}", err);
            1
        }
    });
}
