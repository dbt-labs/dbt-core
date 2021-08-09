extern crate structopt;

mod calculate;
mod exceptions;
mod measure;

use crate::calculate::Calculation;
use crate::exceptions::CalculateError;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use structopt::StructOpt;

// This type defines the commandline interface and is generated
// by `derive(StructOpt)`
#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "performance", about = "performance regression testing runner")]
enum Opt {
    #[structopt(name = "measure")]
    Measure {
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        projects_dir: PathBuf,
        #[structopt(short)]
        branch_name: String,
    },
    #[structopt(name = "calculate")]
    Calculate {
        #[structopt(parse(from_os_str))]
        #[structopt(short)]
        results_dir: PathBuf,
    },
}

// enables proper useage of exit() in main.
// https://doc.rust-lang.org/std/process/fn.exit.html#examples
//
// This is where all the printing should happen. Exiting happens
// in main, and module functions should only return values.
fn run_app() -> Result<i32, CalculateError> {
    // match what the user inputs from the cli
    match Opt::from_args() {
        // measure subcommand
        Opt::Measure {
            projects_dir,
            branch_name,
        } => {
            // if there are any nonzero exit codes from the hyperfine runs,
            // return the first one. otherwise return zero.
            measure::measure(&projects_dir, &branch_name)
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))?
                .iter()
                .map(|status| status.code())
                .flatten()
                .filter(|code| *code != 0)
                .collect::<Vec<i32>>()
                .get(0)
                .map_or(Ok(0), |x| {
                    println!("Main: a child process exited with a nonzero status code.");
                    Ok(*x)
                })
        }

        // calculate subcommand
        Opt::Calculate { results_dir } => {
            // get all the calculations or gracefully show the user an exception
            let calculations = calculate::regressions(&results_dir)?;

            // print all calculations to stdout so they can be easily debugged
            // via CI.
            println!(":: All Calculations ::\n");
            for c in &calculations {
                println!("{:#?}\n", c);
            }

            // indented json string representation of the calculations array
            let json_calcs = serde_json::to_string_pretty(&calculations)
                .expect("Main: Failed to serialize calculations to json");

            // create the empty destination file, and write the json string
            let outfile = &mut results_dir.into_os_string();
            outfile.push("/final_calculations.json");
            let mut f = File::create(outfile).expect("Main: Unable to create file");
            f.write_all(json_calcs.as_bytes())
                .expect("Main: Unable to write data");

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
