extern crate structopt;

mod calculate;
mod exceptions;
mod measure;

use crate::calculate::Calculation;
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::exit;
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

// Given a result with a displayable error, map the value if there is one, or
// print the human readable error message and exit with exit code 1.
fn map_or_gracefully_exit<A, B, E: Display>(f: &dyn Fn(A) -> B, r: Result<A, E>) -> B {
    r.map_or_else(
        |e| {
            println!("{}", e);
            exit(1)
        },
        f,
    )
}

// This is where all the printing, exiting, and error displaying
// should happen. Module functions should only return values.
fn main() {
    // match what the user inputs from the cli
    match Opt::from_args() {
        // measure subcommand
        Opt::Measure {
            projects_dir,
            branch_name,
        } => {
            // get all the statuses of the hyperfine runs or
            // gracefully show the user an exception
            let statuses =
                map_or_gracefully_exit(&|x| x, measure::measure(&projects_dir, &branch_name));

            // only exit with exit code 0 if all hyperfine runs were
            // dispatched correctly.
            for status in statuses {
                match status.code() {
                    None => (),
                    Some(0) => (),
                    // if any child command exited with a non zero status code, exit with the same one here.
                    Some(nonzero) => {
                        println!("a child process exited with a nonzero status code.");
                        exit(nonzero)
                    }
                }
            }
        }

        // calculate subcommand
        Opt::Calculate { results_dir } => {
            // get all the calculations or gracefully show the user an exception
            let calculations = map_or_gracefully_exit(&|x| x, calculate::regressions(&results_dir));

            // print all calculations to stdout so they can be easily debugged
            // via CI.
            println!(":: All Calculations ::\n");
            for c in &calculations {
                println!("{:#?}\n", c);
            }
            println!("");

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

            // exit with non zero exit code if there are regressions
            match regressions[..] {
                [] => println!("congrats! no regressions :)"),
                _ => {
                    // print all calculations to stdout so they can be easily debugged
                    // via CI.
                    println!(":: Regressions Found ::\n");
                    println!("");
                    for r in regressions {
                        println!("{:#?}\n", r);
                    }
                    println!("");
                    exit(1)
                }
            }
        }
    }
}
