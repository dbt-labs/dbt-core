extern crate structopt;

mod calculate;
mod measure;

use crate::calculate::Calculation;
use std::fmt::Display;
use std::path::PathBuf;
use std::process::exit;
use structopt::StructOpt;

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

fn gracefully_exit_or<A, B, E: Display>(f: &dyn Fn(A) -> B, r: Result<A, E>) -> B {
    match r {
        Err(e) => {
            println!("{}", e);
            exit(1)
        },
        Ok(x) => f(x),
    }
}

// This is where all the printing, exiting, and error displaying 
// should happen. Module functions should only return values.
fn main() {
    match Opt::from_args() {

        Opt::Measure { projects_dir, branch_name } => {
            let statuses = gracefully_exit_or(
                &|x| x,
                measure::measure(&projects_dir, &branch_name)
            );

            for status in statuses {
                match status.code() {
                    None => (),
                    Some(0) => (),
                    // if any child command exited with a non zero status code, exit with the same one here.
                    Some(nonzero) => {
                        println!("a child process exited with a nonzero status code.");
                        exit(nonzero)
                    },
                }
            }
        },

        Opt::Calculate { results_dir } => {
            let calculations = gracefully_exit_or(
                &|x| x,
                calculate::regressions(&results_dir)
            );

            println!(":: All Calculations ::\n");
            for c in &calculations {
                println!("{:#?}", c);
            }
            println!("");

            let regressions: Vec<Calculation> = calculations
                .into_iter()
                .filter(|c| c.regression)
                .collect();

            match regressions[..] {
                [] => println!("congrats! no regressions :)"),
                _ => {
                    println!(":: Regressions Found ::\n");
                    for r in regressions {
                        println!("{:#?}", r);
                    }
                    println!("");
                    exit(1)
                }
            }
        },

    }
}
