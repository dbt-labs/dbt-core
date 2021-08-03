extern crate structopt;

mod calculate;
mod measure;

use crate::calculate::Calculation;
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
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

            // print calculations to stdout
            println!(":: All Calculations ::\n");
            for c in &calculations {
                println!("{:#?}\n", c);
            }
            println!("");

            // write calculations to file so it can be downloaded as an artifact
            let json_calcs = serde_json::to_string(&calculations)
                .expect("failed to serialize calculations to json");
            
            let outfile = &mut results_dir.into_os_string();
            outfile.push("/final_calculations.json");

            let mut f = File::create(outfile).expect("Unable to create file");
            f.write_all(json_calcs.as_bytes()).expect("unable to write data");

            // filter for regressions
            let regressions: Vec<Calculation> = calculations
                .into_iter()
                .filter(|c| c.regression)
                .collect();

            // exit with non zero exit code if there are regressions
            match regressions[..] {
                [] => println!("congrats! no regressions :)"),
                _ => {
                    println!(":: Regressions Found ::\n");
                    println!("");
                    for r in regressions {
                        println!("{:#?}\n", r);
                    }
                    println!("");
                    exit(1)
                }
            }
        },

    }
}
