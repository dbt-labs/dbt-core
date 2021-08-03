extern crate structopt;

mod calculate;
mod measure;

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

fn main() {
    match Opt::from_args() {
        Opt::Measure { projects_dir, branch_name } => {
            gracefully_exit_or(
                &measure::proper_exit,
                measure::measure(&projects_dir, &branch_name)
            )
        },
        Opt::Calculate { results_dir } => {
            gracefully_exit_or(
                &calculate::exit_properly,
                calculate::regressions(&results_dir)
            )
        },
    }
}
