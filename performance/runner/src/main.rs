extern crate structopt;

mod calculate;
mod measure;

use std::path::PathBuf;
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

fn main() {
    match Opt::from_args() {
        Opt::Measure { projects_dir, branch_name } => {
            measure::proper_exit(measure::measure(&projects_dir, &branch_name).unwrap())
        },
        Opt::Calculate { results_dir } => {
            calculate::exit_properly(&calculate::regressions(&results_dir).unwrap())
        },
    }
}
