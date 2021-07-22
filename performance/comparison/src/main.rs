use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::path::Path;
use std::process::exit;
use std::{env, fs};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Measurement {
    command: String,
    mean: f64,
    stddev: f64,
    median: f64,
    user: f64,
    system: f64,
    min: f64,
    max: f64,
    times: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Measurements {
    results: Vec<Measurement>,
}

fn main() {
    // TODO args lib
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("please provide the results directory");
        exit(1);
    }
    let results_directory = &args[1];

    let path = Path::new(&results_directory);
    let result_files = fs::read_dir(path).unwrap();
    let x: Result<Vec<Measurements>> = result_files
        .into_iter()
        .map(|f| {
            f.unwrap().path()
        })
        .filter(|filename| {
            filename
                .extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext.ends_with("json"))
        })
        .map(|filename| {
            println!("{:?}", filename);
            let contents = fs::read_to_string(filename).unwrap();
            serde_json::from_str::<Measurements>(&contents)
        })
        .collect();

    println!("{:?}", x);
}
