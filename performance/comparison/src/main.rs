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

struct Regression {
    threshold: f64,
    difference: f64,
    base: Measurement,
    latest: Measurement,
}

fn regression(base: &Measurement, latest: &Measurement) -> Option<Regression> {
    let threshold = 1.05; // 5% regression threshold
    let difference = latest.median / base.median;
    if difference > threshold {
        Some(Regression {
            threshold: threshold,
            difference: difference,
            base: base.clone(),
            latest: latest.clone(),
        })
    } else {
        None
    }
}

struct MeasurementGroup {
    branch: String,
    run: String,
    measurement: Measurement,
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
    let x: Result<Vec<MeasurementGroup>> = result_files
        .into_iter()
        .map(|f| f.unwrap().path())
        .filter(|filename| {
            filename
                .extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext.ends_with("json"))
        })
        .map(|filename| {
            println!("{:?}", filename);
            let contents = fs::read_to_string(&filename).unwrap();
            let measurements: Result<Measurements> = serde_json::from_str(&contents);
            let results = measurements.map(|x| x.results[0].clone());
            let filepath = filename.into_os_string().into_string().unwrap();
            let parts: Vec<&str> = filepath.split("_").collect();

            // the way we're running these, the files will each contain exactly one measurement
            results.map(|r| MeasurementGroup {
                branch: parts[0].to_owned(),
                run: parts[1..].join(""),
                measurement: r.clone(),
            })
        })
        .collect();

    // TODO
    // - group by project and metric
    // - each group will have 2 measurements for each branch
    // - perfrom regressions on each pair

    // TODO exit(1) when Regression is present
    match x {
        Err(e) => panic!("{}", e),
        Ok(_) => (),
    }

    println!("{:?}", x);
}
