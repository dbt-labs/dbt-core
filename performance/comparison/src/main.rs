use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::path::Path;
use std::process::exit;
use std::{env, fs};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Measurements {
    results: Vec<Measurement>,
}

#[derive(Debug, Clone, PartialEq)]
struct Regression {
    threshold: f64,
    difference: f64,
    baseline: Measurement,
    latest: Measurement,
}

#[derive(Debug, Clone, PartialEq)]
struct MeasurementGroup {
    version: String,
    run: String,
    measurement: Measurement,
}

fn regression(baseline: &Measurement, latest: &Measurement) -> Option<Regression> {
    let threshold = 1.05; // 5% regression threshold
    let difference = latest.median / baseline.median;
    if difference > threshold {
        Some(Regression {
            threshold,
            difference,
            baseline: baseline.clone(),
            latest: latest.clone(),
        })
    } else {
        None
    }
}

fn measurements_from_files(
    results_directory: &Path,
) -> std::result::Result<Vec<Measurements>, std::io::Error> {
    let result_files = fs::read_dir(results_directory)?;

    result_files
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
            let contents = fs::read_to_string(&filename);
            contents.map(|c| serde_json::from_str::<Measurements>(&c).unwrap()) //TODO rm unwrap!
        })
        .collect()
}

fn detect_regressions(measurements: Vec<Measurements>) -> Vec<Regression> {
    panic!()
}

fn main() {
    // TODO args lib
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("please provide the results directory");
        exit(1);
    }
    let results_directory = &args[1];

    let foo = measurements_from_files(Path::new(&results_directory));
    foo.map(|x| detect_regressions(x));

    // let measurements: Result<Vec<(&str, Vec<MeasurementGroup>)>> = result_files
    //     .into_iter()
    //     .map(|f| f.unwrap().path())
    //     .filter(|filename| {
    //         filename
    //             .extension()
    //             .and_then(|ext| ext.to_str())
    //             .map_or(false, |ext| ext.ends_with("json"))
    //     })
    //     .map(|filename| {
    //         println!("{:?}", filename);
    //         let contents = fs::read_to_string(&filename).unwrap();
    //         let measurements: Result<Measurements> = serde_json::from_str(&contents);
    //         let results = measurements.map(|x| x.results[0].clone());
    //         let filepath = filename.into_os_string().into_string().unwrap();
    //         let parts: Vec<&str> = filepath.split("_").collect();

    //         // the way we're running these, the files will each contain exactly one measurement
    //         results.map(|r| MeasurementGroup {
    //             version: parts[0].to_owned(),
    //             run: parts[1..].join(""),
    //             measurement: r.clone(),
    //         })
    //     })
    //     .collect();

    // TODO
    // - group by project and metric
    // - each group will have 2 measurements for each branch
    // - perform regressions on each pair
    // latest_parse_02_mini_project_dont_merge.json
    // latest_parse_01_mini_project_dont_merge.json
    // baseline_parse_02_mini_project_dont_merge.json
    // baseline_parse_01_mini_project_dont_merge.json
    // command, project -> 2 version
    // let x: Vec<(&str, Vec<MeasurementGroup>)>;

    // TODO exit(1) when Regression is present
    // match foo {
    //     Err(&e) => panic!("{}", &e),
    //     Ok(_) => (),
    // }
    foo.clone().map(|f| println!("{:?}", f)).unwrap();
}
