use serde::{Deserialize, Serialize};


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
    let data = r#"{
"results": [
    {
    "command": "dbt parse --no-version-check",
    "mean": 3.050361809595,
    "stddev": 0.04615108237908544,
    "median": 3.034926999095,
    "user": 2.7580889199999996,
    "system": 0.22864483500000002,
    "min": 3.005364834595,
    "max": 3.145752726595,
    "times": [
        3.023607223595,
        3.0423504165949997,
        3.145752726595,
        3.114517602595,
        3.062246401595,
        3.051930277595,
        3.027503581595,
        3.013381462595,
        3.005364834595,
        3.016963568595
    ]
    }
]
}"#;
    
    let measurements: Measurements = serde_json::from_str(data).unwrap();

    println!("{:?}", measurements);
}