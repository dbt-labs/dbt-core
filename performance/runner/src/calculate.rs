use crate::exceptions::RunnerError;
use crate::measure;
use crate::types::*;
use chrono::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// TODO find an alternative to all this cloning
fn calculate_regressions(samples: &[Sample], baseline: Baseline, sigma: f64) -> Vec<Calculation> {
    let m_samples: HashMap<Metric, (f64, DateTime<Utc>)> = samples
        .into_iter()
        .map(|x| (x.metric.clone(), (x.value, x.ts)))
        .collect();

    baseline
        .models
        .clone()
        .into_iter()
        .filter_map(|metric_model| {
            let model = metric_model.measurement.clone();
            m_samples.get(&metric_model.metric).map(|(value, ts)| {
                let threshold = model.mean + sigma * model.stddev;
                Calculation {
                    version: baseline.version,
                    metric: metric_model.metric,
                    regression: *value > threshold,
                    ts: *ts,
                    sigma: sigma,
                    mean: model.mean,
                    stddev: model.stddev,
                    threshold: threshold,
                }
            })
        })
        .collect()
}

// Top-level function. Given a path for the result directory, call the above
// functions to compare and collect calculations. Calculations include all samples
// regardless of whether they passed or failed.
pub fn regressions(
    baseline_dir: &PathBuf,
    projects_dir: &PathBuf,
    tmp_dir: &PathBuf,
) -> Result<Vec<Calculation>, RunnerError> {
    let baselines: Vec<Baseline> = measure::from_json_files::<Baseline>(Path::new(&baseline_dir))?
        .into_iter()
        .map(|(_, x)| x)
        .collect();
    let samples: Vec<Sample> = measure::take_samples(projects_dir, tmp_dir)?;

    // this is the baseline to compare these samples against
    let baseline: Baseline = match &baselines[..] {
        [] => panic!("no baselines found in dir"),
        [x, ..] => baselines.clone().into_iter().fold(x.clone(), |max, next| {
            if max.version >= next.version {
                max
            } else {
                next
            }
        }),
    };

    // calculate regressions with a 3 sigma threshold
    Ok(calculate_regressions(&samples, baseline, 3.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_3sigma_regression() {
        let metric = Metric {
            name: "test".to_owned(),
            project_name: "detects 3 sigma".to_owned(),
        };

        let measurement = Measurement {
            command: "some command".to_owned(),
            mean: 1.00,
            stddev: 0.1,
            median: 1.00,
            user: 1.00,
            system: 1.00,
            min: 0.00,
            max: 2.00,
            times: vec![],
        };

        let baseline_metric = MetricModel {
            metric: metric.clone(),
            ts: Utc::now(),
            measurement: measurement,
        };

        let baseline = Baseline {
            version: Version::new(9, 9, 9),
            models: vec![baseline_metric],
        };

        let sample = Sample {
            metric: metric.clone(),
            value: 1.31,
            ts: Utc::now(),
        };

        let calculations = calculate_regressions(
            &[sample],
            baseline,
            3.0, // 3 sigma
        );

        let regressions: Vec<&Calculation> =
            calculations.iter().filter(|calc| calc.regression).collect();

        // expect one regression for the mean being outside the 3 sigma
        println!("{:#?}", regressions);
        assert_eq!(regressions.len(), 1);
    }

    #[test]
    fn passes_near_3sigma() {
        let metric = Metric {
            name: "test".to_owned(),
            project_name: "passes near 3 sigma".to_owned(),
        };

        let measurement = Measurement {
            command: "some command".to_owned(),
            mean: 1.00,
            stddev: 0.1,
            median: 1.00,
            user: 1.00,
            system: 1.00,
            min: 0.00,
            max: 2.00,
            times: vec![],
        };

        let baseline_metric = MetricModel {
            metric: metric.clone(),
            ts: Utc::now(),
            measurement: measurement,
        };

        let baseline = Baseline {
            version: Version::new(9, 9, 9),
            models: vec![baseline_metric],
        };

        let sample = Sample {
            metric: metric.clone(),
            value: 1.29,
            ts: Utc::now(),
        };

        let calculations = calculate_regressions(
            &[sample],
            baseline,
            3.0, // 3 sigma
        );

        let regressions: Vec<&Calculation> =
            calculations.iter().filter(|calc| calc.regression).collect();

        // expect no regressions
        println!("{:#?}", regressions);
        assert!(regressions.is_empty());
    }

    // The serializer and deserializer are custom implementations
    // so they should be tested that they match.
    #[test]
    fn version_serialize_loop() {
        let v = Version {
            major: 1,
            minor: 2,
            patch: 3,
        };
        let v2 = serde_json::from_str::<Version>(&serde_json::to_string_pretty(&v).unwrap());
        assert_eq!(v, v2.unwrap());
    }
}
