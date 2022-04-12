use crate::exceptions::RunnerError;
use crate::fs;
use crate::types::*;
use std::collections::HashMap;
use std::path::PathBuf;

// calculates a single regression for a matching sample-baseline pair.
// does not validate that the sample metric and baseline metric match.
fn calculate_regression(sample: &Sample, baseline: &Baseline, sigma: f64) -> Calculation {
    let model = baseline.measurement.clone();
    let threshold = model.mean + sigma * model.stddev;

    Calculation {
        version: baseline.version,
        metric: baseline.metric.clone(),
        regression: sample.value > threshold,
        ts: sample.ts.clone(),
        sigma: sigma,
        mean: model.mean,
        stddev: model.stddev,
        threshold: threshold,
        sample: sample.value,
    }
}

// Top-level function. Given a path for the result directory, call the above
// functions to compare and collect calculations. Calculations include all samples
// regardless of whether they passed or failed.
pub fn regressions(
    baseline_dir: &PathBuf,
    projects_dir: &PathBuf,
    tmp_dir: &PathBuf,
) -> Result<Vec<Calculation>, RunnerError> {
    // finds the latest version availble in the baseline directory
    let latest_version = fs::latest_version_from(baseline_dir)?;

    // the baseline we want to target is the latest one available in this branch
    let mut target_baseline_dir = baseline_dir.clone();
    target_baseline_dir.push(latest_version.to_string());

    // these are all the metrics for all available baselines from the target version
    let baselines: Vec<Baseline> = fs::from_json_files::<Baseline>(&target_baseline_dir)?
        .into_iter()
        .map(|(_, x)| x)
        .collect();

    // check that we have at least one baseline.
    // If this error is returned, in all liklihood the runner cannot read the existing baselines
    // for some reason. Every branch is expected to have baselines from when they were branched off of main.
    if baselines.is_empty() {
        return Err(RunnerError::NoVersionedBaselineData(
            target_baseline_dir.clone(),
        ));
    }

    let samples: Vec<Sample> = fs::take_samples(projects_dir, tmp_dir)?;

    // turn samples into a map so they can be easily matched to baseline data
    let m_samples: HashMap<Metric, Sample> =
        samples.into_iter().map(|x| (x.metric.clone(), x)).collect();

    // match all baseline metrics to samples taken and calculate regressions with a 3 sigma threshold
    let calculations: Vec<Calculation> = baselines
        .into_iter()
        .map(|baseline| {
            m_samples
                .get(&baseline.metric)
                .ok_or_else(|| RunnerError::BaselineMetricNotSampled(baseline.metric.clone()))
                .map(|sample| calculate_regression(&sample, &baseline, 3.0))
        })
        .collect::<Result<Vec<Calculation>, RunnerError>>()?;

    Ok(calculations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::prelude::*;

    #[test]
    fn detects_3sigma_regression() {
        let metric = Metric {
            name: "test".to_owned(),
            project_name: "detects 3 sigma".to_owned(),
        };

        let baseline = Baseline {
            version: Version::new(9, 9, 9),
            metric: metric.clone(),
            ts: Utc::now(),
            measurement: Measurement {
                command: "some command".to_owned(),
                mean: 1.00,
                stddev: 0.1,
                median: 1.00,
                user: 1.00,
                system: 1.00,
                min: 0.00,
                max: 2.00,
                times: vec![],
            },
        };

        let sample = Sample {
            metric: metric,
            value: 1.31,
            ts: Utc::now(),
        };

        let calculation = calculate_regression(
            &sample, &baseline, 3.0, // 3 sigma
        );

        // expect a regression for the mean being outside the 3 sigma
        println!("{:#?}", calculation);
        assert!(calculation.regression);
    }

    #[test]
    fn passes_near_3sigma() {
        let metric = Metric {
            name: "test".to_owned(),
            project_name: "passes near 3 sigma".to_owned(),
        };

        let baseline = Baseline {
            version: Version::new(9, 9, 9),
            metric: metric.clone(),
            ts: Utc::now(),
            measurement: Measurement {
                command: "some command".to_owned(),
                mean: 1.00,
                stddev: 0.1,
                median: 1.00,
                user: 1.00,
                system: 1.00,
                min: 0.00,
                max: 2.00,
                times: vec![],
            },
        };

        let sample = Sample {
            metric: metric,
            value: 1.29,
            ts: Utc::now(),
        };

        let calculation = calculate_regression(
            &sample, &baseline, 3.0, // 3 sigma
        );

        // expect no regressions
        println!("{:#?}", calculation);
        assert!(!calculation.regression);
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
