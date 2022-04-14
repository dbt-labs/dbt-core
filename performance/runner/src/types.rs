use crate::exceptions::{IOError, RunnerError};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;
use std::{cmp, fmt};

// `HyperfineCmd` defines a command that we want to measure with hyperfine
#[derive(Debug, Clone)]
pub struct HyperfineCmd<'a> {
    pub name: &'a str,
    pub prepare: &'a str,
    pub cmd: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Metric {
    pub name: String,
    pub project_name: String,
}

impl FromStr for Metric {
    type Err = RunnerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split: Vec<&str> = s.split(Metric::sep()).collect();
        match &split[..] {
            [name, project] => Ok(Metric {
                name: name.to_string(),
                project_name: project.to_string(),
            }),
            _ => Err(RunnerError::MetricParseFail(s.to_owned())),
        }
    }
}

impl Metric {
    pub fn sep() -> &'static str {
        "___"
    }

    // encodes the metric name and project in the filename for the hyperfine output.
    pub fn filename(&self) -> String {
        format!("{}{}{}.json", self.name, Metric::sep(), self.project_name)
    }
}

// This type exactly matches the type of array elements
// from hyperfine's output. Deriving `Serialize` and `Deserialize`
// gives us read and write capabilities via json_serde.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Measurement {
    pub command: String,
    pub mean: f64,
    pub stddev: f64,
    pub median: f64,
    pub user: f64,
    pub system: f64,
    pub min: f64,
    pub max: f64,
    pub times: Vec<f64>,
}

// This type exactly matches the type of hyperfine's output.
// Deriving `Serialize` and `Deserialize` gives us read and
// write capabilities via json_serde.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Measurements {
    pub results: Vec<Measurement>,
}

// struct representation for "major.minor.patch" version.
// useful for ordering versions to get the latest
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, DeserializeFromStr, SerializeDisplay,
)]
pub struct Version {
    pub major: i32,
    pub minor: i32,
    pub patch: i32,
}

impl FromStr for Version {
    type Err = RunnerError;

    // TODO: right now this only parses versions in the form "1.1.1"
    // but it could also easily also parse the form "v1.1.1" so dropping the v
    // doesn't have to be done in the action.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ints: Vec<i32> = s
            .split(".")
            .map(|x| x.parse::<i32>())
            .collect::<Result<Vec<i32>, <i32 as FromStr>::Err>>()
            .or_else(|_| Err(RunnerError::VersionParseFail(s.to_owned())))?;

        match ints[..] {
            [major, minor, patch] => Ok(Version {
                major: major,
                minor: minor,
                patch: patch,
            }),
            _ => Err(RunnerError::VersionParseFail(s.to_owned())),
        }
    }
}

impl Version {
    #[cfg(test)]
    pub fn new(major: i32, minor: i32, patch: i32) -> Version {
        Version {
            major: major,
            minor: minor,
            patch: patch,
        }
    }
}

// A model for a single project-command pair
// modeling a version at release time will populate a directory with many of these
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Baseline {
    pub version: Version,
    pub metric: Metric,
    pub ts: DateTime<Utc>,
    pub measurement: Measurement,
}

impl PartialOrd for Baseline {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.version.cmp(&other.version))
    }
}

// A JSON structure outputted by the release process that contains
// a history of all previous version baseline measurements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub metric: Metric,
    pub value: f64,
    pub ts: DateTime<Utc>,
}

impl Sample {
    pub fn from_measurement(
        path: &PathBuf,
        measurement: &Measurement,
        ts: DateTime<Utc>,
    ) -> Result<Sample, RunnerError> {
        // `file_name` is boop___proj.json. `file_stem` is boop___proj.
        let filestem = path.file_stem().map_or_else(
            || Err(IOError::BadFilestemError(path.clone())),
            |stem| Ok(stem.to_string_lossy().to_string()),
        )?;

        let metric = Metric::from_str(&filestem)?;

        // TODO use result values not panics
        match &measurement.times[..] {
            [] => panic!("found a sample with no measurement"),
            [x] => Ok(Sample {
                metric: metric,
                value: *x,
                ts: ts,
            }),
            // TODO this is only while we're taking two runs at a time. should be one.
            [x, _] => Ok(Sample {
                metric: metric,
                value: *x,
                ts: ts,
            }),
            _ => panic!("found a sample with too many measurements!"),
        }
    }
}

// The full output from a comparison between runs on the baseline
// and dev branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Calculation {
    pub version: Version,
    pub metric: Metric,
    pub regression: bool,
    pub ts: DateTime<Utc>,
    pub sigma: f64,
    pub mean: f64,
    pub stddev: f64,
    pub threshold: f64,
    pub sample: f64,
}

// This display instance is used to derive Serialize as well via `SerializeDisplay`
impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_version_order() {
        // for each pair, assert that the left one is bigger than the right one
        let pairs = [
            ((1, 0, 0), (0, 20, 0)),
            ((1, 0, 1), (1, 0, 0)),
            ((1, 0, 9), (0, 20, 0)),
            ((1, 0, 9), (0, 0, 4)),
            ((1, 1, 0), (1, 0, 99)),
        ];

        for (big, small) in pairs {
            let bigv = Version::new(big.0, big.1, big.2);
            let smallv = Version::new(small.0, small.1, small.2);
            assert!(cmp::max(bigv, smallv) == bigv)
        }
    }
}
