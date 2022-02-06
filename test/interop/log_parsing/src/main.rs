
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead};
use walkdir::WalkDir;


// Applies schema tests to file input
// if these fail, we either have a problem in dbt that needs to be resolved
// or we have changed our interface and the log_version should be bumped in dbt,
// modeled appropriately here, and publish new docs for the new log_version.
fn main() -> Result<(), Box<dyn Error>> {
    let log_name = "dbt.log";
    let path = env::var("LOG_DIR").expect("must pass absolute log path to tests with env var `LOG_DIR=/logs/live/here/`");
    
    println!("Looking for files named `{}` in {}", log_name, path);
    let lines: Vec<String> = get_input(&path, log_name)?;
    println!("collected {} log lines.", lines.len());

    println!("");

    println!("testing type-level schema compliance by deserializing each line...");
    let log_lines: Vec<LogLine> = deserialized_input(&lines)
        .map_err(|e| format!("schema test failure: json doesn't match type definition\n{}", e))?;
    println!("Done.");

    println!("");
    println!("because we skip non-json log lines, there are {} collected values to test.", log_lines.len());
    println!("");

    // make sure when we read a string in then output it back to a string the two strings
    // contain all the same key-value pairs.
    println!("testing serialization loop to make sure all key-value pairs are accounted for");
    test_deserialize_serialize_is_unchanged(&lines);
    println!("Done.");

    println!("");

    // make sure each log_line contains the values we expect
    println!("testing that the field values in each log line are expected");
    for log_line in log_lines {
        log_line.value_test()
    }
    println!("Done.");

    Ok(())
}


// each nested type of LogLine should define its own value_test function
// that asserts values are within an expected set of values when possible.
trait ValueTest {
    fn value_test(&self) -> ();
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct LogLine {
    log_version: isize,
    r#type: String,
    code: String,
    #[serde(with = "custom_date_format")]
    ts: DateTime<Utc>,
    pid: isize,
    msg: String,
    level: String,
    invocation_id: String,
    thread_name: String,
    data: serde_json::Value,      // TODO be more specific
}

impl ValueTest for LogLine {
    fn value_test(&self){
        assert_eq!(
            self.log_version, 2,
            "The log version changed. Be sure this was intentional."
        );

        assert_eq!(
            self.r#type,
            "log_line".to_owned(),
            "The type value has changed. If this is intentional, bump the log version"
        );

        assert!(
            ["debug", "info", "warn", "error"]
                .iter()
                .any(|level| **level == self.level),
            "log level had unexpected value {}",
            self.level
        );
    }
}

// logs output timestamps like this: "2021-11-30T12:31:04.312814Z"
// which is so close to the default except for the decimal.
// this requires handling the date with "%Y-%m-%dT%H:%M:%S%.6f" which requires this
// boilerplate-looking module.
mod custom_date_format {
    use chrono::{NaiveDateTime, DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.6fZ";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?, Utc))
    }
}

// finds all files in any subdirectory of this path with this name. returns the contents
// of each file line by line as one continuous structure. No distinction between files.
fn get_input(path: &str, file_name: &str) -> Result<Vec<String>, String> {
    WalkDir::new(path)
        .follow_links(true)
        .into_iter()
        // filters out all the exceptions encountered on this walk silently
        .filter_map(|e| e.ok())
        // walks through each file and returns the contents if the filename matches
        .filter_map(|e| {
            let f_name = e.file_name().to_string_lossy();
            if f_name.ends_with(file_name) {
                let contents = File::open(e.path())
                    .map_err(|e| format!("Something went wrong opening the log file {}\n{}", f_name, e))
                    .and_then(|file| {
                        io::BufReader::new(file)
                            .lines()
                            .map(|l| {
                                l.map_err(|e| format!("Something went wrong reading lines of the log file {}\n{}", f_name, e))
                            })
                            .collect::<Result<Vec<String>, String>>()
                    });
                    
                Some(contents)
            } else {
                None
            }
        })
        .collect::<Result<Vec<Vec<String>>, String>>()
        .map(|vv| vv.concat())
}

// attemps to deserialize the strings into LogLines. If the string isn't valid
// json it skips it instead of failing. This is so that any tests that generate
// non-json logs won't break the schema test.
fn deserialized_input(log_lines: &[String]) -> serde_json::Result<Vec<LogLine>> {
    log_lines
        .into_iter()
        // if the log line isn't valid json format, toss it
        .filter(|log_line| serde_json::from_str::<serde_json::Value>(log_line).is_ok())
        // attempt to deserialize into our LogLine type
        .map(|log_line| serde_json::from_str::<LogLine>(log_line))
        .collect()
}

// turn a String into a LogLine and back into a String returning both Strings so
// they can be compared
fn deserialize_serialize_loop(
    log_lines: &[String],
) -> serde_json::Result<Vec<(String, String)>> {
    log_lines
        .into_iter()
        .map(|log_line| {
            serde_json::from_str::<LogLine>(log_line).and_then(|parsed| {
                serde_json::to_string(&parsed).map(|json| (log_line.clone(), json))
            })
        })
        .collect()
}

// make sure when we read a string in then output it back to a string the two strings
// contain all the same key-value pairs.
fn test_deserialize_serialize_is_unchanged(lines: &[String]) {
    let objects: Result<Vec<(serde_json::Value, serde_json::Value)>, serde_json::Error> =
        deserialize_serialize_loop(lines).and_then(|v| {
            v.into_iter()
                .map(|(s0, s1)| {
                    serde_json::from_str::<serde_json::Value>(&s0).and_then(|s0v| {
                        serde_json::from_str::<serde_json::Value>(&s1).map(|s1v| (s0v, s1v))
                    })
                })
                .collect()
        });

    match objects {
        Err(e) => assert!(false, "{}", e),
        Ok(v) => {
            for pair in v {
                match pair {
                    (
                        serde_json::Value::Object(original),
                        serde_json::Value::Object(looped),
                    ) => {
                        // looping through each key of each json value gives us meaningful failure messages
                        // instead of "this big string" != "this other big string"
                        for (key, value) in original.clone() {
                            let looped_val = looped.get(&key);
                            assert_eq!(
                                looped_val,
                                Some(&value),
                                "original key value ({}, {}) expected in re-serialized result",
                                key,
                                value
                            )
                        }
                        for (key, value) in looped.clone() {
                            let original_val = original.get(&key);
                            assert_eq!(
                                original_val,
                                Some(&value),
                                "looped key value ({}, {}) not found in original result",
                                key,
                                value
                            )
                        }
                    }
                    _ => assert!(false, "not comparing json objects"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    const LOG_LINE: &str = r#"{"code": "Z023", "data": {"stats": {"error": 0, "pass": 3, "skip": 0, "total": 3, "warn": 0}}, "invocation_id": "f1e1557c-4f9d-4053-bb50-572cbbf2ca64", "level": "info", "log_version": 2, "msg": "Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3", "pid": 75854, "thread_name": "MainThread", "ts": "2021-12-03T01:32:38.334601Z", "type": "log_line"}"#;

    #[test]
    fn test_basic_loop() {
        assert!(deserialize_serialize_loop(&[LOG_LINE.to_owned()]).is_ok())
    }

    #[test]
    fn test_values() {
        assert!(deserialized_input(&[LOG_LINE.to_owned()]).map(|v| {
            v.into_iter().map(|ll| ll.value_test())
        }).is_ok())
    }

    #[test]
    fn test_values_loop() {
        test_deserialize_serialize_is_unchanged(&[LOG_LINE.to_owned()]);
    }
}
