
// main doesn't do anything. Just run the tests
fn main() {}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use std::env;
    use std::fs::File;
    use std::io::{self, BufRead};
    use std::path::Path;

    //
    trait ValueTest {
        fn value_test(&self) -> ();
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct LogLine {
        log_version: isize,
        r#type: String,
        code: String,
        ts: DateTime<Utc>,
        pid: isize,
        msg: String,
        level: String,
        invocation_id: String,
        thread_name: String,
        data: serde_json::Value,      // TODO be more specific
        node_info: serde_json::Value, // TODO be more specific
    }

    impl ValueTest for LogLine {
        fn value_test(&self){
            assert_eq!(
                self.log_version, 1,
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

    // TODO stub: should read from file
    fn get_input() -> Vec<String> {
        let path = env::var("LOG").expect("must pass log path to tests with env var `LOG=path/to/dbt.log`");
        println!("{}", path);

        let lines = read_lines(Path::new(&path))
            .expect("Something went wrong opening the log file");

        let contents = lines.into_iter().collect::<Result<Vec<String>, io::Error>>()
            .expect("Something went wrong reading lines of the log file");

        contents 

        // let serialized: &str = r#"{"data": {"code": "I011","path": "tests/generic/builtin.sql"},"invocation_id": "0c3303e3-2c5c-47f5-bc69-dfaae7843f6f","level": "debug","log_version": 1,"msg": "Parsing tests/generic/builtin.sql","node_info": {},"pid": 59758,"thread_name": "MainThread","ts": "2021-11-30T12:31:04.312814","type": "log_line"}"#;
        // let serialized: &str = r#"{"code": "I011", "data": {"path": "tests/generic/builtin.sql"},"invocation_id": "0c3303e3-2c5c-47f5-bc69-dfaae7843f6f","level": "debug","log_version": 1,"msg": "Parsing tests/generic/builtin.sql","node_info": {},"pid": 59758,"thread_name": "MainThread","ts": "2021-12-01T21:33:53.239614Z","type": "log_line"}"#;
        // vec![serialized.to_owned()]
    }

    fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
        let file = File::open(filename)?;
        Ok(io::BufReader::new(file).lines())
    }

    fn deserialized_input(log_lines: &[String]) -> serde_json::Result<Vec<LogLine>> {
        log_lines
            .into_iter()
            .map(|log_line| serde_json::from_str::<LogLine>(log_line))
            .collect()
    }

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

    // If this test breaks we have made a change that could break downstream consumers' apps
    // Either revert the change in dbt, or bump the log_version and add new docs to communicate the change
    #[test]
    fn test_expected_values() {
        let log_lines = deserialized_input(&get_input()).unwrap_or_else(|_| {
            assert!(false, "input failed to deserialize");
            vec![] // unreachable stub
        });

        for log_line in log_lines {
            log_line.value_test()
        }
    }

    // If this test breaks, dbt has logged a line that is out of sync with this schema version.
    // Either conform these log lines to this schema or update this schema, bump the log_version
    // and add new docs to communicate the change
    #[test]
    fn deserialize_serialize_is_unchanged() {
        let objects: Result<Vec<(serde_json::Value, serde_json::Value)>, serde_json::Error> =
            deserialize_serialize_loop(&get_input()).and_then(|v| {
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
}
