use chrono::NaiveDateTime;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogLine {
    log_version: isize,
    r#type: String,
    #[serde(with = "custom_date_format")]
    ts: NaiveDateTime, // TODO how do we want to handle timezone?
    pid: isize,
    msg: String,
    level: String,
    invocation_id: String,
    thread_name: String,
    data: serde_json::Value  // TODO be more specific
}

// logs output timestamps like this: "2021-11-30T12:31:04.312814"
// which is not within the default rfc3339 & ISO8601 format because of the seconds decimal place
// this requires handling the date with "%Y-%m-%dT%H:%M:%S.%f" which requires this
// boilerplate-looking module.
mod custom_date_format {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Serializer, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S.%f";

    pub fn serialize<S>(
        date: &NaiveDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}


fn main() {

    let serialized: &str = r#"
    {
        "data": {
            "code": "I011",
            "path": "tests/generic/builtin.sql"
        },
        "invocation_id": "0c3303e3-2c5c-47f5-bc69-dfaae7843f6f",
        "level": "debug",
        "log_version": 1,
        "msg": "Parsing tests/generic/builtin.sql",
        "node_info": {},
        "pid": 59758,
        "thread_name": "MainThread",
        "ts": "2021-11-30T12:31:04.312814",
        "type": "log_line"
    }"#;

    let deserialized: LogLine = serde_json::from_str(serialized).unwrap();
    print!("{:?}", deserialized)
}
