use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use regex::Regex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::COUNTERS;

pub fn cleanup_schema_name(input: &str) -> String {
    let re = Regex::new(r"___.*?___").unwrap();
    re.replace_all(input, "").to_string()
}

fn checksum8(input: &str) -> String {
    let input = cleanup_schema_name(input);
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{hash:x}")[..8.min(format!("{hash:x}").len())].to_string()
}

pub fn compute_file_name(
    recordings_dir: &Path,
    node_id: Option<&String>,
    sql: Option<&str>,
    metadata: bool,
) -> AdbcResult<String> {
    let id = match node_id {
        Some(node_id) => {
            if metadata {
                debug_assert!(
                    sql.is_some(),
                    "A Statement with metadata must have a SQL query"
                );
                format!("{}-{}", node_id, checksum8(sql.unwrap()))
            } else {
                node_id.to_owned()
            }
        }
        None => match sql {
            Some(sql) => checksum8(sql),
            None => {
                return Err(AdbcError::with_message_and_status(
                    "Neither node id nor sql was set in the query context",
                    AdbcStatus::Internal,
                ));
            }
        },
    };

    let dir_counters = COUNTERS.entry(recordings_dir.to_path_buf()).or_default();
    let mut entry = dir_counters.entry(id.clone()).or_insert(0);
    let file_name = format!("{}-{}", id, *entry);
    *entry += 1;

    Ok(file_name)
}

pub fn compute_file_name_for_table_schema(
    recordings_dir: &Path,
    node_id: Option<&str>,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: &str,
) -> String {
    let fqn = format!(
        "{}.{}.{}",
        catalog.unwrap_or("_"),
        db_schema.unwrap_or("_"),
        table_name
    );
    let hash = checksum8(&fqn);
    let counter_key = match node_id {
        Some(node_id) => format!("{node_id}.get_table_schema.{hash}"),
        None => format!("get_table_schema.{hash}"),
    };
    let dir_counters = COUNTERS.entry(recordings_dir.to_path_buf()).or_default();
    let mut entry = dir_counters.entry(counter_key.clone()).or_insert(0);
    let file_name = format!("{counter_key}-{}", *entry);
    *entry += 1;
    file_name
}
