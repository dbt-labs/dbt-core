use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use std::path::PathBuf;

use super::{ProjectEnv, Task, TestEnv, TestResult};

fn remove_json_path(value: &mut Value, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    remove_json_path_parts(value, &parts);
}

fn remove_json_path_parts(value: &mut Value, parts: &[&str]) {
    if parts.is_empty() {
        return;
    }

    if let Value::Object(map) = value {
        if parts.len() == 1 {
            // Handle wildcard for removing field from all objects in the map
            if parts[0] == "*" {
                // This is not the intended use, but keeping original logic
                map.remove(parts[0]);
            } else {
                map.remove(parts[0]);
            }
        } else if parts[0] == "*" && parts.len() > 1 {
            // Handle wildcard: remove the specified field from all objects in this map
            let field_to_remove = &parts[1..].join(".");
            for (_, obj) in map.iter_mut() {
                remove_json_path(obj, field_to_remove);
            }
        } else if let Some(next) = map.get_mut(parts[0]) {
            remove_json_path_parts(next, &parts[1..]);
        }
    }
}

/// Generic task for comparing artifacts by deserializing and comparing JSON values
pub struct ArtifactComparisonTask<T>
where
    T: Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    pub rel_artifact_path: PathBuf,
    pub expected: T,
    /// . separated paths to ignore
    pub ignored_field_paths: Vec<String>,
}

impl<T> ArtifactComparisonTask<T>
where
    T: Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    pub fn new(
        rel_artifact_path: impl Into<PathBuf>,
        expected: T,
        ignored_field_paths: Vec<String>,
    ) -> Self {
        Self {
            rel_artifact_path: rel_artifact_path.into(),
            expected,
            ignored_field_paths,
        }
    }

    pub fn new_simple(rel_artifact_path: impl Into<PathBuf>, expected: T) -> Self {
        Self::new(rel_artifact_path, expected, vec![])
    }
}

#[async_trait]
impl<T> Task for ArtifactComparisonTask<T>
where
    T: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    async fn run(
        &self,
        project_env: &ProjectEnv,
        _test_env: &TestEnv,
        _task_index: usize,
    ) -> TestResult<()> {
        let artifact_file = project_env
            .absolute_project_dir
            .join(&self.rel_artifact_path);

        let contents = std::fs::read_to_string(&artifact_file)?;
        let actual: T = serde_json::from_str(&contents)?;

        // Convert both expected and actual to JSON for comparison
        let mut expected_json = serde_json::to_value(&self.expected)?;
        let mut actual_json = serde_json::to_value(&actual)?;

        // Remove ignored paths from both objects
        for path in &self.ignored_field_paths {
            remove_json_path(&mut expected_json, path);
            remove_json_path(&mut actual_json, path);
        }

        // Compare using pretty_assertions
        let error_msg = format!(
            "\n\nArtifact comparison failed for '{}'\nLeft (expected)   <\nRight (actual)    >",
            self.rel_artifact_path.display()
        );

        pretty_assertions::assert_eq!(expected_json, actual_json, "{}", error_msg);

        Ok(())
    }
}
