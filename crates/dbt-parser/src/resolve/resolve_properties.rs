use crate::args::ResolveArgs;
use dbt_common::io_args::IoArgs;
use dbt_common::show_warning_soon_to_be_error;
use dbt_common::{constants::PARSING, fsinfo, show_progress, show_warning, ErrorCode, FsResult};
use dbt_common::{fs_err, unexpected_fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_jinja_utils::serde::{into_typed_raw, into_typed_with_jinja, value_from_file};
use dbt_schemas::schemas::properties::{
    DbtPropertiesFileValues, MinimalSchemaValue, MinimalTableValue,
};
use dbt_schemas::schemas::serde::FloatOrString;
use dbt_schemas::state::DbtPackage;
use dbt_serde_yaml::Verbatim;
use itertools::Itertools;
use minijinja::Value as MinijinjaValue;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct MinimalPropertiesEntry {
    pub name: String,
    pub relative_path: PathBuf,
    pub schema_value: dbt_serde_yaml::Value,
    pub table_value: Option<dbt_serde_yaml::Value>,
    pub version_info: Option<VersionInfo>,
    pub duplicate_paths: Vec<PathBuf>,
}

#[derive(Default, Debug)]
pub struct MinimalProperties {
    pub source_tables: BTreeMap<(String, String), MinimalPropertiesEntry>,
    pub models: BTreeMap<String, MinimalPropertiesEntry>,
    pub seeds: BTreeMap<String, MinimalPropertiesEntry>,
    pub snapshots: BTreeMap<String, MinimalPropertiesEntry>,
    pub unit_tests: BTreeMap<String, MinimalPropertiesEntry>,
    pub tests: BTreeMap<String, MinimalPropertiesEntry>,
}

// impl try extend from MinimalResolvedProperties
#[allow(clippy::cognitive_complexity)]
impl MinimalProperties {
    pub fn extend_from_minimal_properties_file(
        &mut self,
        io_args: &IoArgs,
        other: DbtPropertiesFileValues,
        jinja_env: &JinjaEnvironment<'static>,
        properties_path: &Path,
        base_ctx: &BTreeMap<String, MinijinjaValue>,
    ) -> FsResult<()> {
        // TODO: This is a bit repetetive. Can be shortened!
        if let Some(models) = other.models {
            // Extend but error on duplicate keys
            for model_value in models {
                let model = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    Some(io_args),
                    model_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    None,
                )?;
                for (key, maybe_version_info) in collect_model_version_info(&model).into_iter() {
                    if let Some(existing_model) = self.models.get_mut(&key) {
                        existing_model
                            .duplicate_paths
                            .push(properties_path.to_path_buf());
                    } else {
                        self.models.insert(
                            key,
                            MinimalPropertiesEntry {
                                name: validate_resource_name(&model.name)?,
                                relative_path: properties_path.to_path_buf(),
                                version_info: maybe_version_info,
                                schema_value: model_value.clone(),
                                table_value: None,
                                duplicate_paths: vec![],
                            },
                        );
                    }
                }
            }
        }
        if let Some(sources) = other.sources {
            for source_value in sources {
                let source = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    Some(io_args),
                    source_value,
                    false,
                    jinja_env,
                    base_ctx,
                    None,
                )?;
                if let Some(tables) = &*source.tables {
                    // Construct this once and reuse it for all tables:
                    let schema_value = dbt_serde_yaml::to_value(MinimalSchemaValue {
                        // Clear the tables field since it's already processed here:
                        tables: Verbatim(None),
                        ..source.clone()
                    })
                    .map_err(|e| {
                        unexpected_fs_err!(
                            "Failed to convert MinimalSchemaValue to dbt_serde_yaml::Value: {e}"
                        )
                    })?;

                    for table in tables.iter() {
                        let minimum_table_value = into_typed_with_jinja::<MinimalTableValue, _>(
                            Some(io_args),
                            table.clone(),
                            false,
                            jinja_env,
                            base_ctx,
                            None,
                        )?;
                        let key = (source.name.clone(), minimum_table_value.name.clone());

                        if let Some(existing_entry) = self.source_tables.get_mut(&key) {
                            existing_entry
                                .duplicate_paths
                                .push(properties_path.to_path_buf());

                            show_warning!(
                                io_args,
                                fs_err!(
                                    ErrorCode::SchemaError,
                                    "Duplicate definition for table '{}' in source '{}' found in file '{}'. Using definition from '{}'.",
                                    minimum_table_value.name,
                                    source.name,
                                    properties_path.display(),
                                    existing_entry.relative_path.display()
                                )
                            );
                        } else {
                            self.source_tables.insert(
                                key,
                                MinimalPropertiesEntry {
                                    name: validate_resource_name(&format!(
                                        "{}.{}",
                                        source.name, minimum_table_value.name
                                    ))?,
                                    relative_path: properties_path.to_path_buf(),
                                    schema_value: schema_value.clone(),
                                    table_value: Some(table.clone()), // Store table separately
                                    version_info: None,
                                    duplicate_paths: vec![],
                                },
                            );
                        }
                    }
                } else {
                    show_warning!(
                        io_args,
                        fs_err!(
                            ErrorCode::SchemaError,
                            "No tables defined for source '{}' in file '{}'.",
                            source.name,
                            properties_path.display()
                        )
                    );
                }
            }
        }
        if let Some(seeds) = other.seeds {
            for seed_value in seeds {
                let seed = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    Some(io_args),
                    seed_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    None,
                )?;
                if let Some(existing_seed) = self.seeds.get_mut(&seed.name) {
                    existing_seed
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.seeds.insert(
                        seed.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&seed.name)?,
                            relative_path: properties_path.to_path_buf(),
                            schema_value: seed_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(snapshots) = other.snapshots {
            for snapshot_value in snapshots {
                let snapshot = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    Some(io_args),
                    snapshot_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    None,
                )?;
                if let Some(existing_snapshot) = self.snapshots.get_mut(&snapshot.name) {
                    existing_snapshot
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.snapshots.insert(
                        snapshot.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&snapshot.name)?,
                            relative_path: properties_path.to_path_buf(),
                            schema_value: snapshot_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(unit_tests) = other.unit_tests {
            for unit_test_value in unit_tests {
                let unit_test = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    Some(io_args),
                    unit_test_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    None,
                )?;
                if let Some(existing_unit_test) = self.unit_tests.get_mut(&unit_test.name) {
                    existing_unit_test
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.unit_tests.insert(
                        unit_test.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&unit_test.name)?,
                            relative_path: properties_path.to_path_buf(),
                            schema_value: unit_test_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(tests) = other.tests {
            for test_value in tests {
                let test = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    Some(io_args),
                    test_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    None,
                )?;
                if let Some(existing_test) = self.tests.get_mut(&test.name) {
                    existing_test
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.tests.insert(
                        test.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&test.name)?,
                            relative_path: properties_path.to_path_buf(),
                            schema_value: test_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        if let Some(data_tests) = other.data_tests {
            for test_value in data_tests {
                let test = into_typed_with_jinja::<MinimalSchemaValue, _>(
                    Some(io_args),
                    test_value.clone(),
                    false,
                    jinja_env,
                    base_ctx,
                    None,
                )?;
                if let Some(existing_test) = self.tests.get_mut(&test.name) {
                    existing_test
                        .duplicate_paths
                        .push(properties_path.to_path_buf());
                } else {
                    self.tests.insert(
                        test.name.clone(),
                        MinimalPropertiesEntry {
                            name: validate_resource_name(&test.name)?,
                            relative_path: properties_path.to_path_buf(),
                            schema_value: test_value,
                            table_value: None,
                            version_info: None,
                            duplicate_paths: vec![],
                        },
                    );
                }
            }
        }
        Ok(())
    }
}

fn validate_resource_name(name: &str) -> FsResult<String> {
    // Check for the space character for now. This can be extended anytime we deprecate
    // more of special characters like !@#%$":'
    if name.chars().any(|c| matches!(c, ' ')) {
        let err = fs_err!(
            ErrorCode::SchemaError,
            "Resource name '{}' contains forbidden characters",
            name
        );
        Err(err)
    } else {
        Ok(name.to_string())
    }
}

pub fn resolve_minimal_properties(
    arg: &ResolveArgs,
    package: &DbtPackage,
    jinja_env: &JinjaEnvironment<'static>,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
) -> FsResult<MinimalProperties> {
    let mut minimal_resolved_properties = MinimalProperties::default();
    for dbt_asset in package.dbt_properties.iter().dedup() {
        dbt_common::check_cancellation!(arg.io.should_cancel_compilation)?;
        let absolute_path = dbt_asset.base_path.join(&dbt_asset.path);
        show_progress!(
            arg.io,
            fsinfo!(
                PARSING.into(),
                dbt_asset
                    .to_display_path(&arg.io.in_dir)
                    .display()
                    .to_string()
            )
        );
        let properties_file_value = value_from_file(Some(&arg.io), &absolute_path)?;

        match into_typed_raw::<DbtPropertiesFileValues>(Some(&arg.io), properties_file_value) {
            Ok(properties_file_values) => {
                minimal_resolved_properties.extend_from_minimal_properties_file(
                    &arg.io,
                    properties_file_values,
                    jinja_env,
                    &dbt_asset.path,
                    base_ctx,
                )?;
            }
            Err(e) => {
                show_warning_soon_to_be_error!(arg.io, e);
                continue; // processing other files
            }
        }
    }
    Ok(minimal_resolved_properties)
}

#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version: String,
    pub latest_version: String,
    pub versioned_name: String,
    pub version_config: Verbatim<Option<dbt_serde_yaml::Value>>,
    // TODO: Remove this and figure out more efficient way to handle this
    pub all_versions: BTreeMap<String, String>,
}

// Collect and build a properites config for all versions of a model
pub fn collect_model_version_info(
    model: &MinimalSchemaValue,
) -> Vec<(String, Option<VersionInfo>)> {
    if let Some(versions) = &model.versions {
        let mut version_entries = versions
            .iter()
            .map(|v| {
                let version = match &v.v {
                    Value::String(s) => Some(s.to_string()),
                    Value::Number(n) => Some(n.to_string()),
                    _ => None,
                }
                .unwrap_or_else(|| {
                    panic!("Version '{}' does not meet the required format", v.v);
                });

                let versioned_name = format!("{}_v{}", model.name, version);

                let defined_in = v.__additional_properties__.get("defined_in").and_then(|d| {
                    d.as_str().map(|s| {
                        if s.ends_with(".sql") {
                            s.strip_suffix(".sql").unwrap().to_string()
                        } else {
                            s.to_string()
                        }
                    })
                });

                let version_config = v.config.clone();

                (
                    version,
                    defined_in.unwrap_or(versioned_name),
                    version_config,
                )
            })
            .collect::<Vec<_>>();
        let latest_version = model
            .latest_version
            .clone()
            .map(|v| match v {
                FloatOrString::String(s) => s,
                FloatOrString::Number(n) => n.to_string(),
            })
            .unwrap_or_else(|| {
                // Try parsing as numbers first
                let numeric_versions: Vec<_> = version_entries
                    .iter()
                    .filter_map(|(v, _, _)| v.parse::<f32>().ok())
                    .collect();

                if numeric_versions.len() == version_entries.len() {
                    // If all versions are numeric, use highest number
                    numeric_versions
                        .iter()
                        .reduce(|a, b| if a > b { a } else { b })
                        .map(|n| n.to_string())
                        .expect("Versions should not be empty")
                } else {
                    // Otherwise use lexicographically last
                    version_entries
                        .iter()
                        .map(|(v, _, _)| v)
                        .max()
                        .unwrap()
                        .clone()
                }
            });

        // Find the config for the latest version from existing version entries
        let latest_version_config = version_entries
            .iter()
            .find(|(v, _, _)| v == &latest_version)
            .map(|(_, _, config)| config.clone())
            .unwrap_or_else(|| Verbatim(None));

        // Only add the latest version by model.name if it's not already in the list (as in, defined by a defined_in)
        if !version_entries.iter().any(|(_, d, _)| d == &model.name) {
            // how do I get the config for the latest version?
            version_entries.push((
                latest_version.clone(),
                model.name.clone(),
                latest_version_config,
            ));
        }
        version_entries
            .iter()
            .map(|(v, d, config)| {
                (
                    d.clone(),
                    Some(VersionInfo {
                        version: v.clone(),
                        latest_version: latest_version.clone(),
                        versioned_name: d.clone(),
                        all_versions: version_entries
                            .iter()
                            .map(|(v, d, _)| (v.clone(), d.clone()))
                            .collect(),
                        version_config: config.clone(),
                    }),
                )
            })
            .collect()
    } else {
        vec![(model.name.clone(), None)]
    }
}
