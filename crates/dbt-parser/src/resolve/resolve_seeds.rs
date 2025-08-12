use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::utils::{
    RelationComponents, get_node_fqn, register_duplicate_resource, trigger_duplicate_errors,
    update_node_relation_components,
};
use dbt_common::{ErrorCode, FsResult, fs_err, show_error, stdfs};
use dbt_frontend_common::Dialect;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::refs_and_sources::RefsAndSources;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::dbt_utils::validate_delimeter;
use dbt_schemas::schemas::common::{DbtChecksum, DbtMaterialization, DbtQuoting, NodeDependsOn};
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::project::DefaultTo;
use dbt_schemas::schemas::project::{DbtProject, SeedConfig};
use dbt_schemas::schemas::properties::SeedProperties;
use dbt_schemas::schemas::{CommonAttributes, DbtSeed, DbtSeedAttr, NodeBaseAttributes};
use dbt_schemas::state::{DbtAsset, DbtPackage};
use dbt_schemas::state::{ModelStatus, RefsAndSourcesTracker};
use minijinja::value::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;
use super::resolve_tests::persist_generic_data_tests::TestableNodeTrait;

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn resolve_seeds(
    arg: &ResolveArgs,
    mut seed_properties: BTreeMap<String, MinimalPropertiesEntry>,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    database: &str,
    schema: &str,
    adapter_type: &str,
    package_name: &str,
    jinja_env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    collected_tests: &mut Vec<DbtAsset>,
    refs_and_sources: &mut RefsAndSources,
) -> FsResult<(HashMap<String, Arc<DbtSeed>>, HashMap<String, Arc<DbtSeed>>)> {
    let mut seeds: HashMap<String, Arc<DbtSeed>> = HashMap::new();
    let mut disabled_seeds: HashMap<String, Arc<DbtSeed>> = HashMap::new();
    let is_replay_mode = arg.replay.is_some();
    let io_args = &arg.io;
    let dependency_package_name = dependency_package_name_from_ctx(jinja_env, base_ctx);

    let local_project_config = init_project_config(
        io_args,
        &package.dbt_project.seeds,
        SeedConfig {
            enabled: Some(true),
            quoting: Some(package_quoting),
            ..Default::default()
        },
        dependency_package_name,
    )?;

    // TODO: update this to be relative of the root project
    let mut duplicate_errors = Vec::new();
    for seed_file in package.seed_files.iter() {
        // Validate that path extension is one of csv, parquet, or json
        let path = seed_file.path.clone();
        let path_extension = path.extension().unwrap_or_default().to_ascii_lowercase();
        if path_extension != "csv" && path_extension != "parquet" && path_extension != "json" {
            continue;
        }

        let seed_name = if path_extension == "parquet" {
            path.parent()
                .unwrap()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
        } else {
            path.file_stem().unwrap().to_str().unwrap()
        };
        let unique_id = format!("seed.{package_name}.{seed_name}");

        let fqn = get_node_fqn(package_name, path.to_owned(), vec![seed_name.to_owned()]);

        // Merge schema_file_info
        let (seed, patch_path) = if let Some(mpe) = seed_properties.remove(seed_name) {
            if !mpe.duplicate_paths.is_empty() {
                register_duplicate_resource(&mpe, seed_name, "seed", &mut duplicate_errors);
            }
            (
                into_typed_with_jinja::<SeedProperties, _>(
                    io_args,
                    mpe.schema_value,
                    false,
                    jinja_env,
                    base_ctx,
                    &[],
                    dependency_package_name,
                )?,
                Some(mpe.relative_path.clone()),
            )
        } else {
            (SeedProperties::empty(seed_name.to_owned()), None)
        };

        let project_config = local_project_config.get_config_for_path(
            &path,
            package_name,
            &package
                .dbt_project
                .seed_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        );
        let mut properties_config = if let Some(properties) = &seed.config {
            let mut properties_config: SeedConfig = properties.clone();
            properties_config.default_to(project_config);
            properties_config
        } else {
            project_config.clone()
        };

        // normalize column_types to uppercase if it is snowflake
        if adapter_type == "snowflake" || adapter_type == "replay" {
            if let Some(column_types) = &properties_config.column_types {
                let column_types = column_types
                    .iter()
                    .map(|(k, v)| {
                        Ok((
                            Dialect::Snowflake
                                .parse_identifier(k)
                                .map_err(|e| {
                                    fs_err!(
                                        ErrorCode::InvalidColumnReference,
                                        "Invalid identifier: {}",
                                        e
                                    )
                                })?
                                .to_value(),
                            v.to_owned(),
                        ))
                    })
                    .collect::<FsResult<_>>()?;

                properties_config.column_types = Some(column_types);
            }
        }

        if package_name != root_project.name {
            let mut root_config = root_project_configs
                .seeds
                .get_config_for_path(
                    &path,
                    package_name,
                    &package
                        .dbt_project
                        .seed_paths
                        .as_ref()
                        .unwrap_or(&vec!["seeds".to_string()])
                        .clone(),
                )
                .clone();
            root_config.default_to(&properties_config);
            properties_config = root_config;
        }

        let is_enabled = properties_config.get_enabled().unwrap_or(true);

        let columns = process_columns(
            seed.columns.as_ref(),
            properties_config.meta.clone(),
            properties_config.tags.clone().map(|tags| tags.into()),
        )?;

        validate_delimeter(&properties_config.delimiter)?;

        // Create initial seed with default values
        let mut dbt_seed = DbtSeed {
            common_attr: CommonAttributes {
                name: seed_name.to_owned(),
                package_name: package_name.to_owned(),
                path: path.to_owned(),
                original_file_path: stdfs::diff_paths(
                    seed_file.base_path.join(&path),
                    &io_args.in_dir,
                )?,
                checksum: DbtChecksum::hash(
                    std::fs::read(seed_file.base_path.join(&path))
                        .map_err(|e| {
                            fs_err!(ErrorCode::IoError, "Failed to read seed file: {}", e)
                        })?
                        .as_slice(),
                ),
                patch_path,
                unique_id: unique_id.clone(),
                fqn,
                description: seed.description.clone(),
                raw_code: None,
                language: None,
                tags: properties_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: properties_config.meta.clone().unwrap_or_default(),
            },
            base_attr: NodeBaseAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                alias: "".to_owned(),           // will be updated below
                relation_name: None,            // will be updated below
                columns,
                depends_on: NodeDependsOn::default(),
                quoting: properties_config
                    .quoting
                    .expect("quoting is required")
                    .try_into()
                    .expect("quoting is required"),
                materialized: DbtMaterialization::Table,
                ..Default::default()
            },
            seed_attr: DbtSeedAttr {
                quote_columns: properties_config.quote_columns.unwrap_or(false),
                column_types: properties_config.column_types.clone(),
                delimiter: properties_config.delimiter.clone().map(|d| d.into_inner()),
                root_path: Some(seed_file.base_path.clone()),
            },
            other: BTreeMap::new(),
            deprecated_config: properties_config.clone(),
        };

        let components = RelationComponents {
            database: properties_config.database.clone(),
            schema: properties_config.schema.clone(),
            alias: properties_config.alias.clone(),
            store_failures: None,
        };

        update_node_relation_components(
            &mut dbt_seed,
            jinja_env,
            &root_project.name,
            package_name,
            base_ctx,
            &components,
            adapter_type,
        )?;

        let status = if is_enabled {
            ModelStatus::Enabled
        } else {
            ModelStatus::Disabled
        };

        match refs_and_sources.insert_ref(&dbt_seed, adapter_type, status, false) {
            Ok(_) => (),
            Err(e) => {
                show_error!(&io_args, e.with_location(path.clone()));
            }
        }

        match status {
            ModelStatus::Enabled => {
                seeds.insert(unique_id, Arc::new(dbt_seed));
                seed.as_testable().persist(
                    package_name,
                    &root_project.name,
                    collected_tests,
                    adapter_type,
                    is_replay_mode,
                    io_args,
                )?;
            }
            ModelStatus::Disabled => {
                disabled_seeds.insert(unique_id, Arc::new(dbt_seed));
            }
            _ => {}
        }
    }
    trigger_duplicate_errors(io_args, &mut duplicate_errors)?;
    Ok((seeds, disabled_seeds))
}
