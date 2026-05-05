use crate::args::ResolveArgs;
use crate::dbt_project_config::{ProjectConfigResolver, RootProjectConfigs, init_project_config};
use crate::resolve::resolve_utils::err_resource_name_has_spaces;
use crate::utils::{
    RelationComponents, get_node_fqn, register_duplicate_resource, trigger_duplicate_errors,
    update_node_relation_components,
};
use crate::validation::check_node_static_analysis;
use dbt_adapter_core::AdapterType;
use dbt_common::io_args::{StaticAnalysisKind, StaticAnalysisOffReason};
use dbt_common::tracing::emit::{emit_error_log_from_fs_error, emit_warn_log_from_fs_error};
use dbt_common::{ErrorCode, FsResult, fs_err, stdfs};
use dbt_frontend_common::Dialect;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::node_resolver::NodeResolver;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::dbt_utils::validate_delimiter;
use dbt_schemas::schemas::common::{DbtChecksum, DbtMaterialization, DbtQuoting, NodeDependsOn};
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::properties::SeedProperties;
use dbt_schemas::schemas::{CommonAttributes, DbtSeed, DbtSeedAttr, NodeBaseAttributes};
use dbt_schemas::state::{DbtPackage, GenericTestAsset};
use dbt_schemas::state::{ModelStatus, NodeResolverTracker};
use dbt_yaml::Value as YmlValue;
use minijinja::value::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;
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
    adapter_type: AdapterType,
    package_name: &str,
    jinja_env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    collected_generic_tests: &mut Vec<GenericTestAsset>,
    test_name_truncations: &mut HashMap<String, String>,
    node_resolver: &mut NodeResolver,
) -> FsResult<(HashMap<String, Arc<DbtSeed>>, HashMap<String, Arc<DbtSeed>>)> {
    let mut seeds: HashMap<String, Arc<DbtSeed>> = HashMap::new();
    let mut disabled_seeds: HashMap<String, Arc<DbtSeed>> = HashMap::new();
    let io_args = &arg.io;
    let dependency_package_name = dependency_package_name_from_ctx(jinja_env, base_ctx);

    let mut seed_root_dirs: Vec<String> = package
        .dbt_project
        .seed_paths
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|path| {
            Path::new(&path)
                .components()
                .next()
                .map(|comp| comp.as_os_str().to_string_lossy().to_string())
        })
        .collect();
    if seed_root_dirs.is_empty() {
        seed_root_dirs.push("seeds".to_string());
    }

    let config_resolver = ProjectConfigResolver::build(
        root_project_configs.seeds.clone(),
        dependency_package_name.is_some(),
        || {
            init_project_config(
                io_args,
                &package.dbt_project.seeds,
                package_quoting,
                dependency_package_name,
            )
        },
    )?
    .with_resolve_defaults(arg.static_analysis.unwrap_or_default());

    // TODO: update this to be relative of the root project
    let mut duplicate_errors = Vec::new();
    // Track seed names seen so far (name → relative path) to detect duplicates across subdirs
    let mut seen_seed_names: HashMap<String, std::path::PathBuf> = HashMap::new();
    for seed_file in package.seed_files.iter() {
        // Validate that path extension is one of csv, parquet, or json
        let path = seed_file.path.clone();
        let path_extension = path.extension().unwrap_or_default().to_ascii_lowercase();
        if path_extension != "csv" && path_extension != "parquet" && path_extension != "json" {
            continue;
        }

        let seed_name_owned = if path_extension == "parquet" {
            let components: Vec<String> = path
                .iter()
                .map(|part| part.to_string_lossy().to_string())
                .collect();
            if components.len() >= 2 {
                let parent_component = &components[components.len() - 2];
                let parent_is_seed_root =
                    seed_root_dirs.iter().any(|root| root == parent_component);
                if parent_is_seed_root && components.len() == 2 {
                    path.file_stem()
                        .and_then(|s| s.to_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| parent_component.clone())
                } else {
                    parent_component.clone()
                }
            } else {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| path.to_string_lossy().to_string())
            }
        } else {
            path.file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| path.to_string_lossy().to_string())
        };
        let seed_name = seed_name_owned.as_str();
        if seed_name.contains(' ') {
            return Err(err_resource_name_has_spaces(seed_name, &path));
        }

        // Detect two seeds with the same name in different subdirectories
        let original_file_path_for_name_check =
            stdfs::diff_paths(seed_file.base_path.join(&path), &io_args.in_dir)?;
        if let Some(existing_path) = seen_seed_names.get(seed_name) {
            let err_msg = format!(
                "dbt found two seeds with the name \"{}\".\n  Since these resources have the same name, dbt will be unable to find the correct resource when ref(\"{}\") is used.\n  To fix this, change the name of one of these resources:\n  - seed.{}.{} ({})\n  - seed.{}.{} ({})",
                seed_name,
                seed_name,
                package_name,
                seed_name,
                existing_path.display(),
                package_name,
                seed_name,
                original_file_path_for_name_check.display(),
            );
            duplicate_errors.push(
                *fs_err!(code => ErrorCode::InvalidConfig, loc => original_file_path_for_name_check.clone(), "{}", err_msg),
            );
            continue;
        }
        seen_seed_names.insert(seed_name.to_string(), original_file_path_for_name_check);

        let unique_id = format!("seed.{package_name}.{seed_name}");

        let fqn = get_node_fqn(
            package_name,
            path.to_owned(),
            vec![seed_name.to_owned()],
            package.dbt_project.seed_paths.as_ref().unwrap_or(&vec![]),
        );

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
                    true,
                )?,
                Some(mpe.relative_path.clone()),
            )
        } else {
            (SeedProperties::empty(seed_name.to_owned()), None)
        };

        let mut properties_config =
            config_resolver.resolve_with_properties(&fqn, seed.config.as_ref());
        let static_analysis = properties_config.static_analysis.clone();
        check_node_static_analysis(
            &properties_config,
            arg.static_analysis,
            seed_name,
            dependency_package_name,
            arg.io.status_reporter.as_ref(),
        );

        // XXX: normalize column_types to uppercase if it is snowflake
        if matches!(adapter_type, AdapterType::Snowflake)
            && let Some(column_types) = &properties_config.column_types
        {
            let column_types = column_types
                .iter()
                .map(|(k, v)| {
                    // Normalize column names for Snowflake case folding.
                    // If the key is not a valid unquoted identifier (e.g. contains
                    // spaces), auto-wrap it in double-quotes before parsing so it
                    // is treated as a quoted (case-preserving) identifier instead
                    // of being rejected. This matches Mantle behavior.
                    // Normalize column names for Snowflake case folding.
                    // If the key is not a valid unquoted identifier (e.g. it
                    // contains spaces), auto-wrap it in SQL double-quotes so it
                    // is treated as a case-preserving quoted identifier instead
                    // of being rejected. This matches Mantle behavior.
                    let key = k.as_str();
                    let sql;
                    let sql_str = if Dialect::Snowflake.parse_identifier(key).is_ok() {
                        key
                    } else {
                        sql = format!("\"{}\"", key.replace('"', "\"\""));
                        sql.as_str()
                    };
                    Ok((
                        Dialect::Snowflake
                            .parse_identifier(sql_str)
                            .map_err(|e| {
                                fs_err!(
                                    code => ErrorCode::InvalidColumnReference,
                                    loc => k.span().clone(),
                                    "Invalid identifier: {e}",
                                )
                            })?
                            .to_value()
                            .into(),
                        v.to_owned(),
                    ))
                })
                .collect::<FsResult<_>>()?;

            properties_config.column_types = Some(column_types);
        }
        let is_enabled = properties_config.enabled;

        let columns = process_columns(
            seed.columns.as_ref(),
            properties_config.meta.clone(),
            properties_config.tags.clone().map(|tags| tags.into()),
        )?;

        validate_delimiter(&properties_config.delimiter)?;

        // Calculate original file path first so we can use it for the checksum
        // if necessary for large seeds
        let original_file_path =
            stdfs::diff_paths(seed_file.base_path.join(&path), &io_args.in_dir)?;

        // Create initial seed with default values
        let mut dbt_seed = DbtSeed {
            __common_attr__: CommonAttributes {
                name: seed_name.to_owned(),
                package_name: package_name.to_owned(),
                path: path.to_owned(),
                name_span: dbt_common::Span::default(),
                original_file_path: original_file_path.clone(),
                checksum: DbtChecksum::seed_file_hash(
                    std::fs::read(seed_file.base_path.join(&path))
                        .map_err(|e| {
                            fs_err!(ErrorCode::IoError, "Failed to read seed file: {}", e)
                        })?
                        .as_slice(),
                    &original_file_path.to_string_lossy(),
                ),
                patch_path: patch_path.clone(),
                unique_id: unique_id.clone(),
                fqn,
                // dbt-core: description is always default ''
                description: Some(seed.description.clone().unwrap_or_default()),
                raw_code: None,
                language: None,
                tags: properties_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: properties_config.meta.clone().unwrap_or_default(),
            },
            __base_attr__: NodeBaseAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                alias: "".to_owned(),           // will be updated below
                relation_name: None,            // will be updated below
                columns,
                depends_on: NodeDependsOn::default(),
                quoting: properties_config
                    .quoting
                    .try_into()
                    .expect("DbtQuoting -> ResolvedQuoting conversion"),
                materialized: DbtMaterialization::Table,
                static_analysis_off_reason: (*static_analysis == StaticAnalysisKind::Off)
                    .then_some(StaticAnalysisOffReason::ConfiguredOff),
                static_analysis,
                ..Default::default()
            },
            __seed_attr__: DbtSeedAttr {
                quote_columns: properties_config.quote_columns.unwrap_or(false),
                column_types: properties_config.column_types.clone(),
                delimiter: properties_config.delimiter.clone().map(|d| d.into_inner()),
                root_path: Some(seed_file.base_path.clone()),
                catalog_name: properties_config.catalog_name.clone(),
            },
            __other__: BTreeMap::new(),
            deprecated_config: properties_config.clone().into(),
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

        // Populate unrendered_config.database/schema/alias for dbt-core compatible
        // state:* comparisons (mirrors what resolve_models does for model nodes).
        if let Some(db) = &properties_config.database {
            dbt_seed
                .__base_attr__
                .unrendered_config
                .insert("database".to_string(), YmlValue::string(db.clone()));
        }
        if let Some(sch) = &properties_config.schema {
            dbt_seed
                .__base_attr__
                .unrendered_config
                .insert("schema".to_string(), YmlValue::string(sch.clone()));
        }
        if let Some(alias) = &properties_config.alias {
            dbt_seed
                .__base_attr__
                .unrendered_config
                .insert("alias".to_string(), YmlValue::string(alias.clone()));
        }

        let status = if is_enabled {
            ModelStatus::Enabled
        } else {
            ModelStatus::Disabled
        };

        match node_resolver.insert_ref(&dbt_seed, adapter_type, status, false) {
            Ok(_) => (),
            Err(e) => {
                let err_with_loc = e.with_location(path.clone());
                emit_error_log_from_fs_error(&err_with_loc, io_args.status_reporter.as_ref());
            }
        }

        match status {
            ModelStatus::Enabled => {
                seeds.insert(unique_id, Arc::new(dbt_seed));
                if !arg.skip_creating_generic_tests {
                    seed.as_testable().persist(
                        package_name,
                        &root_project.name,
                        collected_generic_tests,
                        test_name_truncations,
                        adapter_type,
                        io_args,
                        patch_path.as_ref().unwrap_or(&path),
                    )?;
                }
            }
            ModelStatus::Disabled => {
                disabled_seeds.insert(unique_id, Arc::new(dbt_seed));
            }
            _ => {}
        }
    }

    for (seed_name, mpe) in seed_properties.iter() {
        if !mpe.schema_value.is_null() {
            let err = fs_err!(
                code => ErrorCode::NoNodeForYamlKey,
                loc => mpe.relative_path.clone(),
                "Unused schema.yml entry for seed '{}'",
                seed_name,
            );
            emit_warn_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
        }
    }

    trigger_duplicate_errors(io_args, &mut duplicate_errors)?;
    Ok((seeds, disabled_seeds))
}
