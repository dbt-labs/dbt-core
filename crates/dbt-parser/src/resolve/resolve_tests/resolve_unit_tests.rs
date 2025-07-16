use crate::dbt_project_config::RootProjectConfigs;
use crate::dbt_project_config::init_project_config;
use crate::resolve::resolve_properties::MinimalPropertiesEntry;
use crate::utils::get_node_fqn;
use crate::utils::get_unique_id;
use dbt_common::CodeLocation;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::err;
use dbt_common::error::AbstractLocation;
use dbt_common::fs_err;
use dbt_common::io_args::IoArgs;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_jinja_utils::phases::parse::build_resolve_model_context;
use dbt_jinja_utils::phases::parse::render_extract_ref_or_source_expr;
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::DbtUnitTestAttr;
use dbt_schemas::schemas::common::DbtChecksum;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::schemas::common::Expect;
use dbt_schemas::schemas::common::Formats;
use dbt_schemas::schemas::common::Given;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::packages::DeprecatedDbtPackageLock;
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::project::DefaultTo;
use dbt_schemas::schemas::project::UnitTestConfig;
use dbt_schemas::schemas::properties::UnitTestProperties;
use dbt_schemas::schemas::ref_and_source::DbtRef;
use dbt_schemas::schemas::ref_and_source::DbtSourceWrapper;
use dbt_schemas::schemas::{CommonAttributes, DbtUnitTest, NodeBaseAttributes};
use dbt_schemas::state::DbtPackage;
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_schemas::state::ResourcePathKind;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn resolve_unit_tests(
    io_args: &IoArgs,
    unit_test_properties: BTreeMap<String, MinimalPropertiesEntry>,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    adapter_type: &str,
    package_name: &str,
    jinja_env: &JinjaEnvironment<'static>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    model_properties: &BTreeMap<String, MinimalPropertiesEntry>,
    runtime_config: Arc<DbtRuntimeConfig>,
    models: &BTreeMap<String, Arc<DbtModel>>,
) -> FsResult<(
    BTreeMap<String, Arc<DbtUnitTest>>,
    BTreeMap<String, Arc<DbtUnitTest>>,
)> {
    let mut unit_tests: BTreeMap<String, Arc<DbtUnitTest>> = BTreeMap::new();
    let mut disabled_unit_tests: BTreeMap<String, Arc<DbtUnitTest>> = BTreeMap::new();
    let local_project_config = init_project_config(
        io_args,
        &package.dbt_project.unit_tests,
        UnitTestConfig {
            enabled: Some(true),
            ..Default::default()
        },
    )?;

    for (unit_test_name, mpe) in unit_test_properties.into_iter() {
        let unit_test = into_typed_with_jinja::<UnitTestProperties, _>(
            Some(io_args),
            mpe.schema_value,
            false,
            jinja_env,
            base_ctx,
            &[],
        )?;
        // todo: Unit test should have a database and schema,
        //    derived from the underlying model, correct?
        // - if so, we should get it and still store it so that it is available,
        // - but we should not serialize it
        // - for now just use the global ones

        let location = CodeLocation::default(); // TODO
        let model_name = format!("model.{}.{}", package_name, unit_test.model);
        let (database, schema, alias, model_found) = match models.get(&model_name) {
            Some(model) => (
                model.base_attr.database.clone(),
                model.base_attr.schema.clone(),
                model.base_attr.alias.clone(),
                true,
            ),
            None => (String::new(), String::new(), unit_test.model.clone(), false),
        };

        // Create base unit test node
        let base_unique_id = format!(
            "unit_test.{}.{}.{}",
            package_name, unit_test.model, unit_test_name
        );

        let fqn = get_node_fqn(
            package_name,
            mpe.relative_path.to_owned(),
            vec![unit_test.model.to_owned(), unit_test_name.to_owned()],
        );

        let global_config =
            local_project_config.get_config_for_path(&mpe.relative_path, package_name, &[]);
        let mut project_config = root_project_configs
            .unit_tests
            .get_config_for_path(&mpe.relative_path, package_name, &[])
            .clone();
        project_config.default_to(global_config);
        let properties_config = if let Some(properties) = &unit_test.config {
            let mut properties_config: UnitTestConfig = properties.clone();
            properties_config.default_to(&project_config);
            properties_config
        } else {
            project_config
        };

        let enabled = properties_config.get_enabled().unwrap_or(true);

        // todo: generalize given input format, according to https://docs.getdbt.com/docs/build/unit-tests

        let mut dependent_refs = vec![];

        // Add the model ref to the dependent refs
        dependent_refs.push(DbtRef {
            name: unit_test.model.to_owned(),
            package: Some(package_name.to_owned()),
            version: None,
            location: Some(CodeLocation::default()),
        });

        let mut dependent_sources = vec![];
        // Process unit test given inputs to extract ref nodes
        for given_group in unit_test.given.iter() {
            for g in given_group.iter() {
                let input = &g.input;
                if input.contains("ref") || input.contains("source") {
                    let sql_resources: Arc<Mutex<Vec<SqlResource<UnitTestConfig>>>> =
                        Arc::new(Mutex::new(Vec::new()));
                    let mut resolve_model_context = base_ctx.clone();
                    resolve_model_context.extend(build_resolve_model_context(
                        &properties_config,
                        adapter_type,
                        &database,
                        &schema,
                        &unit_test_name,
                        fqn.clone(),
                        package_name,
                        &root_project.name,
                        package_quoting,
                        runtime_config.clone(),
                        sql_resources.clone(),
                        Arc::new(AtomicBool::new(false)),
                    ));
                    let sql_resource = render_extract_ref_or_source_expr(
                        jinja_env,
                        &resolve_model_context,
                        sql_resources.clone(),
                        input,
                    )?;
                    match sql_resource {
                        SqlResource::Ref(ref_info) => {
                            dependent_refs.push(DbtRef {
                                name: ref_info.0,
                                package: ref_info.1,
                                version: ref_info.2.map(|v| v.into()),
                                location: Some(ref_info.3.with_file(&mpe.relative_path)),
                            });
                        }
                        SqlResource::Source(source_info) => {
                            dependent_sources.push(DbtSourceWrapper {
                                source: vec![source_info.0, source_info.1],
                                location: Some(source_info.2.with_file(&mpe.relative_path)),
                            });
                        }
                        _ => {
                            return err!(ErrorCode::Unexpected, "Invalid given input: {}", input);
                        }
                    }
                } else if input.eq("this") {
                    // this is handled at render time.
                    continue;
                } else {
                    return err!(ErrorCode::Unexpected, "Invalid given input: {}", input);
                }
            }
        }

        let mut file_map: BTreeMap<String, String> = BTreeMap::new();

        for asset in package.fixture_files.iter() {
            asset.path.file_name().map(|file_name| {
                file_map.insert(
                    file_name.to_string_lossy().to_string(),
                    asset.path.to_string_lossy().to_string(),
                )
            });
        }

        let given = unit_test.given.as_ref().map_or(vec![], |vec| {
            vec.iter()
                .map(|given| {
                    let full_path: Option<String> = match given.fixture {
                        Some(ref fixture) if given.format == Formats::Csv => {
                            file_map.get(&(fixture.clone() + ".csv")).cloned()
                        }
                        _ => given.fixture.clone(),
                    };

                    Given {
                        fixture: full_path,
                        input: given.input.clone(),
                        rows: given.rows.clone(),
                        format: given.format.clone(),
                    }
                })
                .collect::<Vec<_>>()
        });

        let expect = {
            let full_path: Option<String> = match unit_test.expect.fixture {
                Some(ref fixture) if unit_test.expect.format == Formats::Csv => {
                    file_map.get(&(fixture.clone() + ".csv")).cloned()
                }
                _ => unit_test.expect.fixture.clone(),
            };

            Expect {
                fixture: full_path,
                rows: unit_test.expect.rows.clone(),
                format: unit_test.expect.format.clone(),
            }
        };

        let base_unit_test = DbtUnitTest {
            common_attr: CommonAttributes {
                name: unit_test_name.to_owned(),
                package_name: package_name.to_owned(),
                original_file_path: mpe.relative_path.clone(),
                path: mpe.relative_path.clone(),
                unique_id: base_unique_id.clone(),
                fqn,
                description: unit_test.description.to_owned(),
                patch_path: None,
                checksum: DbtChecksum::default(),
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
                database: database.to_owned(),
                schema: schema.to_owned(),
                alias: alias.to_owned(), // alias will be used to constrcut `this` relation.
                relation_name: None,
                depends_on: NodeDependsOn::default(),
                refs: dependent_refs,
                sources: dependent_sources,
                enabled,
                extended_model: false,
                quoting: package_quoting.try_into()?,
                materialized: DbtMaterialization::Unit,
                static_analysis: properties_config
                    .static_analysis
                    .unwrap_or(StaticAnalysisKind::On),
                columns: BTreeMap::new(),
                metrics: vec![],
            },
            unit_test_attr: DbtUnitTestAttr {
                model: unit_test.model.to_owned(),
                given,
                expect,
                versions: None,
                version: None,
                overrides: unit_test.overrides.clone(),
            },
            deprecated_config: properties_config,
        };
        // Check if this model has versions
        if let Some(version_info) = model_properties
            .get(&unit_test.model)
            .and_then(|mpe| mpe.version_info.as_ref())
        {
            // Parse version configuration to get the include and exclude lists
            // this include and exclude accepted values are different than for generic tests
            // no 'all' or '*' accepted
            let version_config = unit_test.versions.as_ref().map(|v| {
                let v = v.as_object().expect("Version config is not an object");
                (
                    v.get("include").and_then(parse_version_numbers),
                    v.get("exclude").and_then(parse_version_numbers),
                )
            });

            // In the main code:
            let versions = version_info
                .all_versions
                .keys()
                .filter(|version| {
                    version_config
                        .as_ref()
                        .map(|(include, exclude)| {
                            should_include_version_for_unit_test(include, exclude, version)
                        })
                        .unwrap_or(true) // No version config means include all versions
                })
                .collect::<Vec<&String>>(); // Explicitly collect into Vec<&String>

            if !enabled {
                disabled_unit_tests.insert(base_unique_id, Arc::new(base_unit_test));
                continue;
            }

            // Create a unit test node for each version
            for version in versions {
                let versioned_model_id = version_info
                    .all_versions
                    .get(version as &str) // Explicitly convert to &str for lookup
                    .expect("Version should exist in lookup");

                let mut versioned_test = base_unit_test.clone();
                versioned_test.common_attr.unique_id = format!("{base_unique_id}.v{version}");
                versioned_test.unit_test_attr.version = Some(version.clone().into());
                versioned_test.base_attr.depends_on.nodes = vec![versioned_model_id.clone()];
                versioned_test.base_attr.depends_on.nodes_with_ref_location =
                    vec![(versioned_model_id.clone(), location.clone())];

                unit_tests.insert(
                    versioned_test.common_attr.unique_id.clone(),
                    Arc::new(versioned_test),
                );
            }
        } else {
            // Non-versioned case
            if !model_found || !enabled {
                disabled_unit_tests.insert(base_unique_id, Arc::new(base_unit_test));
            } else {
                unit_tests.insert(base_unique_id, Arc::new(base_unit_test));
            }
        }
    }
    Ok((unit_tests, disabled_unit_tests))
}

fn parse_version_numbers(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Array(arr) => Some(
            arr.iter()
                .filter_map(|v| match v {
                    Value::Number(n) => n.as_i64().map(|n| n.to_string()),
                    Value::String(s) => s.parse::<i64>().ok().map(|n| n.to_string()),
                    _ => None,
                })
                .collect(),
        ),
        Value::String(s) => s.parse::<i64>().ok().map(|n| vec![n.to_string()]),
        _ => None,
    }
}

fn should_include_version_for_unit_test(
    include: &Option<Vec<String>>,
    exclude: &Option<Vec<String>>,
    version: &str,
) -> bool {
    // If there's an include list, version must be in it
    let meets_include = include
        .as_ref()
        .map(|inc| inc.contains(&version.to_string()))
        .unwrap_or(true); // No include list means include all

    // If there's an exclude list, version must not be in it
    let meets_exclude = !exclude
        .as_ref()
        .map(|exc| exc.contains(&version.to_string()))
        .unwrap_or(false); // No exclude list means exclude none

    meets_include && meets_exclude
}
