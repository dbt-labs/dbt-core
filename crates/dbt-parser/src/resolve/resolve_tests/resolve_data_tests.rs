use crate::args::ResolveArgs;
use crate::dbt_project_config::RootProjectConfigs;
use crate::dbt_project_config::init_project_config;
use crate::renderer::RenderCtx;
use crate::renderer::RenderCtxInner;
use crate::renderer::SqlFileRenderResult;
use crate::renderer::collect_adapter_identifiers_detect_unsafe;
use crate::renderer::render_unresolved_sql_files;
use crate::resolve::resolve_properties::MinimalPropertiesEntry;
use crate::utils::RelationComponents;
use crate::utils::generate_relation_components;
use crate::utils::get_node_fqn;
use crate::utils::get_original_file_contents;
use crate::utils::get_original_file_path;
use crate::utils::update_node_relation_components;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME;
use dbt_common::error::AbstractLocation;
use dbt_common::fs_err;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::io_args::StaticAnalysisOffReason;
use dbt_common::io_utils::try_read_yml_to_str;
use dbt_common::stdfs;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
use dbt_jinja_utils::listener::JinjaTypeCheckingEventListenerFactory;
use dbt_jinja_utils::node_resolver::NodeResolver;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::DbtTestAttr;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::IntrospectionKind;
use dbt_schemas::schemas::common::DbtChecksum;
use dbt_schemas::schemas::common::DbtContract;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::schemas::common::DocsConfig;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::nodes::TestMetadata;
use dbt_schemas::schemas::project::DataTestConfig;
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::project::DefaultTo;
use dbt_schemas::schemas::properties::DataTestProperties;
use dbt_schemas::schemas::properties::ModelProperties;
use dbt_schemas::schemas::ref_and_source::DbtRef;
use dbt_schemas::schemas::ref_and_source::DbtSourceWrapper;
use dbt_schemas::schemas::{CommonAttributes, DbtTest, InternalDbtNode, NodeBaseAttributes};
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_schemas::state::GenericTestAsset;
use dbt_schemas::state::ModelStatus;
use dbt_schemas::state::{DbtAsset, DbtPackage};
use dbt_serde_yaml::Value as YmlValue;
use md5;
use minijinja::Value;
use minijinja::constants::DEFAULT_TEST_SCHEMA;
use serde::de;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Arc;

fn test_metadata_from_asset(asset: &GenericTestAsset) -> Option<TestMetadata> {
    if let Some(name) = &asset.test_metadata_name {
        let mut kwargs = BTreeMap::new();
        if let Some(col) = &asset.test_metadata_column_name {
            kwargs.insert("column_name".to_string(), YmlValue::string(col.clone()));
        }
        if let Some(cols) = &asset.test_metadata_combination_of_columns {
            let seq = cols
                .iter()
                .cloned()
                .map(YmlValue::string)
                .collect::<Vec<_>>();
            kwargs.insert(
                "combination_of_columns".to_string(),
                YmlValue::Sequence(seq, Default::default()),
            );
        }
        return Some(TestMetadata {
            name: name.clone(),
            kwargs,
            namespace: asset.test_metadata_namespace.clone(),
        });
    }
    None
}

pub fn build_data_test_raw_code(
    test_metadata: Option<TestMetadata>,
    alias: String,
) -> Option<String> {
    if let Some(test_metadata) = test_metadata {
        let config_str = format!("config(alias=\"{alias}\")");

        let mut test_macro_name = format!("test_{}", test_metadata.name);
        if let Some(namespace) = test_metadata.namespace {
            test_macro_name = format!("{}.{}", namespace, test_macro_name);
        }

        return Some(format!(
            "{{{{ {test_macro_name}(**_dbt_generic_test_kwargs) }}}}{{{{ {config_str} }}}}"
        ));
    }

    None
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_data_tests(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    test_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    database: &str,
    schema: &str,
    adapter_type: AdapterType,
    env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    runtime_config: Arc<DbtRuntimeConfig>,
    collected_generic_tests: &[GenericTestAsset],
    node_resolver: &NodeResolver,
    jinja_env: Arc<JinjaEnv>,
    token: &CancellationToken,
) -> FsResult<(HashMap<String, Arc<DbtTest>>, HashMap<String, Arc<DbtTest>>)> {
    let jinja_type_checking_event_listener_factory =
        Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default());
    let mut nodes: HashMap<String, Arc<DbtTest>> = HashMap::new();
    let mut nodes_with_execute: HashMap<String, DbtTest> = HashMap::new();
    let mut disabled_tests: HashMap<String, Arc<DbtTest>> = HashMap::new();
    let package_name = package.dbt_project.name.as_str();

    let tests_config = match (
        package.dbt_project.tests.clone(),
        package.dbt_project.data_tests.clone(),
    ) {
        (Some(_), Some(_)) => {
            unimplemented!("Merge logic for tests and data tests is unimplemented")
        }
        (Some(tests), None) => Some(tests),
        (None, Some(data_tests)) => Some(data_tests),
        (None, None) => None,
    };

    let local_project_config = init_project_config(
        &arg.io,
        &tests_config,
        DataTestConfig {
            enabled: Some(true),
            quoting: Some(package_quoting),
            ..Default::default()
        },
        dependency_package_name_from_ctx(&env, base_ctx),
    )?;

    // Create a map of dbt_asset.path.stem to GenericTestAsset for efficient lookup
    let test_path_to_test_asset: HashMap<PathBuf, &GenericTestAsset> = collected_generic_tests
        .iter()
        .map(|test_asset| (test_asset.dbt_asset.path.clone(), test_asset))
        .collect();

    let mut test_assets_to_render = package.test_files.clone();
    test_assets_to_render.extend(
        collected_generic_tests
            .iter()
            .map(|test_asset| test_asset.dbt_asset.clone()),
    );

    let render_ctx = RenderCtx {
        inner: Arc::new(RenderCtxInner {
            args: arg.clone(),
            root_project_name: root_project.name.clone(),
            root_project_config: root_project_configs.tests.clone(),
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type,
            database: database.to_string(),
            schema: schema.to_string(),
            local_project_config,
            resource_paths: package
                .dbt_project
                .test_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        }),
        jinja_env: env.clone(),
        runtime_config: runtime_config.clone(),
    };

    let mut test_sql_resources_map =
        render_unresolved_sql_files::<DataTestConfig, DataTestProperties>(
            &render_ctx,
            &test_assets_to_render,
            test_properties,
            token,
            jinja_type_checking_event_listener_factory.clone(),
        )
        .await?;
    // make deterministic
    test_sql_resources_map.sort_by(|a, b| {
        a.asset
            .path
            .file_name()
            .cmp(&b.asset.path.file_name())
            .then(a.asset.path.cmp(&b.asset.path))
    });

    let default_dbt_config = DataTestConfig {
        fail_calc: Some("count(*)".to_string()),
        warn_if: Some("!= 0".to_string()),
        error_if: Some("!= 0".to_string()),
        limit: None,
        ..Default::default()
    };

    let all_depends_on = jinja_type_checking_event_listener_factory
        .all_depends_on
        .read()
        .unwrap()
        .clone();

    for SqlFileRenderResult {
        asset: dbt_asset,
        sql_file_info,
        rendered_sql,
        macro_spans: _macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
    } in test_sql_resources_map.iter()
    {
        let mut test_config = sql_file_info.config.clone();
        test_config.default_to(&default_dbt_config);

        if test_config.schema.is_none() {
            test_config.schema = Some(DEFAULT_TEST_SCHEMA.to_string());
        }

        // Use the custom test name from GenericTestAsset if available, otherwise use the filename
        let test_name = test_path_to_test_asset
            .get(&dbt_asset.path)
            .map(|test_asset| test_asset.test_name.clone())
            .unwrap_or_else(|| {
                dbt_asset
                    .path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            });

        let properties = if let Some(properties) = maybe_properties {
            properties
        } else {
            &DataTestProperties::empty(test_name.to_owned())
        };

        // To conform to the unique_id format in dbt-core, we need to hash the test name
        // append the last 10 characters of the hash to the unique_id.
        // See the `create_test_node` function in
        // https://github.com/dbt-labs/dbt-core/blob/3de3b827bfffdc43845780f484d4d53011f20a37/core/dbt/parser/schema_generic_tests.py#L132
        // Note that fusion does not produce the same hash as dbt-core since dbt-core
        // performs a hash of serialized python data strutures whereas fusion
        // performs a hash of soe of the contents of what was in the python data structs.
        // See https://github.com/dbt-labs/fs/pull/4725#issuecomment-3133476096 for more details.
        const HASH_LENGTH: usize = 10;
        let hash_hex = format!("{:x}", md5::compute(&test_name));
        let test_hash = hash_hex[hash_hex.len() - HASH_LENGTH..].to_string();

        let unique_id = format!("test.{package_name}.{test_name}.{test_hash}");

        // Check if this test_name corresponds to any test in our collected tests
        // If so, use the original_file_path from the GenericTestAsset for the fqn construction and original_file_path
        let path_for_fqn = test_path_to_test_asset
            .get(&dbt_asset.path)
            .map(|test_asset| test_asset.original_file_path.clone())
            .unwrap_or_else(|| dbt_asset.path.to_owned());

        // singular data tests are only found in test_paths, but generic tests
        // can be found in any directory in all_source_paths
        let fqn = get_node_fqn(
            package_name,
            path_for_fqn,
            vec![test_name.to_owned()],
            &package.dbt_project.all_source_paths(),
        );

        // Errored models can be enabled, so enabled is set to the opposite of disabled
        test_config.enabled = Some(!(*status == ModelStatus::Disabled));

        let static_analysis = test_config
            .static_analysis
            .unwrap_or(StaticAnalysisKind::On);

        let macro_depends_on = all_depends_on
            .get(&format!("{package_name}.{test_name}"))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // NOTE: This says get_original_file_path but for tests this is the path to the generated sql file
        let generated_file_path =
            get_original_file_path(&dbt_asset.base_path, &arg.io.in_dir, &dbt_asset.path);

        let defined_at = test_path_to_test_asset
            .get(&dbt_asset.path)
            .map(|test_asset| test_asset.defined_at.clone());

        let patch_path = test_path_to_test_asset
            .get(&dbt_asset.path)
            .map(|test_asset| test_asset.original_file_path.clone())
            .or_else(|| patch_path.clone());

        let is_singular_data_test = defined_at.is_none();

        let manifest_original_file_path = if is_singular_data_test {
            generated_file_path.clone()
        } else {
            patch_path.clone().unwrap()
        };

        // Populate TestMetadata only for generic data tests (not singular .sql tests)
        // This ensures external tooling (e.g., project-evaluator) correctly classifies tests
        let inferred_test_metadata = if is_singular_data_test {
            None
        } else {
            test_path_to_test_asset
                .get(&dbt_asset.path)
                .and_then(|asset| test_metadata_from_asset(asset))
        };

        let mut dbt_test = DbtTest {
            defined_at,
            manifest_original_file_path: manifest_original_file_path.clone(),
            __common_attr__: CommonAttributes {
                name: test_name.to_owned(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.to_owned(),
                name_span: dbt_common::Span::default(),
                // original_file_path is a misnomer for tests, it's the path to the generated sql file
                original_file_path: generated_file_path,
                patch_path,
                unique_id: unique_id.clone(),
                fqn,
                description: properties.description.clone(),
                checksum: DbtChecksum::hash(rendered_sql.trim().as_bytes()),
                // TODO: hydrate for generic + singular tests
                // Examples in Mantle:
                // - Generic test: "{{ test_not_null(**_dbt_generic_test_kwargs) }}"
                // - Generic test with dbt_utils.expression_is_true: "{{ dbt_utils.test_expression_is_true(**_dbt_generic_test_kwargs) }}{{ config(alias=\"dbt_utils_expression_is_true_c_177c20685a18a9071d4a71719e3d9565\") }}"
                // - Singular test: "SELECT 1\nFROM {{ ref('customers') }}\nLIMIT 0"
                raw_code: Some("will_be_updated_below".to_string()),
                language: Some("sql".to_string()),
                tags: test_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: test_config.meta.clone().unwrap_or_default(),
            },
            __base_attr__: NodeBaseAttributes {
                database: database.to_owned(),
                schema: schema.to_owned(),
                alias: "will_be_updated_below".to_owned(),
                relation_name: None,
                static_analysis_off_reason: matches!(static_analysis, StaticAnalysisKind::Off)
                    .then(|| StaticAnalysisOffReason::ConfiguredOff),
                static_analysis,
                quoting: test_config
                    .quoting
                    .expect("quoting is required")
                    .try_into()
                    .expect("quoting is required"),
                quoting_ignore_case: test_config
                    .quoting
                    .expect("quoting is required")
                    .snowflake_ignore_case
                    .unwrap_or(false),
                materialized: DbtMaterialization::Test,
                enabled: test_config.enabled.unwrap_or(true),
                extended_model: false,
                persist_docs: None,
                columns: vec![],
                depends_on: NodeDependsOn {
                    macros: macro_depends_on,
                    nodes: vec![],
                    nodes_with_ref_location: vec![],
                },
                refs: sql_file_info
                    .refs
                    .iter()
                    .map(|(model, project, version, location)| DbtRef {
                        name: model.to_owned(),
                        package: project.to_owned(),
                        version: version.clone().map(|v| v.into()),
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                functions: sql_file_info
                    .functions
                    .iter()
                    .map(|(function_name, package, location)| DbtRef {
                        name: function_name.to_owned(),
                        package: package.to_owned(),
                        version: None, // Functions don't have versions
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                sources: sql_file_info
                    .sources
                    .iter()
                    .map(|(source, table, location)| DbtSourceWrapper {
                        source: vec![source.to_owned(), table.to_owned()],
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                metrics: vec![],
            },
            __test_attr__: DbtTestAttr {
                column_name: None,
                attached_node: test_path_to_test_asset
                    .get(&dbt_asset.path)
                    .map(|test_asset| {
                        format!(
                            "{}.{}.{}",
                            test_asset.resource_type,
                            test_asset.dbt_asset.package_name,
                            test_asset.resource_name
                        )
                    }),
                test_metadata: inferred_test_metadata.clone(),
                file_key_name: None,
                introspection: IntrospectionKind::None,
            },
            deprecated_config: *test_config.clone(),
            __other__: BTreeMap::new(),
        };

        let components = RelationComponents {
            database: test_config.database.clone(),
            schema: test_config.schema.clone(),
            alias: test_config.alias.clone(),
            store_failures: test_config.store_failures,
        };

        // Update with relation components
        update_node_relation_components(
            &mut dbt_test,
            &env,
            &root_project.name,
            package_name,
            base_ctx,
            &components,
            adapter_type,
        )?;

        dbt_test.__common_attr__.raw_code = if is_singular_data_test {
            get_original_file_contents(&arg.io.in_dir, &manifest_original_file_path)
        } else {
            build_data_test_raw_code(inferred_test_metadata, dbt_test.__base_attr__.alias.clone())
        };

        match status {
            ModelStatus::Enabled => {
                if sql_file_info.execute {
                    nodes_with_execute.insert(unique_id.to_owned(), dbt_test);
                } else {
                    nodes.insert(unique_id, Arc::new(dbt_test));
                }
            }
            ModelStatus::Disabled => {
                disabled_tests.insert(unique_id, Arc::new(dbt_test));
            }
            ModelStatus::ParsingFailed => {}
        }
    }

    // Second pass to capture all identifiers with the appropriate context
    // `models_with_execute` should never have overlapping Arc pointers with `models` and `disabled_models`
    // otherwise make_mut will clone the inner model, and the modifications inside this function call will be lost
    let tests_rest = collect_adapter_identifiers_detect_unsafe(
        arg,
        nodes_with_execute,
        node_resolver,
        jinja_env,
        adapter_type,
        package.dbt_project.name.as_str(),
        &root_project.name,
        runtime_config,
        token,
    )
    .await?;

    nodes.extend(
        tests_rest
            .into_iter()
            .map(|(k, _)| (k.__common_attr__.unique_id.clone(), Arc::new(k))),
    );

    Ok((nodes, disabled_tests))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::state::DbtAsset;

    #[test]
    fn test_builds_test_metadata_from_asset_single_col() {
        let asset = GenericTestAsset {
            dbt_asset: DbtAsset {
                base_path: PathBuf::new(),
                path: PathBuf::from("generic_tests/not_null_customers_id.sql"),
                package_name: "pkg".to_string(),
            },
            original_file_path: PathBuf::from("models/schema.yml"),
            resource_name: "customers".to_string(),
            resource_type: "model".to_string(),
            test_name: "not_null_customers_id".to_string(),
            defined_at: Default::default(),
            test_metadata_name: Some("not_null".to_string()),
            test_metadata_namespace: None,
            test_metadata_column_name: Some("id".to_string()),
            test_metadata_combination_of_columns: None,
        };
        let md = test_metadata_from_asset(&asset).expect("metadata");
        assert_eq!(md.name, "not_null");
        assert!(md.namespace.is_none());
        assert_eq!(
            md.kwargs
                .get("column_name")
                .and_then(|v| v.as_str())
                .unwrap(),
            "id"
        );
    }

    #[test]
    fn test_builds_test_metadata_from_asset_combo_cols() {
        let asset = GenericTestAsset {
            dbt_asset: DbtAsset {
                base_path: PathBuf::new(),
                path: PathBuf::from(
                    "generic_tests/unique_combination_of_columns_customers_a__b.sql",
                ),
                package_name: "pkg".to_string(),
            },
            original_file_path: PathBuf::from("models/schema.yml"),
            resource_name: "customers".to_string(),
            resource_type: "model".to_string(),
            test_name: "unique_combination_of_columns_customers_a__b".to_string(),
            defined_at: Default::default(),
            test_metadata_name: Some("unique_combination_of_columns".to_string()),
            test_metadata_namespace: None,
            test_metadata_column_name: None,
            test_metadata_combination_of_columns: Some(vec!["a".to_string(), "b".to_string()]),
        };
        let md = test_metadata_from_asset(&asset).expect("metadata");
        assert_eq!(md.name, "unique_combination_of_columns");
        let vals: Vec<String> = match md.kwargs.get("combination_of_columns").unwrap() {
            YmlValue::Sequence(arr, _) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => panic!("expected sequence for combination_of_columns"),
        };
        assert_eq!(vals, vec!["a", "b"]);
    }
}
