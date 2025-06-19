use crate::args::ResolveArgs;
use crate::dbt_project_config::init_project_config;
use crate::dbt_project_config::RootProjectConfigs;
use crate::renderer::render_unresolved_sql_files;
use crate::renderer::SqlFileRenderResult;
use crate::resolve::resolve_properties::MinimalPropertiesEntry;
use crate::utils::generate_relation_components;
use crate::utils::get_node_fqn;
use crate::utils::get_original_file_path;
use crate::utils::update_node_relation_components;
use crate::utils::RelationComponents;
use dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME;
use dbt_common::error::AbstractLocation;
use dbt_common::io_utils::try_read_yml_to_str;
use dbt_common::stdfs;
use dbt_common::FsResult;
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_schemas::schemas::common::DbtChecksum;
use dbt_schemas::schemas::common::DbtContract;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::schemas::common::DocsConfig;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::project::DataTestConfig;
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::project::DefaultTo;
use dbt_schemas::schemas::properties::DataTestProperties;
use dbt_schemas::schemas::properties::ModelProperties;
use dbt_schemas::schemas::ref_and_source::DbtRef;
use dbt_schemas::schemas::ref_and_source::DbtSourceWrapper;
use dbt_schemas::schemas::{CommonAttributes, DbtTest, InternalDbtNode, NodeBaseAttributes};
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_schemas::state::ModelStatus;
use dbt_schemas::state::{DbtAsset, DbtPackage};
use minijinja::constants::DEFAULT_TEST_SCHEMA;
use minijinja::Value;
use serde::de;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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
    adapter_type: &str,
    env: &JinjaEnvironment<'static>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    runtime_config: Arc<DbtRuntimeConfig>,
    collected_tests: &Vec<DbtAsset>,
) -> FsResult<(HashMap<String, Arc<DbtTest>>, HashMap<String, Arc<DbtTest>>)> {
    let mut nodes: HashMap<String, Arc<DbtTest>> = HashMap::new();
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
    )?;

    let mut test_assets_to_render = package.test_files.clone();
    test_assets_to_render.extend(collected_tests.to_owned());
    // Note (Ani):Tests have a different jinja context, need to render them separately

    let mut test_sql_resources_map =
        render_unresolved_sql_files::<DataTestConfig, DataTestProperties>(
            arg,
            &test_assets_to_render,
            package_name,
            package_quoting,
            adapter_type,
            database,
            schema,
            env,
            base_ctx,
            test_properties,
            root_project.name.as_str(),
            &root_project_configs.tests,
            &local_project_config,
            runtime_config.clone(),
            &package
                .dbt_project
                .test_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
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

        let model_name = dbt_asset
            .path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let properties = if let Some(properties) = maybe_properties {
            properties
        } else {
            &DataTestProperties::empty(model_name.to_owned())
        };

        let unique_id = format!("test.{}.{}", package_name, model_name);
        let fqn = get_node_fqn(
            package_name,
            dbt_asset.path.to_owned(),
            vec![model_name.to_owned()],
        );
        // merge schema_file_info
        let columns_map = BTreeMap::new();

        // Errored models can be enabled, so enabled is set to the opposite of disabled
        test_config.enabled = Some(!(*status == ModelStatus::Disabled));

        let mut dbt_test = DbtTest {
            common_attr: CommonAttributes {
                database: database.to_owned(),
                schema: schema.to_owned(),
                name: model_name.to_owned(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.to_owned(),
                original_file_path: get_original_file_path(
                    &dbt_asset.base_path,
                    &arg.io.in_dir,
                    &dbt_asset.path,
                ),
                patch_path: patch_path.clone(),
                unique_id: unique_id.clone(),
                fqn,
                description: properties.description.clone(),
            },
            base_attr: NodeBaseAttributes {
                alias: "will_be_updated_below".to_owned(),
                checksum: DbtChecksum::hash(rendered_sql.as_bytes()),
                relation_name: None,
                compiled_path: None,
                columns: columns_map,
                depends_on: NodeDependsOn::default(),
                language: None,
                raw_code: Some("".to_string()),
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
                sources: sql_file_info
                    .sources
                    .iter()
                    .map(|(source, table, location)| DbtSourceWrapper {
                        source: vec![source.to_owned(), table.to_owned()],
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                build_path: None,
                contract: DbtContract::default(),
                created_at: None,
                metrics: vec![],
                unrendered_config: BTreeMap::new(),
                compiled: None,
                compiled_code: None,
                doc_blocks: None,
                extra_ctes_injected: None,
                extra_ctes: None,
            },
            deprecated_config: *test_config.clone(),
            column_name: None,
            attached_node: None,
            test_metadata: None,
            other: BTreeMap::new(),
            file_key_name: None,
            quoting: test_config
                .quoting
                .expect("quoting is required")
                .try_into()
                .expect("quoting is required"),
            tags: test_config
                .tags
                .clone()
                .map(|tags| tags.into())
                .unwrap_or_default(),
            meta: test_config.meta.clone().unwrap_or_default(),
        };

        let components = RelationComponents {
            database: test_config.database.clone(),
            schema: test_config.schema.clone(),
            alias: test_config.alias.clone(),
            store_failures: None,
        };

        // Update with relation components
        update_node_relation_components(
            &mut dbt_test,
            env,
            &root_project.name,
            package_name,
            base_ctx,
            &components,
            adapter_type,
        )?;

        match status {
            ModelStatus::Enabled => {
                nodes.insert(unique_id, Arc::new(dbt_test));
            }
            ModelStatus::Disabled => {
                disabled_tests.insert(unique_id, Arc::new(dbt_test));
            }
            ModelStatus::ParsingFailed => {}
        }
    }

    Ok((nodes, disabled_tests))
}
