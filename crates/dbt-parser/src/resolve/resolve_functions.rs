use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};

use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::{FsResult, error::AbstractLocation, show_error};
use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_jinja_utils::{jinja_environment::JinjaEnv, node_resolver::NodeResolver};
use dbt_schemas::schemas::DbtFunctionAttr;
use dbt_schemas::schemas::common::{Access, DbtQuoting};
use dbt_schemas::schemas::project::FunctionConfig;
use dbt_schemas::{
    schemas::{
        CommonAttributes, DbtFunction, NodeBaseAttributes,
        common::NodeDependsOn,
        project::DbtProject,
        properties::FunctionProperties,
        ref_and_source::{DbtRef, DbtSourceWrapper},
    },
    state::{DbtPackage, DbtRuntimeConfig, NodeResolverTracker},
};
use minijinja::MacroSpans;

use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::renderer::{RenderCtx, RenderCtxInner};
use crate::utils::{RelationComponents, update_node_relation_components};
use crate::{
    args::ResolveArgs,
    renderer::{SqlFileRenderResult, render_unresolved_sql_files},
    utils::{get_node_fqn, get_original_file_path, get_unique_id},
};

use super::resolve_properties::MinimalPropertiesEntry;

#[allow(clippy::too_many_arguments)]
pub async fn resolve_functions(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    function_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    database: &str,
    schema: &str,
    adapter_type: AdapterType,
    package_name: &str,
    env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    runtime_config: Arc<DbtRuntimeConfig>,
    node_resolver: &mut NodeResolver,
    token: &CancellationToken,
) -> FsResult<(
    HashMap<String, Arc<DbtFunction>>,
    HashMap<String, (String, MacroSpans)>,
)> {
    let mut functions: HashMap<String, Arc<DbtFunction>> = HashMap::new();
    let mut rendering_results: HashMap<String, (String, MacroSpans)> = HashMap::new();

    let local_project_config = if package.dbt_project.name == root_project.name {
        root_project_configs.functions.clone()
    } else {
        init_project_config(
            &arg.io,
            &package.dbt_project.functions,
            FunctionConfig {
                enabled: Some(true),
                quoting: Some(package_quoting),
                ..Default::default()
            },
            dependency_package_name_from_ctx(&env, base_ctx),
        )?
    };

    let render_ctx = RenderCtx {
        inner: Arc::new(RenderCtxInner {
            args: arg.clone(),
            root_project_name: root_project.name.clone(),
            root_project_config: root_project_configs.functions.clone(),
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type,
            database: database.to_string(),
            schema: schema.to_string(),
            local_project_config: local_project_config.clone(),
            resource_paths: package
                .dbt_project
                .function_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        }),
        jinja_env: env.clone(),
        runtime_config: runtime_config.clone(),
    };

    let mut function_sql_resources_map =
        render_unresolved_sql_files::<FunctionConfig, FunctionProperties>(
            &render_ctx,
            &package.function_sql_files,
            function_properties,
            token,
            Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default()),
        )
        .await?;
    // make deterministic
    function_sql_resources_map.sort_by(|a, b| {
        a.asset
            .path
            .file_name()
            .cmp(&b.asset.path.file_name())
            .then(a.asset.path.cmp(&b.asset.path))
    });

    for SqlFileRenderResult {
        asset: dbt_asset,
        sql_file_info,
        rendered_sql,
        macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
    } in function_sql_resources_map.into_iter()
    {
        let function_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
        let model_config = *sql_file_info.config;

        let original_file_path =
            get_original_file_path(&dbt_asset.base_path, &arg.io.in_dir, &dbt_asset.path);

        let unique_id = get_unique_id(function_name, package_name, None, "function");

        let fqn = get_node_fqn(
            package_name,
            dbt_asset.path.to_owned(),
            vec![function_name.to_owned()],
            package
                .dbt_project
                .function_paths
                .as_ref()
                .unwrap_or(&vec![]),
        );

        let properties = if let Some(properties) = maybe_properties {
            properties
        } else {
            FunctionProperties::empty(function_name.to_owned())
        };

        let depends_on = NodeDependsOn {
            macros: vec![],
            nodes: vec![],
            nodes_with_ref_location: vec![],
        };

        rendering_results.insert(unique_id.clone(), (rendered_sql.clone(), macro_spans));

        let mut function = DbtFunction {
            __common_attr__: CommonAttributes {
                unique_id: unique_id.clone(),
                name: function_name.to_owned(),
                name_span: Default::default(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.clone(),
                original_file_path,
                patch_path,
                fqn,
                description: properties.description,
                raw_code: Some(rendered_sql.clone()),
                checksum: sql_file_info.checksum,
                language: properties.language.clone(),
                tags: model_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: model_config.meta.clone().unwrap_or_default(),
            },
            __base_attr__: NodeBaseAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                alias: "".to_owned(),           // will be updated below
                relation_name: None,            // will be updated below
                materialized: dbt_schemas::schemas::common::DbtMaterialization::Function,
                static_analysis: StaticAnalysisKind::On,
                static_analysis_off_reason: None,
                quoting: package_quoting
                    .try_into()
                    .expect("DbtQuoting should be set"),
                quoting_ignore_case: false,
                enabled: model_config.enabled.unwrap_or(true),
                extended_model: false,
                persist_docs: None,
                columns: vec![],
                depends_on,
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
            __function_attr__: DbtFunctionAttr {
                access: properties
                    .config
                    .as_ref()
                    .and_then(|c| c.access.clone())
                    .unwrap_or(Access::Private),
                group: properties.config.as_ref().and_then(|c| c.group.clone()),
                language: properties.language.clone(),
                on_configuration_change: properties
                    .config
                    .as_ref()
                    .and_then(|c| c.on_configuration_change.clone()),
                returns: properties.returns.clone(),
                arguments: properties.arguments.clone(),
                function_kind: properties.function_kind.clone().unwrap_or_default(),
            },
            deprecated_config: FunctionConfig {
                enabled: model_config.enabled,
                group: model_config.group.clone(),
                tags: model_config.tags.clone(),
                meta: model_config.meta.clone(),
                ..Default::default()
            },
            __other__: BTreeMap::new(),
        };

        let components = RelationComponents {
            database: model_config.database.into_inner().unwrap_or(None),
            schema: model_config.schema.into_inner().unwrap_or(None),
            alias: model_config.alias.clone(),
            store_failures: None,
        };

        // update model components using the generate_relation_components function
        update_node_relation_components(
            &mut function,
            &env,
            &root_project.name,
            package_name,
            base_ctx,
            &components,
            adapter_type,
        )?;

        match node_resolver.insert_function(&function, adapter_type, status) {
            Ok(_) => (),
            Err(e) => {
                show_error!(&arg.io, e.with_location(dbt_asset.path.clone()));
            }
        }

        functions.insert(unique_id, Arc::new(function));
    }

    Ok((functions, rendering_results))
}
