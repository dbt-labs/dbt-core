use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};

use dbt_adapter_core::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::path::DbtPath;
use dbt_common::static_analysis::{
    StaticAnalysisDeprecationOrigin, check_deprecated_static_analysis_kind,
};
use dbt_common::tracing::dbt_emit::emit_error_log_from_fs_error;
use dbt_common::{FsResult, error::AbstractLocation};
use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_jinja_utils::{jinja_environment::JinjaEnv, node_resolver::NodeResolver};
use dbt_schemas::schemas::DbtFunctionAttr;
use dbt_schemas::schemas::common::{Access, DbtQuoting};
use dbt_schemas::schemas::project::FunctionConfig;
use dbt_schemas::schemas::project::ResolvedConfig;
use dbt_schemas::{
    schemas::{
        CommonAttributes, DbtFunction, NodeBaseAttributes,
        common::NodeDependsOn,
        properties::{
            FUNCTION_LANGUAGE_JAVASCRIPT, FUNCTION_LANGUAGE_PYTHON, FunctionKind,
            FunctionProperties,
        },
        ref_and_source::{DbtRef, DbtSourceWrapper},
    },
    state::{DbtPackage, DbtRuntimeConfig, NodeResolverTracker},
};
use minijinja::MacroSpans;

use crate::dbt_project_config::{ProjectConfigResolver, RootProjectConfigs, init_project_config};
use crate::renderer::{RenderCtx, RenderCtxInner};
use crate::resolve::resolve_utils::{build_unrendered_config, extract_config_map};
use crate::utils::{
    RelationComponents, extract_resource_config_from_raw_project, parse_unrendered_config,
    update_node_relation_components,
};
use crate::{
    args::ResolveArgs,
    renderer::{SqlFileRenderResult, render_unresolved_sql_files},
    utils::{get_node_fqn, get_original_file_path, get_unique_id},
};
use dbt_common::tokiofs::read_to_string;

use super::resolve_properties::MinimalPropertiesEntry;

#[allow(clippy::too_many_arguments)]
pub async fn resolve_functions(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_package: &DbtPackage,
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
    let dependency_package_name = dependency_package_name_from_ctx(&env, base_ctx);

    let config_resolver = ProjectConfigResolver::build(
        root_project_configs.functions.clone(),
        dependency_package_name.is_some(),
        || {
            init_project_config(
                &arg.io,
                &package.dbt_project.functions,
                package_quoting,
                dependency_package_name,
            )
        },
    )?
    .with_resolve_defaults(arg.static_analysis.unwrap_or_default());

    let render_ctx = RenderCtx {
        inner: Arc::new(RenderCtxInner {
            args: arg.clone(),
            root_project_name: root_package.dbt_project.name.clone(),
            config_resolver,
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type,
            database: database.to_string(),
            schema: schema.to_string(),
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

    // Raw config sources for `unrendered_config`, mirroring resolve_models.rs. These preserve
    // Jinja as raw strings so that env-aware configs (identical authored Jinja that renders
    // differently per target) are not flagged as `state:modified` (Approach A soundness
    // prerequisite — the Stage-1 wholesale comparison requires a fully populated
    // `unrendered_config`).
    let is_dependency = dependency_package_name.is_some();
    let raw_local_project_config =
        extract_resource_config_from_raw_project(&package.raw_project_yml, "functions");
    let raw_root_project_functions_cfg = if is_dependency {
        Some(extract_resource_config_from_raw_project(
            &root_package.raw_project_yml,
            "functions",
        ))
    } else {
        None
    };

    // Snapshot raw schema.yml `config:` blocks before render_unresolved_sql_files nulls out
    // schema_value entries via std::mem::replace. Keyed by function name.
    let raw_schema_yml_configs: BTreeMap<String, BTreeMap<String, dbt_yaml::Value>> =
        function_properties
            .iter()
            .filter_map(|(key, mpe)| {
                let config_map = extract_config_map(&mpe.schema_value)?;
                Some((key.clone(), config_map))
            })
            .collect();

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
        config: model_config,
        raw_code,
        rendered_sql,
        macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
        ..
    } in function_sql_resources_map.into_iter()
    {
        let function_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();

        let original_file_path =
            get_original_file_path(&dbt_asset.base_path, &arg.io.in_dir, &dbt_asset.path);

        let unique_id = get_unique_id(function_name, package_name, None, "function");
        let static_analysis = model_config.static_analysis.clone();
        if let Some(spanned) = model_config.get_static_analysis() {
            let kind = spanned.into_inner();
            if kind != arg.static_analysis.unwrap_or_default() {
                check_deprecated_static_analysis_kind(
                    kind,
                    StaticAnalysisDeprecationOrigin::NodeConfig {
                        unique_id: unique_id.as_str(),
                    },
                    dependency_package_name,
                    arg.io.status_reporter.as_ref(),
                );
                if dbt_asset.is_python() {
                    crate::validation::warn_python_static_analysis(
                        kind,
                        unique_id.as_str(),
                        arg.io.status_reporter.as_ref(),
                    );
                }
            }
        }

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

        // Capture inline `{{ config(...) }}` overrides from the function SQL file, Jinja preserved.
        let raw_config_call_dict = read_to_string(dbt_asset.base_path.join(&dbt_asset.path))
            .await
            .ok()
            .and_then(|sql| parse_unrendered_config(&sql, false));

        // Merge the four raw sources (project < root < schema.yml < inline) into the node's
        // `unrendered_config`. Functions do not support pre_hook/post_hook, so hook-name
        // normalization is disabled.
        let unrendered_config = build_unrendered_config(
            &fqn,
            &raw_local_project_config,
            raw_root_project_functions_cfg.as_ref(),
            raw_schema_yml_configs.get(function_name),
            raw_config_call_dict.as_ref(),
            false,
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
                path: DbtPath::from(&dbt_asset.path),
                original_file_path,
                patch_path: patch_path.map(DbtPath::from),
                fqn,
                description: properties.description,
                raw_code: Some(raw_code),
                checksum: sql_file_info.checksum,
                language: if dbt_asset.is_python() {
                    Some(FUNCTION_LANGUAGE_PYTHON.to_string())
                } else if dbt_asset.is_javascript() {
                    Some(FUNCTION_LANGUAGE_JAVASCRIPT.to_string())
                } else {
                    properties.language.clone()
                },
                tags: model_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                classifiers: Default::default(),
                meta: model_config.meta.clone().unwrap_or_default(),
            },
            __base_attr__: NodeBaseAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                alias: "".to_owned(),           // will be updated below
                relation_name: None,            // will be updated below
                materialized: dbt_schemas::schemas::common::DbtMaterialization::Function,
                static_analysis,
                static_analysis_off_reason: None,
                compute: None,
                quoting: package_quoting
                    .try_into()
                    .expect("DbtQuoting should be set"),
                quoting_ignore_case: false,
                enabled: model_config.enabled,
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
                        version: version.clone(),
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
                unrendered_config,
            },
            __function_attr__: DbtFunctionAttr {
                access: properties
                    .config
                    .as_ref()
                    .and_then(|c| c.access.clone())
                    .unwrap_or(Access::Private),
                group: properties.config.as_ref().and_then(|c| c.group.clone()),
                language: if dbt_asset.is_python() {
                    Some(FUNCTION_LANGUAGE_PYTHON.to_string())
                } else if dbt_asset.is_javascript() {
                    Some(FUNCTION_LANGUAGE_JAVASCRIPT.to_string())
                } else {
                    properties.language.clone()
                },
                on_configuration_change: properties
                    .config
                    .as_ref()
                    .and_then(|c| c.on_configuration_change.clone()),
                returns: properties.returns.clone(),
                arguments: properties.arguments.clone(),
            },
            // TODO: can we just take model_config and apply function_kind default elsewhere?
            deprecated_config: FunctionConfig {
                enabled: Some(model_config.enabled),
                group: model_config.group.clone(),
                tags: model_config.tags.clone(),
                meta: model_config.meta.clone(),
                function_kind: model_config
                    .function_kind
                    .clone()
                    .or(Some(FunctionKind::Scalar)),
                volatility: model_config.volatility.clone(),
                runtime_version: model_config.runtime_version.clone(),
                entry_point: model_config.entry_point.clone(),
                packages: model_config.packages.clone(),
                snowflake: model_config.snowflake.clone(),
                ..Default::default()
            },
            __other__: BTreeMap::new(),
        };

        let components = RelationComponents {
            database: model_config.database.clone().into_inner().unwrap_or(None),
            schema: model_config.schema.clone().into_inner().unwrap_or(None),
            alias: model_config.alias.clone(),
            store_failures: None,
        };

        // update model components using the generate_relation_components function
        update_node_relation_components(
            &mut function,
            &env,
            &root_package.dbt_project.name,
            package_name,
            base_ctx,
            &components,
            adapter_type,
        )?;

        match node_resolver.insert_function(&function, adapter_type, status) {
            Ok(_) => (),
            Err(e) => {
                let err_with_loc = e.with_location(dbt_asset.path.clone());
                emit_error_log_from_fs_error(&err_with_loc, arg.io.status_reporter.as_ref());
            }
        }

        functions.insert(unique_id, Arc::new(function));
    }

    Ok((functions, rendering_results))
}
