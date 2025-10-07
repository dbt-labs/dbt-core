use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};

use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::{ErrorCode, FsResult, error::AbstractLocation, fs_err, show_warning};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::common::{DbtMaterialization, DbtQuoting, ResolvedQuoting};
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::project::AnalysesConfig;
use dbt_schemas::state::ModelStatus;
use dbt_schemas::{
    schemas::{
        CommonAttributes, DbtAnalysis, DbtAnalysisAttr, NodeBaseAttributes,
        common::NodeDependsOn,
        project::DbtProject,
        properties::AnalysesProperties,
        ref_and_source::{DbtRef, DbtSourceWrapper},
    },
    state::{DbtPackage, DbtRuntimeConfig},
};
use minijinja::MacroSpans;

use super::resolve_properties::MinimalPropertiesEntry;
use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::renderer::{RenderCtx, RenderCtxInner};
use crate::utils::{RelationComponents, update_node_relation_components};
use crate::{
    args::ResolveArgs,
    renderer::{SqlFileRenderResult, render_unresolved_sql_files},
    utils::{get_node_fqn, get_original_file_path, get_unique_id},
};

/// Resolve analysis resources for a package into models and rendered SQL, updating refs/sources.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_analyses(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    analysis_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    database: &str,
    schema: &str,
    adapter_type: AdapterType,
    package_name: &str,
    env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    runtime_config: Arc<DbtRuntimeConfig>,
    token: &CancellationToken,
) -> FsResult<(
    HashMap<String, Arc<DbtAnalysis>>,
    HashMap<String, (String, MacroSpans)>,
)> {
    let mut analyses: HashMap<String, Arc<DbtAnalysis>> = HashMap::new();
    let mut rendering_results: HashMap<String, (String, MacroSpans)> = HashMap::new();
    let jinja_type_checking_event_listener_factory =
        Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default());

    let local_project_config = if package.dbt_project.name == root_project.name {
        root_project_configs.analyses.clone()
    } else {
        init_project_config(
            &arg.io,
            &package.dbt_project.analyses,
            AnalysesConfig {
                enabled: Some(true),
                static_analysis: Some(StaticAnalysisKind::Off),
                ..Default::default()
            },
            dependency_package_name_from_ctx(&env, base_ctx),
        )?
    };

    let render_ctx = RenderCtx {
        inner: Arc::new(RenderCtxInner {
            args: arg.clone(),
            root_project_name: root_project.name.clone(),
            root_project_config: root_project_configs.analyses.clone(),
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type,
            database: database.to_string(),
            schema: schema.to_string(),
            local_project_config,
            resource_paths: package
                .dbt_project
                .analysis_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        }),
        jinja_env: env.clone(),
        runtime_config: runtime_config.clone(),
    };

    let mut analysis_sql_resources_map =
        render_unresolved_sql_files::<AnalysesConfig, AnalysesProperties>(
            &render_ctx,
            &package.analysis_files,
            analysis_properties,
            token,
            jinja_type_checking_event_listener_factory.clone(),
        )
        .await?;
    // make deterministic
    analysis_sql_resources_map.sort_by(|a, b| {
        a.asset
            .path
            .file_name()
            .cmp(&b.asset.path.file_name())
            .then(a.asset.path.cmp(&b.asset.path))
    });

    let all_depends_on = jinja_type_checking_event_listener_factory
        .all_depends_on
        .read()
        .unwrap()
        .clone();

    for SqlFileRenderResult {
        asset: dbt_asset,
        sql_file_info,
        rendered_sql,
        macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
    } in analysis_sql_resources_map.into_iter()
    {
        let analysis_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
        let analysis_config = *sql_file_info.config;

        let original_file_path =
            get_original_file_path(&dbt_asset.base_path, &arg.io.in_dir, &dbt_asset.path);

        // TODO: Implement statement splitting to create multiple analysis nodes per file
        // For now, create a single analysis node per file (index 0)
        // Each statement should get its own node with suffix: analysis.project.filename.0, analysis.project.filename.1, etc.
        // let statement_index = 0;
        let unique_id = get_unique_id(analysis_name, package_name, None, "analysis");
        // unique_id.push_str(&format!(".{statement_index}"));

        let fqn = get_node_fqn(
            package_name,
            dbt_asset.path.to_owned(),
            vec![analysis_name.to_owned()],
            package
                .dbt_project
                .analysis_paths
                .as_ref()
                .unwrap_or(&vec![]),
        );

        let properties = if let Some(properties) = maybe_properties {
            properties
        } else {
            AnalysesProperties::empty(analysis_name.to_owned())
        };

        // Iterate over metrics and construct the dependencies
        let mut metrics = Vec::new();
        for (metric, package) in sql_file_info.metrics.iter() {
            if let Some(package_str) = package {
                metrics.push(vec![package_str.to_owned(), metric.to_owned()]);
            } else {
                metrics.push(vec![metric.to_owned()]);
            }
        }
        let columns = process_columns(
            properties.columns.as_ref(),
            analysis_config.meta.clone(),
            analysis_config.tags.clone().map(|tags| tags.into()),
        )?;

        let is_enabled = matches!(status, ModelStatus::Enabled);
        let macro_depends_on = all_depends_on
            .get(&format!("{package_name}.{analysis_name}"))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();

        let mut dbt_analysis = DbtAnalysis {
            __common_attr__: CommonAttributes {
                name: analysis_name.to_owned(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.to_owned(),
                name_span: dbt_common::Span::default(),
                original_file_path,
                unique_id: unique_id.clone(),
                fqn,
                description: properties.description.clone(),
                patch_path,
                checksum: sql_file_info.checksum.clone(),
                language: Some("sql".to_string()),
                raw_code: None,
                tags: analysis_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: analysis_config.meta.clone().unwrap_or_default(),
            },
            __base_attr__: NodeBaseAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                alias: "".to_owned(),           // will be updated below
                relation_name: None,            // will be updated below
                enabled: is_enabled,
                extended_model: false,
                persist_docs: None,
                materialized: DbtMaterialization::Analysis,
                quoting: ResolvedQuoting::trues(),
                quoting_ignore_case: false,
                static_analysis: analysis_config
                    .static_analysis
                    .unwrap_or(StaticAnalysisKind::Unsafe),
                static_analysis_off_reason: None,
                columns,
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
                metrics,
            },
            __analysis_attr__: DbtAnalysisAttr::default(),
            deprecated_config: analysis_config,
            __other__: BTreeMap::new(),
        };

        let components = RelationComponents {
            database: None,
            schema: None,
            alias: None,
            store_failures: None,
        };

        // update model components using the generate_relation_components function
        update_node_relation_components(
            &mut dbt_analysis,
            &env,
            &root_project.name,
            package_name,
            base_ctx,
            &components,
            adapter_type,
        )?;

        if status == ModelStatus::Enabled {
            analyses.insert(unique_id.to_owned(), Arc::new(dbt_analysis));
            rendering_results.insert(
                unique_id.to_owned(),
                (rendered_sql.clone(), macro_spans.clone()),
            );
        }
    }

    for (analysis_name, mpe) in analysis_properties.iter() {
        if !mpe.schema_value.is_null() {
            let err = fs_err!(
                code => ErrorCode::InvalidConfig,
                loc => mpe.relative_path.clone(),
                "Unused schema.yml entry for analysis '{}'",
                analysis_name,
            );
            show_warning!(&arg.io, err);
        }
    }

    Ok((analyses, rendering_results))
}
