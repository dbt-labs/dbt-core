use std::collections::HashMap;
use std::{collections::BTreeMap, sync::Arc};

use dbt_common::show_error;
use dbt_common::{error::AbstractLocation, FsResult};
use dbt_jinja_utils::{jinja_environment::JinjaEnvironment, refs_and_sources::RefsAndSources};
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::state::{ModelStatus, RefsAndSourcesTracker};
use dbt_schemas::{
    project_configs::ProjectConfigs,
    schemas::{
        common::{DbtContract, NodeDependsOn},
        dbt_column::DbtColumn,
        manifest::{CommonAttributes, DbtModel, ManifestModelConfig, NodeBaseAttributes},
        project::DbtProject,
        properties::ModelProperties,
        ref_and_source::{DbtRef, DbtSourceWrapper},
    },
    state::{DbtPackage, DbtRuntimeConfig},
};
use minijinja::MacroSpans;

use crate::dbt_project_config::{init_project_config, RootProjectConfigs};
use crate::{
    args::ResolveArgs,
    renderer::{render_unresolved_sql_files, SqlFileRenderResult},
    utils::{get_node_fqn, get_original_file_path, get_unique_id, update_node_relation_components},
};

use super::resolve_properties::MinimalPropertiesEntry;

#[allow(clippy::too_many_arguments)]
pub async fn resolve_analyses(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    model_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    database: &str,
    schema: &str,
    adapter_type: &str,
    package_name: &str,
    env: &JinjaEnvironment<'static>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    runtime_config: Arc<DbtRuntimeConfig>,
    refs_and_sources: &mut RefsAndSources,
) -> FsResult<(
    HashMap<String, Arc<DbtModel>>,
    HashMap<String, (String, MacroSpans)>,
)> {
    let mut analyses: HashMap<String, Arc<DbtModel>> = HashMap::new();
    let mut rendering_results: HashMap<String, (String, MacroSpans)> = HashMap::new();

    let local_project_config = init_project_config(
        &arg.io,
        package_quoting,
        &package
            .dbt_project
            .models // TODO
            .as_ref()
            .map(ProjectConfigs::ModelConfigs),
        env,
        base_ctx,
    )?;

    let mut analysis_sql_resources_map = render_unresolved_sql_files::<ModelProperties>(
        arg,
        &package.analysis_files,
        package_name,
        package_quoting,
        adapter_type,
        database,
        schema,
        env,
        base_ctx,
        model_properties,
        root_project.name.as_str(),
        &root_project_configs.models,
        &local_project_config,
        runtime_config.clone(),
        &package
            .dbt_project
            .analysis_paths
            .as_ref()
            .unwrap_or(&vec![])
            .clone(),
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
        let analysis_config = ManifestModelConfig::from(*sql_file_info.config.clone());

        let original_file_path =
            get_original_file_path(&dbt_asset.base_path, &arg.io.in_dir, &dbt_asset.path);

        let unique_id = get_unique_id(analysis_name, package_name, None, "analysis");

        let fqn = get_node_fqn(
            package_name,
            dbt_asset.path.to_owned(),
            vec![analysis_name.to_owned()],
        );

        let properties = if let Some(properties) = maybe_properties {
            properties
        } else {
            ModelProperties::empty(analysis_name.to_owned())
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

        let mut dbt_model = DbtModel {
            common_attr: CommonAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                name: analysis_name.to_owned(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.to_owned(),
                original_file_path,
                unique_id: unique_id.clone(),
                fqn,
                description: properties.description.clone(),
                patch_path,
            },
            base_attr: NodeBaseAttributes {
                alias: "".to_owned(), // will be updated below
                checksum: sql_file_info.checksum.clone(),

                relation_name: None, // will be updated below
                compiled_path: None,
                compiled: None,
                compiled_code: None,
                columns: properties
                    .columns
                    .as_ref()
                    .map(|c| {
                        c.iter()
                            .map(|cp| cp.clone().try_into())
                            .collect::<Result<Vec<DbtColumn>, _>>()
                    })
                    .transpose()?
                    .map(|c| {
                        c.into_iter()
                            .map(|c| (c.name.clone(), c))
                            .collect::<BTreeMap<_, _>>()
                    })
                    .unwrap_or_default(),
                depends_on: NodeDependsOn::default(),
                language: Some("sql".to_string()),
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
                metrics,
                build_path: None,
                contract: DbtContract::default(),
                created_at: None,
                raw_code: None,
                unrendered_config: BTreeMap::new(),
                doc_blocks: None,
                extra_ctes_injected: None,
                extra_ctes: None,
            },
            config: ManifestModelConfig {
                group: analysis_config.group.clone(),
                ..Default::default()
            },
            ..Default::default()
        };

        // update model components using the generate_relation_components function
        update_node_relation_components(
            &mut dbt_model,
            env,
            &root_project.name,
            package_name,
            base_ctx,
            &sql_file_info.config,
            adapter_type,
        )?;

        match refs_and_sources.insert_ref(&dbt_model, adapter_type, status, false) {
            Ok(_) => (),
            Err(e) => {
                show_error!(&arg.io, e.with_location(dbt_asset.path.clone()));
            }
        }

        if status == ModelStatus::Enabled {
            analyses.insert(unique_id.to_owned(), Arc::new(dbt_model));
            rendering_results.insert(
                unique_id.to_owned(),
                (rendered_sql.clone(), macro_spans.clone()),
            );
        }
    }

    Ok((analyses, rendering_results))
}
