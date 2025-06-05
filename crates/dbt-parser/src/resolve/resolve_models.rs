use crate::args::ResolveArgs;
use crate::dbt_project_config::init_project_config;
use crate::dbt_project_config::RootProjectConfigs;
use crate::renderer::collect_adapter_identifiers_detect_unsafe;
use crate::renderer::render_unresolved_sql_files;
use crate::renderer::SqlFileRenderResult;
use crate::utils::get_node_fqn;
use crate::utils::get_original_file_path;
use crate::utils::get_unique_id;
use crate::utils::update_node_relation_components;

use dbt_common::error::AbstractLocation;
use dbt_common::fs_err;
use dbt_common::show_error;
use dbt_common::show_warning;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_jinja_utils::refs_and_sources::RefsAndSources;
use dbt_schemas::project_configs::ProjectConfigs;
use dbt_schemas::schemas::common::DbtContract;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::schemas::common::FreshnessRules;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::manifest::CommonAttributes;
use dbt_schemas::schemas::manifest::NodeBaseAttributes;
use dbt_schemas::schemas::manifest::{DbtModel, ManifestModelConfig};
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::properties::ModelProperties;
use dbt_schemas::schemas::ref_and_source::{DbtRef, DbtSourceWrapper};
use dbt_schemas::state::DbtAsset;
use dbt_schemas::state::DbtPackage;
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_schemas::state::ModelStatus;
use dbt_schemas::state::RefsAndSourcesTracker;
use minijinja::MacroSpans;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;
use super::resolve_tests::persist_generic_data_tests::TestableNodeTrait;

#[allow(clippy::too_many_arguments)]
pub async fn resolve_models(
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
    collected_tests: &mut Vec<DbtAsset>,
    refs_and_sources: &mut RefsAndSources,
) -> FsResult<(
    HashMap<String, Arc<DbtModel>>,
    HashMap<String, (String, MacroSpans)>,
    HashMap<String, Arc<DbtModel>>,
)> {
    let mut models: HashMap<String, Arc<DbtModel>> = HashMap::new();
    let mut models_with_execute = HashMap::new();
    let mut disabled_models: HashMap<String, Arc<DbtModel>> = HashMap::new();
    let mut node_names = HashSet::new();
    let mut rendering_results: HashMap<String, (String, MacroSpans)> = HashMap::new();

    let local_project_config = init_project_config(
        &arg.io,
        package_quoting,
        &package
            .dbt_project
            .models
            .as_ref()
            .map(ProjectConfigs::ModelConfigs),
        env,
        base_ctx,
    )?;
    let mut model_sql_resources_map: Vec<SqlFileRenderResult<ModelProperties>> =
        render_unresolved_sql_files::<ModelProperties>(
            arg,
            &package.model_sql_files,
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
                .model_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        )
        .await?;
    // make deterministic
    model_sql_resources_map.sort_by(|a, b| {
        a.asset
            .path
            .file_name()
            .cmp(&b.asset.path.file_name())
            .then(a.asset.path.cmp(&b.asset.path))
    });

    // Initialize a counter struct to track the version of each model
    let mut duplicates = Vec::new();
    for SqlFileRenderResult {
        asset: dbt_asset,
        sql_file_info,
        rendered_sql,
        macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
    } in model_sql_resources_map.into_iter()
    {
        let ref_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
        // Is there a better way to handle this if the model doesn't have a config?
        let mut model_config = ManifestModelConfig::from(*sql_file_info.config.clone());
        if model_config.materialized.is_none() {
            model_config.materialized = Some(DbtMaterialization::View);
        }

        let model_name = model_properties
            .get(ref_name)
            .map(|mpe| mpe.name.clone())
            .unwrap_or_else(|| ref_name.to_owned());

        let maybe_version = model_properties
            .get(ref_name)
            .and_then(|mpe| mpe.version_info.as_ref().map(|v| v.version.clone()));

        let maybe_latest_version = model_properties
            .get(ref_name)
            .and_then(|mpe| mpe.version_info.as_ref().map(|v| v.latest_version.clone()));

        let unique_id = get_unique_id(&model_name, package_name, maybe_version.clone(), "model");

        model_config.enabled = Some(!(status == ModelStatus::Disabled));

        if let Some(freshness) = &model_config.freshness {
            FreshnessRules::validate(freshness.build_after.as_ref()).map_err(|e| {
                fs_err!(
                    code => ErrorCode::InvalidConfig,
                    loc => dbt_asset.path.clone(),
                    "{}",
                    e
                )
            })?;
        }

        // Keep track of duplicates (often happens with versioned models)
        if (models.contains_key(&unique_id) || models_with_execute.contains_key(&unique_id))
            && !(status == ModelStatus::Disabled)
        {
            duplicates.push((
                unique_id.clone(),
                model_name.clone(),
                maybe_version.clone(),
                dbt_asset.path.clone(),
            ));
            continue;
        }

        let original_file_path =
            get_original_file_path(&dbt_asset.base_path, &arg.io.in_dir, &dbt_asset.path);

        // Model fqn includes v{version} for versioned models
        let fqn_components = if let Some(version) = &maybe_version {
            vec![model_name.to_owned(), format!("v{}", version)]
        } else {
            vec![model_name.to_owned()]
        };
        let fqn = get_node_fqn(package_name, dbt_asset.path.to_owned(), fqn_components);

        let properties = if let Some(properties) = maybe_properties {
            properties
        } else {
            ModelProperties::empty(model_name.to_owned())
        };
        let model_constraints = properties.constraints.clone().unwrap_or_default();

        // Iterate over metrics and construct the dependencies
        let mut metrics = Vec::new();
        for (metric, package) in sql_file_info.metrics.iter() {
            if let Some(package_str) = package {
                metrics.push(vec![package_str.to_owned(), metric.to_owned()]);
            } else {
                metrics.push(vec![metric.to_owned()]);
            }
        }

        let columns = process_columns(properties.columns.as_ref(), &model_config.clone().into())?;

        // Create the DbtModel with all properties already set
        let mut dbt_model = DbtModel {
            common_attr: CommonAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                name: model_name.to_owned(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.to_owned(),
                original_file_path,
                patch_path,
                unique_id: unique_id.clone(),
                fqn,
                description: properties.description.clone(),
            },
            base_attr: NodeBaseAttributes {
                alias: "".to_owned(), // will be updated below
                checksum: sql_file_info.checksum.clone(),
                relation_name: None, // will be updated below
                compiled_path: None,
                compiled: None,
                compiled_code: None,
                columns,
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
                raw_code: Some("--placeholder--".to_string()),
                unrendered_config: BTreeMap::new(),
                doc_blocks: None,
                extra_ctes_injected: None,
                extra_ctes: None,
            },
            introspection: None,
            version: maybe_version.map(|v| v.into()),
            latest_version: maybe_latest_version.map(|v| v.into()),
            constraints: model_constraints,
            config: model_config.clone(),
            other: BTreeMap::new(),
            deprecation_date: None,
            primary_key: vec![],
            time_spine: None,
            is_extended_model: false,
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

        let model = Arc::new(dbt_model);
        match status {
            ModelStatus::Enabled => {
                // merge them later for the returned models
                if sql_file_info.execute {
                    models_with_execute.insert(unique_id.to_owned(), model.clone());
                } else {
                    models.insert(unique_id.to_owned(), model.clone());
                }
                node_names.insert(model_name.to_owned());
                rendering_results.insert(unique_id, (rendered_sql.clone(), macro_spans.clone()));

                properties
                    .as_testable()
                    .persist(package_name, &arg.io.out_dir, collected_tests)?;
            }
            ModelStatus::Disabled => {
                disabled_models.insert(unique_id.to_owned(), model.clone());
            }
            ModelStatus::ParsingFailed => {}
        }
    }

    for (model_name, mpe) in model_properties.iter() {
        // Skip until we support better error messages for versioned models
        if mpe.version_info.is_some() {
            continue;
        }
        if !mpe.schema_value.is_null() {
            // Validate that the model is not latest and flattened
            let err = fs_err!(
                code =>ErrorCode::InvalidConfig,
                loc => mpe.relative_path.clone(),
                "Unused schema.yml entry for model '{}'",
                model_name,
            );
            show_warning!(&arg.io, err);
        }
    }

    // Report duplicates
    if !duplicates.is_empty() {
        let mut errs = Vec::new();
        for (_, model_name, maybe_version, path) in duplicates {
            let msg = if let Some(version) = maybe_version {
                format!(
                    "Found duplicate model '{}' with version '{}'",
                    model_name, version
                )
            } else {
                format!("Found duplicate model '{}'", model_name)
            };
            let err = fs_err!(
                code => ErrorCode::InvalidConfig,
                loc => path.clone(),
                "{}",
                msg,
            );
            errs.push(err);
        }
        while let Some(err) = errs.pop() {
            if errs.is_empty() {
                return Err(err);
            }
            show_error!(&arg.io, err);
        }
    }

    // Second pass to capture all identifiers with the appropriate context
    // `models_with_execute` should never have overlapping Arc pointers with `models` and `disabled_models`
    // otherwise make_mut will clone the inner model, and the modifications inside this function call will be lost
    collect_adapter_identifiers_detect_unsafe(
        arg,
        &mut models_with_execute,
        refs_and_sources,
        env,
        adapter_type,
        package_name,
        runtime_config,
    )
    .await?;
    models.extend(models_with_execute);

    Ok((models, rendering_results, disabled_models))
}
