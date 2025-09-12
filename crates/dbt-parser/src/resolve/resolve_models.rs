use crate::args::ResolveArgs;
use crate::dbt_project_config::RootProjectConfigs;
use crate::dbt_project_config::init_project_config;
use crate::renderer::RenderCtx;
use crate::renderer::RenderCtxInner;
use crate::renderer::SqlFileRenderResult;
use crate::renderer::collect_adapter_identifiers_detect_unsafe;
use crate::renderer::render_unresolved_sql_files;
use crate::utils::RelationComponents;
use crate::utils::convert_macro_names_to_unique_ids;
use crate::utils::get_node_fqn;
use crate::utils::get_original_file_path;
use crate::utils::get_unique_id;
use crate::utils::update_node_relation_components;

use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::error::AbstractLocation;
use dbt_common::fs_err;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::show_error;
use dbt_common::show_warning;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::refs_and_sources::RefsAndSources;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::CommonAttributes;
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::DbtModelAttr;
use dbt_schemas::schemas::IntrospectionKind;
use dbt_schemas::schemas::NodeBaseAttributes;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::schemas::common::ModelFreshnessRules;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::common::Versions;
use dbt_schemas::schemas::dbt_column::ColumnInheritanceRules;
use dbt_schemas::schemas::dbt_column::ColumnProperties;
use dbt_schemas::schemas::dbt_column::DbtColumnRef;
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::nodes::AdapterAttr;
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::properties::ModelProperties;
use dbt_schemas::schemas::ref_and_source::{DbtRef, DbtSourceWrapper};
use dbt_schemas::state::DbtPackage;
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_schemas::state::GenericTestAsset;
use dbt_schemas::state::ModelStatus;
use dbt_schemas::state::RefsAndSourcesTracker;
use minijinja::MacroSpans;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;
use super::resolve_tests::persist_generic_data_tests::TestableNodeTrait;

#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_arguments)]
pub async fn resolve_models(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    models_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    database: &str,
    schema: &str,
    adapter_type: AdapterType,
    package_name: &str,
    env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    runtime_config: Arc<DbtRuntimeConfig>,
    collected_generic_tests: &mut Vec<GenericTestAsset>,
    refs_and_sources: &mut RefsAndSources,
    token: &CancellationToken,
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

    let local_project_config = if package.dbt_project.name == root_project.name {
        root_project_configs.models.clone()
    } else {
        init_project_config(
            &arg.io,
            &package.dbt_project.models,
            ModelConfig {
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
            root_project_config: root_project_configs.models.clone(),
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type,
            database: database.to_string(),
            schema: schema.to_string(),
            local_project_config: local_project_config.clone(),
            resource_paths: package
                .dbt_project
                .model_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        }),
        jinja_env: env.clone(),
        runtime_config: runtime_config.clone(),
    };

    // HACK: strip semantic resources out of all model properties
    // this is because semantic resources have fields that have jinja expressions
    // but should not be rendered (they are hydrated verbatim in manifest.json)
    //
    // This is a hack because we treat models and models.metrics differently in an attempt
    // for only-once parsing of model yaml properties in resolver.rs, which duplicates the knowledge
    // that you must treat them separately, such as the removal of semantic properties here.
    let mut models_properties_sans_semantics: BTreeMap<String, MinimalPropertiesEntry> =
        BTreeMap::new();
    models_properties.iter().for_each(|(model_key, v)| {
        let mut v = v.clone();
        v.schema_value.as_mapping_mut().map(|m| {
            m.remove("metrics");
            // derived semantics shouldn't have jinja, but also has no use in this function
            m.remove("derived_semantics")
        });

        models_properties_sans_semantics.insert(model_key.clone(), v);
    });

    let mut model_sql_resources_map: Vec<SqlFileRenderResult<ModelConfig, ModelProperties>> =
        // FIXME -- this attempts to deserialize the model properties
        // and renders jinja but we shouldn't be doing so with metrics.filter
        render_unresolved_sql_files::<ModelConfig, ModelProperties>(
            &render_ctx,
            &package.model_sql_files,
            &mut models_properties_sans_semantics,
            token,
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
        macro_calls,
        properties: maybe_properties,
        status,
        patch_path,
    } in model_sql_resources_map.into_iter()
    {
        let ref_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
        // Is there a better way to handle this if the model doesn't have a config?
        let mut model_config = *sql_file_info.config;
        if model_config.materialized.is_none() {
            model_config.materialized = Some(DbtMaterialization::View);
        }

        let model_name = models_properties_sans_semantics
            .get(ref_name)
            .map(|mpe| mpe.name.clone())
            .unwrap_or_else(|| ref_name.to_owned());

        let maybe_version = models_properties_sans_semantics
            .get(ref_name)
            .and_then(|mpe| mpe.version_info.as_ref().map(|v| v.version.clone()));

        let maybe_latest_version = models_properties_sans_semantics
            .get(ref_name)
            .and_then(|mpe| mpe.version_info.as_ref().map(|v| v.latest_version.clone()));

        let unique_id = get_unique_id(&model_name, package_name, maybe_version.clone(), "model");

        model_config.enabled = Some(!(status == ModelStatus::Disabled));

        if let Some(freshness) = &model_config.freshness {
            ModelFreshnessRules::validate(freshness.build_after.as_ref()).map_err(|e| {
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
        let fqn = get_node_fqn(
            package_name,
            dbt_asset.path.to_owned(),
            fqn_components,
            package.dbt_project.model_paths.as_ref().unwrap_or(&vec![]),
        );

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

        let mut columns = process_columns(
            properties.columns.as_ref(),
            model_config.meta.clone(),
            model_config.tags.clone().map(|tags| tags.into()),
        )?;

        if let Some(versions) = &properties.versions {
            columns = process_versioned_columns(
                &model_config,
                maybe_version.as_ref(),
                versions,
                columns,
            )?;
        }

        validate_merge_update_columns_xor(&model_config, &dbt_asset.path)?;

        if let Some(freshness) = &model_config.freshness {
            ModelFreshnessRules::validate(freshness.build_after.as_ref())?;
        }

        // Create the DbtModel with all properties already set
        let mut dbt_model = DbtModel {
            __common_attr__: CommonAttributes {
                name: model_name.to_owned(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.to_owned(),
                name_span: dbt_common::Span::default(),
                original_file_path,
                patch_path: patch_path.clone(),
                unique_id: unique_id.clone(),
                fqn,
                description: model_config
                    .description
                    .clone()
                    .or_else(|| properties.description.clone()),
                checksum: sql_file_info.checksum.clone(),
                raw_code: Some("--placeholder--".to_string()),
                language: Some("sql".to_string()),
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
                enabled: model_config.enabled.unwrap_or(true),
                extended_model: false,
                persist_docs: model_config.persist_docs.clone(),
                columns,
                depends_on: NodeDependsOn {
                    macros: convert_macro_names_to_unique_ids(&macro_calls),
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
                sources: sql_file_info
                    .sources
                    .iter()
                    .map(|(source, table, location)| DbtSourceWrapper {
                        source: vec![source.to_owned(), table.to_owned()],
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                metrics,
                materialized: model_config
                    .materialized
                    .clone()
                    .expect("materialized is required"),
                quoting: model_config
                    .quoting
                    .expect("quoting is required")
                    .try_into()
                    .expect("quoting is required"),
                quoting_ignore_case: model_config
                    .quoting
                    .unwrap_or_default()
                    .snowflake_ignore_case
                    .unwrap_or(false),
                static_analysis: model_config
                    .static_analysis
                    .unwrap_or(StaticAnalysisKind::On),
            },
            __model_attr__: DbtModelAttr {
                introspection: IntrospectionKind::None,
                version: maybe_version.map(|v| v.into()),
                latest_version: maybe_latest_version.map(|v| v.into()),
                constraints: model_constraints,
                deprecation_date: None,
                primary_key: vec![],
                time_spine: None,
                access: model_config.access.clone().unwrap_or_default(),
                group: model_config.group.clone(),
                contract: model_config.contract.clone(),
                incremental_strategy: model_config.incremental_strategy.clone(),
                freshness: model_config.freshness.clone(),
                event_time: model_config.event_time.clone(),
            },
            __adapter_attr__: AdapterAttr::from_config_and_dialect(
                &model_config.__warehouse_specific_config__,
                adapter_type,
            ),
            // Derived from the model config
            deprecated_config: model_config.clone(),
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
            &mut dbt_model,
            &env,
            &root_project.name,
            package_name,
            base_ctx,
            &components,
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

                properties.as_testable().persist(
                    package_name,
                    &root_project.name,
                    collected_generic_tests,
                    adapter_type,
                    &arg.io,
                    patch_path.as_ref().unwrap_or(&dbt_asset.path),
                )?;
            }
            ModelStatus::Disabled => {
                disabled_models.insert(unique_id.to_owned(), model.clone());
            }
            ModelStatus::ParsingFailed => {}
        }
    }

    for (model_name, mpe) in models_properties_sans_semantics.iter() {
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
                format!("Found duplicate model '{model_name}' with version '{version}'")
            } else {
                format!("Found duplicate model '{model_name}'")
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
        &root_project.name,
        runtime_config,
        token,
    )
    .await?;
    models.extend(models_with_execute);

    Ok((models, rendering_results, disabled_models))
}

fn process_versioned_columns(
    model_config: &ModelConfig,
    maybe_version: Option<&String>,
    versions: &[Versions],
    columns: BTreeMap<String, DbtColumnRef>,
) -> Result<BTreeMap<String, DbtColumnRef>, Box<dbt_common::FsError>> {
    for version in versions.iter() {
        if maybe_version.is_some_and(|v| Some(v) == version.get_version().as_ref()) {
            if let Some(column_props) = version.__additional_properties__.get("columns") {
                let column_map: Vec<ColumnProperties> = column_props
                    .as_sequence()
                    .map(|cols| {
                        cols.iter()
                            .filter_map(|col| col.as_mapping())
                            .filter(|map| {
                                !(map.contains_key("include") || map.contains_key("exclude"))
                            })
                            .filter_map(|map| {
                                dbt_serde_yaml::from_value::<ColumnProperties>(map.clone().into())
                                    .ok()
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let mut versioned_columns = process_columns(
                    Some(&column_map),
                    model_config.meta.clone(),
                    model_config.tags.clone().map(|tags| tags.into()),
                )?;

                if let Some(rules) = ColumnInheritanceRules::from_version_columns(column_props) {
                    columns
                        .iter()
                        .filter(|(name, _)| rules.should_include_column(name))
                        .for_each(|(name, col)| {
                            versioned_columns.insert(name.clone(), col.clone());
                        });
                }
                return Ok(versioned_columns);
            }
        }
    }

    Ok(columns)
}

pub fn validate_merge_update_columns_xor(model_config: &ModelConfig, path: &Path) -> FsResult<()> {
    if model_config.merge_update_columns.is_some() && model_config.merge_exclude_columns.is_some() {
        let err = fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => path.to_path_buf(),
            "merge_update_columns and merge_exclude_columns cannot both be set",
        );
        return Err(err);
    }
    Ok(())
}
