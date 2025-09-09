use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, init_project_config};
use crate::utils::{get_node_fqn, get_original_file_path, get_unique_id};

use dbt_common::FsResult;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::CommonAttributes;
use dbt_schemas::schemas::common::{DbtChecksum, Dimension, DimensionTypeParams, NodeDependsOn};
use dbt_schemas::schemas::dbt_column::{
    ColumnPropertiesDimension, ColumnPropertiesDimensionConfig, Entity, EntityConfig,
};
use dbt_schemas::schemas::manifest::semantic_model::{
    DbtSemanticModel, DbtSemanticModelAttr, SemanticEntity,
};
use dbt_schemas::schemas::project::{DefaultTo, ModelConfig, SemanticModelConfig};
use dbt_schemas::schemas::properties::ModelProperties;
use dbt_schemas::state::DbtPackage;
use minijinja::value::Value as MinijinjaValue;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;

/// Helper to compute the effective semantic model config for a given semantic model
fn get_effective_semantic_model_config(
    semantic_model_fqn: &[String],
    root_project_configs: &RootProjectConfigs,
    resource_config: &SemanticModelConfig,
    model_props: &ModelProperties,
) -> SemanticModelConfig {
    let mut project_config = root_project_configs
        .semantic_models
        .get_config_for_fqn(semantic_model_fqn)
        .clone();
    project_config.default_to(resource_config);

    if let Some(config) = &model_props.semantic_model {
        let mut final_config = config.clone();
        final_config.default_to(&project_config);
        SemanticModelConfig {
            enabled: Some(final_config.enabled),
            group: final_config.group,
            meta: final_config.config.unwrap_or_default().meta,
            tags: project_config.tags,
        }
    } else {
        project_config
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_semantic_models(
    args: &ResolveArgs,
    package: &DbtPackage,
    root_project_configs: &RootProjectConfigs,
    minimal_model_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    package_name: &str,
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
) -> FsResult<(
    HashMap<String, Arc<DbtSemanticModel>>,
    HashMap<String, Arc<DbtSemanticModel>>,
)> {
    let mut semantic_models: HashMap<String, Arc<DbtSemanticModel>> = HashMap::new();
    let mut disabled_semantic_models: HashMap<String, Arc<DbtSemanticModel>> = HashMap::new();

    if minimal_model_properties.is_empty() {
        return Ok((semantic_models, disabled_semantic_models));
    }

    // TODO: what is the difference between 'package_name' and 'dependency_package_name'?
    let dependency_package_name = dependency_package_name_from_ctx(env, base_ctx);
    let _local_model_project_config = init_project_config(
        &args.io,
        &package.dbt_project.models,
        ModelConfig {
            enabled: Some(true),
            ..Default::default()
        },
        dependency_package_name,
    )?;
    let local_semantic_model_project_config = init_project_config(
        &args.io,
        &package.dbt_project.semantic_models,
        SemanticModelConfig {
            enabled: Some(false),
            ..Default::default()
        },
        dependency_package_name,
    )?;

    for (model_name, mpe) in minimal_model_properties.iter_mut() {
        if mpe.schema_value.is_null() {
            continue;
        }

        // TODO: Do we need to validate semantic_model like how we validate
        // exposure names to only contain letters, numbers, and underscores?

        let mut model_schema_value =
            std::mem::replace(&mut mpe.schema_value, dbt_serde_yaml::Value::null());

        // strip metrics out of model properties
        // this is because metrics have fields that have jinja expressions
        // but should not be rendered (they are hydrated verbatim in manifest.json)
        if let Some(m) = model_schema_value.as_mapping_mut() {
            m.remove("metrics");
        }

        // Parse the semantic_model properties from YAML
        let model_props: ModelProperties = into_typed_with_jinja(
            &args.io,
            model_schema_value,
            false,
            env,
            base_ctx,
            &[],
            dependency_package_name,
        )?;

        if model_props.semantic_model.is_none() {
            continue;
        }
        if !model_props.semantic_model.clone().unwrap().enabled {
            continue;
        }

        // TODO: These are reused from resolve_models, can probably refactor to implement methods in MinimalPropertiesEntry
        let model_maybe_version = mpe.version_info.as_ref().map(|v| v.version.clone());
        // Model fqn includes v{version} for versioned models
        let model_fqn_components = if let Some(version) = &model_maybe_version {
            vec![model_name.to_owned(), format!("v{}", version)]
        } else {
            vec![model_name.to_owned()]
        };

        // We only need to model_fqn if we need to reconcile model config with semantic model config
        // but it seems like we may not be using `models.$.config` at all and instead using `models.$.semantic_models`
        let _model_fqn = get_node_fqn(
            package_name,
            mpe.relative_path.clone(),
            model_fqn_components,
            &package.dbt_project.all_source_paths(),
        );

        // TODO: semantic_model_name may not always be equal to model_name in the future
        // TODO: if the underlying model has versions, which version is the semantic_model tied to?
        let semantic_model_name = model_props
            .semantic_model
            .clone()
            .unwrap()
            .name
            .unwrap_or(model_name.clone());
        let semantic_model_unique_id =
            get_unique_id(&semantic_model_name, package_name, None, "semantic_model");
        let semantic_model_fqn = get_node_fqn(
            package_name,
            mpe.relative_path.clone(),
            vec![semantic_model_name.to_owned()],
            &package.dbt_project.all_source_paths(),
        );

        // Get combined config from project config and semantic_model config
        let semantic_model_resource_config =
            local_semantic_model_project_config.get_config_for_fqn(&semantic_model_fqn);
        let semantic_model_config = get_effective_semantic_model_config(
            &semantic_model_fqn,
            root_project_configs,
            semantic_model_resource_config,
            &model_props,
        );

        let dbt_semantic_model = DbtSemanticModel {
            __common_attr__: CommonAttributes {
                name: semantic_model_name.clone(),
                package_name: package_name.to_string(),
                path: mpe.relative_path.clone(),
                original_file_path: get_original_file_path(
                    &package.package_root_path,
                    &args.io.in_dir,
                    &mpe.relative_path,
                ),
                name_span: dbt_common::Span::from_serde_span(
                    mpe.name_span.clone(),
                    mpe.relative_path.clone(),
                ),
                patch_path: Some(mpe.relative_path.clone()),
                unique_id: semantic_model_unique_id.clone(),
                fqn: semantic_model_fqn.clone(),
                description: model_props.description.clone(),
                checksum: DbtChecksum::default(),
                raw_code: None,
                language: None,
                tags: semantic_model_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: semantic_model_config.meta.clone().unwrap_or_default(),
            },
            __semantic_model_attr__: DbtSemanticModelAttr {
                unrendered_config: BTreeMap::new(), // TODO: do we need to hydrate?
                depends_on: NodeDependsOn::default(), // TODO: should it depend on the underlying model itself or inherit the depends_on of the model?
                group: semantic_model_config.group.clone(),
                created_at: chrono::Utc::now().timestamp() as f64,
                metadata: None,            // TODO: confirm no need for this and remove
                refs: vec![], // TODO: should it ref the underlying model itself or inherit the refs of the model?
                label: None, // TODO: confirm no need for this and remove - there doesn't seem to be a top level label for semantic_model, but there are labels in entities and dimensions
                model: Default::default(), // TODO: confirm no need for this and remove
                node_relation: Default::default(), // TODO: definitely need this. get from a model's database.schema.alias
                defaults: None,                    // TODO: confirm no need for this and remove
                entities: model_props_to_semantic_entities(model_props.clone()),
                measures: vec![], // TODO: confirm no need for this and remove
                dimensions: model_props_to_dimensions(model_props.clone()),
                primary_entity: model_props.primary_entity.clone(),
            },
            deprecated_config: semantic_model_config.clone(),
            __other__: BTreeMap::new(),
        };

        // Check if semantic_model is enabled (following exposures pattern)
        if semantic_model_config.enabled.unwrap_or(true) {
            semantic_models.insert(semantic_model_unique_id, Arc::new(dbt_semantic_model));
        } else {
            disabled_semantic_models.insert(semantic_model_unique_id, Arc::new(dbt_semantic_model));
        }
    }

    Ok((semantic_models, disabled_semantic_models))
}

pub fn model_props_to_semantic_entities(model_props: ModelProperties) -> Vec<SemanticEntity> {
    let mut entities: Vec<SemanticEntity> = vec![];

    for column in model_props.columns.unwrap_or_default() {
        if let Some(column_entity) = column.entity {
            let column_entity_config = match column_entity {
                Entity::EntityType(ref entity_type) => EntityConfig {
                    name: Some(column.name.clone()), // defaults to column.name if there is no column.entity.name
                    type_: entity_type.clone(),
                    description: column.description.clone(), // defaults to column.description if there is no column.entity.description
                    label: None,
                    config: None,
                },
                Entity::EntityConfig(ref config) => config.clone(),
            };

            let semantic_entity = SemanticEntity {
                name: column_entity_config.name.unwrap(), // TODO: confirm this should not always be column.name
                entity_type: column_entity_config.type_,
                description: column.description,
                expr: None, // only applicable for derived_semantics
                label: column_entity_config.label,
                config: column_entity_config.config,
                // fields below are always null (for now)
                role: None,
                metadata: None,
            };
            entities.push(semantic_entity);
        }
    }

    let derived_entities = model_props
        .derived_semantics
        .unwrap_or_default()
        .entities
        .unwrap_or_default();
    for derived_entity in derived_entities {
        let semantic_entity = SemanticEntity {
            name: derived_entity.name.clone(),
            expr: Some(derived_entity.expr.clone()),
            entity_type: derived_entity.type_.clone(),
            description: derived_entity.description.clone(),
            label: derived_entity.label.clone(),
            config: derived_entity.config.clone(),
            // fields below are always null (for now)
            role: None,
            metadata: None,
        };
        entities.push(semantic_entity);
    }

    entities
}

pub fn model_props_to_dimensions(model_props: ModelProperties) -> Vec<Dimension> {
    let mut dimensions: Vec<Dimension> = vec![];

    for column in model_props.columns.unwrap_or_default() {
        if let Some(column_dimension) = column.dimension {
            let column_dimension_config = match column_dimension {
                ColumnPropertiesDimension::DimensionType(ref dimension_type) => {
                    ColumnPropertiesDimensionConfig {
                        type_: dimension_type.clone(),
                        is_partition: Some(false),
                        name: Some(column.name.clone()), // defaults to column.name if there is no column.dimension.name
                        description: column.description.clone(), // defaults to column.description if there is no column.dimension.description
                        label: None,
                        config: None,
                    }
                }
                ColumnPropertiesDimension::DimensionConfig(ref config) => config.clone(),
            };

            let dimension = Dimension {
                name: column.name,
                dimension_type: column_dimension_config.type_.clone(),
                description: column.description,
                expr: None, // is it only applicable for derived_semantics? can one provide a 'expr' for a primary dimension?
                label: column_dimension_config.label,
                is_partition: column_dimension_config.is_partition.unwrap_or(false),
                type_params: column.granularity.map(|granularity| DimensionTypeParams {
                    time_granularity: Some(granularity),
                    validity_params: None,
                }),
                config: column_dimension_config.config.clone(),
                // fields below are always null (for now)
                metadata: None,
            };
            dimensions.push(dimension);
        }
    }

    let derived_dimensions = model_props
        .derived_semantics
        .unwrap_or_default()
        .dimensions
        .unwrap_or_default();
    for derived_dimension in derived_dimensions {
        let dimension = Dimension {
            name: derived_dimension.name.clone(),
            expr: Some(derived_dimension.expr.clone()),
            dimension_type: derived_dimension.type_.clone(),
            is_partition: derived_dimension.is_partition.unwrap_or(false),
            description: derived_dimension.description.clone(),
            type_params: derived_dimension
                .granularity
                .map(|granularity| DimensionTypeParams {
                    time_granularity: Some(granularity),
                    validity_params: None,
                }),
            label: derived_dimension.label.clone(),
            config: derived_dimension.config.clone(),
            // fields below are always null (for now)
            metadata: None,
        };
        dimensions.push(dimension);
    }

    dimensions
}
