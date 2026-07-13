//! Microbatch model execution runner.
//!
//! This module provides the execution logic for models using the microbatch
//! incremental strategy. It processes data in time-based windows (batches).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use crate::microbatch::BatchContext;
use dbt_adapter::cast_util::downcast_value_to_dyn_base_relation;
use dbt_adapter::formatter::SqlLiteralFormatter;
use dbt_adapter::relation::create_relation_from_node;
use dbt_adapter_core::AdapterType;
use dbt_common::FsResult;
use dbt_common::constants::DBT_EPHEMERAL_DIR_NAME;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::compile::DependencyValidationConfig;
use dbt_jinja_utils::phases::{MicrobatchRefContext, RefFunction, SourceFunction};
use dbt_jinja_utils::utils::inject_and_persist_ephemeral_models;
use dbt_schemas::schemas::{DbtModel, InternalDbtNode};
use dbt_schemas::state::{DbtRuntimeConfig, NodeResolverTracker};
use minijinja::Value;
use tracing::warn;

/// Extend the Jinja context with microbatch-specific overrides.
///
/// This replaces `ref` and `source` with microbatch-aware versions that inject
/// event-time filters, adds batch metadata variables, patches the `model` value
/// with batch info and internal timestamps, and controls pre/post hook lists.
pub fn extend_microbatch_node_context(
    batch_ctx: &BatchContext,
    adapter_type: AdapterType,
    model: &DbtModel,
    node_resolver: Arc<dyn NodeResolverTracker>,
    runtime_config: &DbtRuntimeConfig,
    jinja_context: &mut BTreeMap<String, Value>,
    event_time_mapping: Arc<BTreeMap<String, String>>,
) {
    // Create microbatch ref context for this batch
    let microbatch_ctx = MicrobatchRefContext::new(
        batch_ctx.event_time_start,
        batch_ctx.event_time_end,
        event_time_mapping,
    );
    // TODO(chasewalden): maybe we make a "proxy" type that impls `Object`
    //  could be cleaner/more performant than re-building the entire BTreeMap

    // TODO(chasewalden): we likely need to create a proxy for `{{ this }}` to detect if it is used.

    // Replace the ref function with one that has microbatch context
    // Note: The base_context should already have a ref function, but we need
    // to create a new one with the microbatch context
    let allowed_deps = model
        .__base_attr__
        .depends_on
        .nodes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    let microbatch_ref = RefFunction::new_with_microbatch_context(
        node_resolver.clone(),
        model.__common_attr__.package_name.clone(),
        runtime_config.clone().into(),
        DependencyValidationConfig::new_for_node(model)
            .validate()
            .allow_dependencies(allowed_deps.iter()),
        microbatch_ctx.clone(),
        model.common().unique_id.clone(),
    );

    // Insert the microbatch-aware ref into context
    jinja_context.insert("ref".to_string(), Value::from_object(microbatch_ref));

    // Replace the source function with one that has microbatch context
    let microbatch_source = SourceFunction::new_with_microbatch_context(
        node_resolver,
        model.__common_attr__.package_name.clone(),
        microbatch_ctx,
    );

    // Insert the microbatch-aware source into context
    jinja_context.insert("source".to_string(), Value::from_object(microbatch_source));

    // Add batch-specific context variables
    jinja_context.insert(
        "__dbt_microbatch_batch_id__".to_string(),
        Value::from(batch_ctx.id.clone()),
    );
    let formatter = SqlLiteralFormatter::new(adapter_type);
    jinja_context.insert(
        "__dbt_microbatch_event_time_start__".to_string(),
        Value::from(formatter.format_timestamp(batch_ctx.event_time_start)),
    );
    jinja_context.insert(
        "__dbt_microbatch_event_time_end__".to_string(),
        Value::from(formatter.format_timestamp(batch_ctx.event_time_end)),
    );
    jinja_context.insert(
        "__dbt_microbatch_is_first_batch__".to_string(),
        Value::from(batch_ctx.is_first()),
    );
    jinja_context.insert(
        "__dbt_microbatch_is_last_batch__".to_string(),
        Value::from(batch_ctx.is_last()),
    );

    // Add model.batch for the Snowflake incremental macro to use
    // The macro checks model.batch.event_time_start and model.batch.event_time_end
    let mut batch_info = BTreeMap::new();
    batch_info.insert(
        "event_time_start".to_string(),
        Value::from(formatter.format_timestamp(batch_ctx.event_time_start)),
    );
    batch_info.insert(
        "event_time_end".to_string(),
        Value::from(formatter.format_timestamp(batch_ctx.event_time_end)),
    );
    batch_info.insert("id".to_string(), Value::from(batch_ctx.id.clone()));

    // Update the model in context to include batch info
    if let Some(model_value) = jinja_context.get("model").cloned() {
        if let Some(model_obj) = model_value.as_object() {
            // Create a mutable map from the model and add batch
            let mut model_map: BTreeMap<String, Value> = BTreeMap::new();

            // Copy existing model properties
            if let Some(iter) = model_obj.try_iter() {
                for key in iter {
                    if let Some(val) = model_obj.get_value(&key) {
                        if let Some(key_str) = key.as_str() {
                            model_map.insert(key_str.to_string(), val);
                        }
                    }
                }
            }

            // Add batch info
            model_map.insert("batch".to_string(), Value::from_serialize(&batch_info));

            // Update config with microbatch timestamps
            if let Some(config_val) = model_map.get("config").cloned() {
                if let Some(config_obj) = config_val.as_object() {
                    let mut config_map: BTreeMap<String, Value> = BTreeMap::new();

                    // Copy existing config properties
                    if let Some(iter) = config_obj.try_iter() {
                        for key in iter {
                            if let Some(val) = config_obj.get_value(&key) {
                                if let Some(key_str) = key.as_str() {
                                    config_map.insert(key_str.to_string(), val);
                                }
                            }
                        }
                    }

                    // Add microbatch timestamps to config
                    config_map.insert(
                        "__dbt_internal_microbatch_event_time_start".to_string(),
                        Value::from(formatter.format_timestamp(batch_ctx.event_time_start)),
                    );
                    config_map.insert(
                        "__dbt_internal_microbatch_event_time_end".to_string(),
                        Value::from(formatter.format_timestamp(batch_ctx.event_time_end)),
                    );

                    model_map.insert("config".to_string(), Value::from_serialize(&config_map));
                }
            }

            jinja_context.insert("model".to_string(), Value::from_serialize(&model_map));
        }
    }

    // Control pre/post hooks based on batch position
    // Note: dbt-core runs pre-hooks only on first batch, post-hooks only on last batch
    if !batch_ctx.is_first() {
        jinja_context.insert("pre_hooks".to_string(), Value::from(Vec::<Value>::new()));
    }
    if !batch_ctx.is_last() {
        jinja_context.insert("post_hooks".to_string(), Value::from(Vec::<Value>::new()));
    }
}

/// Re-render the raw SQL template with the batch context.
///
/// This re-renders the original model SQL (with Jinja tags like {{ ref(...) }})
/// using a context that includes a microbatch-aware ref function. The ref()
/// function will wrap upstream models with event_time filtering to only include
/// rows within the current batch window.
pub fn render_batch_sql(
    raw_sql: &str,
    jinja_env: Arc<JinjaEnv>,
    jinja_context: &BTreeMap<String, Value>,
    out_dir: &Path,
) -> FsResult<String> {
    // Render the raw SQL through Jinja
    // The context already contains config from build_run_node_context
    // This will cause ref() calls to use the microbatch-aware ref function
    // which wraps refs in filtered subqueries based on event_time
    //
    // TODO(chasewalden): {{ source(...) }} calls also need to be made microbatch-aware
    let rendered = jinja_env.render_str(raw_sql, jinja_context, &[])?;
    tracing::debug!("Rendered batch SQL: {rendered}...",);

    // BUG(chasewalden): the microbatch-aware {{ ref(...) }} does not correctly render upstream `ephemeral` models
    let mut macro_spans = Default::default(); // create dummy MacroSpans
    let rendered = inject_and_persist_ephemeral_models(
        rendered,
        &mut macro_spans,
        "microbatch_not_persisted", // can be anything; only used to persist an ephemeral model
        false,                      // a microbatch model cannot be ephemeral
        &out_dir.join(DBT_EPHEMERAL_DIR_NAME),
    )?;

    tracing::debug!("Rendered batch SQL w/ injected CTE (first 200 chars): {rendered:.200}...",);

    Ok(rendered)
}

/// Build the event_time mapping for dependencies of a microbatch model.
///
/// This mapping is used to determine which refs should have event_time filters
/// applied during batch execution.
pub fn build_event_time_mapping(
    model: &DbtModel,
    nodes: &dbt_schemas::schemas::Nodes,
) -> BTreeMap<String, String> {
    let mut mapping = BTreeMap::new();

    // Resolve each dependency's `event_time` through the `InternalDbtNode` trait
    for dep_id in &model.__base_attr__.depends_on.nodes {
        if let Some(event_time) = nodes.get_node(dep_id).and_then(|node| node.event_time()) {
            mapping.insert(dep_id.clone(), event_time);
        }
    }

    mapping
}

pub fn is_incremental(
    model: &DbtModel,
    full_refresh: bool,
    adapter_type: AdapterType,
    jinja_env: Arc<JinjaEnv>,
) -> bool {
    if full_refresh {
        return false;
    }

    let state = jinja_env.new_state_with_context(BTreeMap::new());
    let adapter = match jinja_env.get_base_adapter() {
        Some(a) => a,
        None => return false,
    };

    let relation = match create_relation_from_node(adapter_type, model, None) {
        Ok(rel) => rel,
        Err(e) => {
            warn!(
                "Failed to create relation for checkpoint query: {}. Starting fresh.",
                e
            );
            return false;
        }
    };

    adapter
        .get_relation(
            &state,
            relation.database().unwrap_or_default(),
            relation.schema().unwrap_or_default(),
            relation.identifier().unwrap_or_default(),
            false,
        )
        .ok()
        .filter(|v| !v.is_none())
        .and_then(|v| downcast_value_to_dyn_base_relation(&v).ok())
        .is_some_and(|rel| rel.is_table())
}

#[cfg(test)]
mod tests {
    use dbt_common::path::DbtPath;

    use super::*;

    #[test]
    fn test_build_event_time_mapping_empty() {
        // Create a minimal model without depends_on
        let model = create_test_model();
        let nodes = dbt_schemas::schemas::Nodes::default();

        let mapping = build_event_time_mapping(&model, &nodes);
        assert!(mapping.is_empty());
    }

    #[test]
    fn test_build_event_time_mapping_resolves_seed_event_time() {
        // A microbatch model whose upstream is a seed (not a model/source) should
        // still pick up the seed's `event_time`, matching dbt-core. Seeds store
        // event_time in `deprecated_config`, like sources.
        use dbt_schemas::schemas::project::SeedConfig;
        use dbt_schemas::schemas::{CommonAttributes, DbtSeed};

        let seed_uid = "seed.test.raw_events".to_string();
        let seed = DbtSeed {
            __common_attr__: CommonAttributes {
                unique_id: seed_uid.clone(),
                name: "raw_events".to_string(),
                package_name: "test".to_string(),
                ..Default::default()
            },
            deprecated_config: SeedConfig {
                event_time: Some("event_time".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut model = create_test_model();
        model.__base_attr__.depends_on.nodes = vec![seed_uid.clone()];

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes.seeds.insert(seed_uid.clone(), Arc::new(seed));

        let mapping = build_event_time_mapping(&model, &nodes);
        assert_eq!(
            mapping.get(&seed_uid).map(String::as_str),
            Some("event_time")
        );
    }

    #[test]
    fn test_build_event_time_mapping_resolves_snapshot_event_time() {
        // A microbatch model whose upstream is a snapshot should also pick up the
        // snapshot's `event_time`. Like seeds and sources, snapshots store it in
        // `deprecated_config`, and resolution now goes through the node trait.
        use dbt_schemas::schemas::CommonAttributes;
        use dbt_schemas::schemas::DbtSnapshot;
        use dbt_schemas::schemas::project::SnapshotConfig;

        let snapshot_uid = "snapshot.test.raw_events".to_string();
        let snapshot = DbtSnapshot {
            __common_attr__: CommonAttributes {
                unique_id: snapshot_uid.clone(),
                name: "raw_events".to_string(),
                package_name: "test".to_string(),
                ..Default::default()
            },
            deprecated_config: SnapshotConfig {
                event_time: Some("event_time".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut model = create_test_model();
        model.__base_attr__.depends_on.nodes = vec![snapshot_uid.clone()];

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes
            .snapshots
            .insert(snapshot_uid.clone(), Arc::new(snapshot));

        let mapping = build_event_time_mapping(&model, &nodes);
        assert_eq!(
            mapping.get(&snapshot_uid).map(String::as_str),
            Some("event_time")
        );
    }

    fn create_test_model() -> DbtModel {
        use dbt_schemas::schemas::project::ModelConfig;
        use dbt_schemas::schemas::{CommonAttributes, DbtModelAttr, NodeBaseAttributes};

        DbtModel {
            __common_attr__: CommonAttributes {
                unique_id: "model.test.my_model".to_string(),
                name: "my_model".to_string(),
                package_name: "test".to_string(),
                path: DbtPath::from("models/my_model.sql"),
                original_file_path: DbtPath::from("models/my_model.sql"),
                ..Default::default()
            },
            __base_attr__: NodeBaseAttributes::default(),
            __model_attr__: DbtModelAttr::default(),
            __adapter_attr__: Default::default(),
            deprecated_config: ModelConfig::default(),
            __other__: Default::default(),
        }
    }
}
