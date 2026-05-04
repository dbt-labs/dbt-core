//! Module containing the entrypoint for the resolve phase.
use crate::args::ResolveArgs;
use crate::dbt_project_config::{ProjectConfigResolver, RootProjectConfigs, init_project_config};
use crate::utils::get_node_fqn;

use dbt_adapter_core::AdapterType;
use dbt_common::io_args::{StaticAnalysisKind, StaticAnalysisOffReason};
use dbt_common::static_analysis::{
    StaticAnalysisDeprecationOrigin, check_deprecated_static_analysis_kind,
};
use dbt_common::tracing::emit::{emit_error_log_from_fs_error, emit_warn_log_from_fs_error};
use dbt_common::{ErrorCode, FsResult, err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::node_resolver::NodeResolver;
use dbt_jinja_utils::serde::{Omissible, into_typed_with_jinja};
use dbt_jinja_utils::utils::generate_relation_name;
use dbt_schemas::schemas::common::{
    DbtChecksum, DbtMaterialization, FreshnessDefinition, FreshnessRules, NodeDependsOn,
    merge_meta, merge_tags, normalize_quoting,
};
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::project::{DefaultTo, SourceConfig};
use dbt_schemas::schemas::properties::{SourceProperties, Tables};
use dbt_schemas::schemas::relations::default_dbt_quoting_for;
use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
use dbt_schemas::schemas::{CommonAttributes, DbtSource, DbtSourceAttr, NodeBaseAttributes};
use dbt_schemas::state::{DbtPackage, GenericTestAsset, ModelStatus, NodeResolverTracker};
use minijinja::Value as MinijinjaValue;
use regex::Regex;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;
use super::resolve_tests::persist_generic_data_tests::{TestableNodeTrait, TestableTable};

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn resolve_sources(
    arg: &ResolveArgs,
    package: &DbtPackage,
    root_package_name: &str,
    root_project_configs: &RootProjectConfigs,
    source_properties: BTreeMap<(String, String), MinimalPropertiesEntry>,
    database: &str,
    adapter_type: AdapterType,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    jinja_env: &JinjaEnv,
    collected_generic_tests: &mut Vec<GenericTestAsset>,
    test_name_truncations: &mut HashMap<String, String>,
    node_resolver: &mut NodeResolver,
) -> FsResult<(
    HashMap<String, Arc<DbtSource>>,
    HashMap<String, Arc<DbtSource>>,
)> {
    let io_args = &arg.io;
    let mut sources: HashMap<String, Arc<DbtSource>> = HashMap::new();
    let mut disabled_sources: HashMap<String, Arc<DbtSource>> = HashMap::new();
    let package_name = package.dbt_project.name.as_ref();

    let dependency_package_name = if package.dbt_project.name != root_package_name {
        Some(package.dbt_project.name.as_str())
    } else {
        None
    };

    let special_chars = Regex::new(r"[^a-zA-Z0-9_]").unwrap();

    // Sources use adapter-specific quoting defaults, NOT project-level quoting
    // https://docs.getdbt.com/reference/resource-properties/quoting
    let source_default_quoting = default_dbt_quoting_for(adapter_type);

    let config_resolver = ProjectConfigResolver::build(
        root_project_configs.sources.clone(),
        dependency_package_name.is_some(),
        || {
            init_project_config(
                io_args,
                &package.dbt_project.sources,
                SourceConfig {
                    quoting: Some(source_default_quoting),
                    ..Default::default()
                },
                dependency_package_name,
            )
        },
    )?;
    for ((source_name, table_name), mpe) in source_properties.into_iter() {
        // Extract raw (unrendered) database and schema from the YAML before Jinja rendering.
        // These preserve Jinja templates like `{{ env_var('DBT_ENV') }}` for state comparisons.
        let raw_source_database = mpe.schema_value.get("database").cloned();
        let raw_source_schema = mpe.schema_value.get("schema").cloned();

        let source: SourceProperties = into_typed_with_jinja(
            io_args,
            mpe.schema_value,
            false,
            jinja_env,
            base_ctx,
            &[],
            dependency_package_name,
            true,
        )?;

        let table: Tables = into_typed_with_jinja(
            io_args,
            mpe.table_value.unwrap(),
            false,
            jinja_env,
            base_ctx,
            &[],
            dependency_package_name,
            true,
        )?;
        let database: String = source
            .database
            .clone()
            .or_else(|| source.catalog.clone())
            .unwrap_or_else(|| database.to_owned());
        let schema = source.schema.clone().unwrap_or_else(|| source.name.clone());

        let fqn = get_node_fqn(
            package_name,
            mpe.relative_path.clone(),
            vec![source_name.to_owned(), table_name.to_owned()],
            &package.dbt_project.all_source_paths(),
        );

        let mut source_properties_config =
            config_resolver.resolve_with_properties(&fqn, source.config.as_ref());

        let table_config = table.config.clone().unwrap_or_default();

        let is_enabled = table_config
            .enabled
            .unwrap_or_else(|| source_properties_config.get_enabled_resolved());

        let normalized_table_name = special_chars.replace_all(&table_name, "__");
        let unique_id = format!(
            "source.{}.{}.{}",
            &package_name, source_name, &normalized_table_name
        );

        // Merge `loaded_at_field` / `loaded_at_query` across source and
        // table levels. The override semantics, the inheritance rules, and
        // the same-block validation are unit-tested directly via
        // [`merge_loaded_at_pair`] in this file's `mod tests` block — those
        // unit tests pin the RIGHT (`field`, `query`) pair for every input
        // combination, addressing the e2e tests' inherent inability to
        // assert resolved per-row values.
        let merged = merge_loaded_at_pair(
            source_properties_config.loaded_at_field.as_deref(),
            source_properties_config.loaded_at_query.0.as_deref(),
            table_config.loaded_at_field.as_deref(),
            table_config.loaded_at_query.0.as_deref(),
        )
        .map_err(|msg| {
            // Annotate with `source.table` so the error pins the offending
            // row rather than just observing that "an error fired
            // somewhere" inside the per-table merge loop.
            dbt_common::fs_err!(
                ErrorCode::Unexpected,
                "{} on source `{}.{}`",
                msg,
                source_name,
                table_name
            )
        })?;
        let merged_loaded_at_field = Some(merged.field);
        let merged_loaded_at_query = Some(merged.query);

        let merged_schema_origin = table_config
            .schema_origin
            .or_else(|| source.config.as_ref().and_then(|c| c.schema_origin))
            .or(source_properties_config.schema_origin)
            .unwrap_or_default();

        // Merge sync config: table-level overrides source-level
        let merged_sync = table_config
            .sync
            .clone()
            .or_else(|| source.config.as_ref().and_then(|c| c.sync.clone()))
            .or_else(|| source_properties_config.sync.clone());
        let merged_freshness = merge_freshness(
            source_properties_config.freshness.as_ref(),
            &table_config.freshness,
        );
        source_properties_config.freshness = merged_freshness.clone();

        // This should be set due to propagation from the resolved root project
        let properties_quoting = source_properties_config
            .quoting
            .expect("quoting should be set");

        let mut source_quoting = source.quoting.unwrap_or_default();
        source_quoting.default_to(&properties_quoting);

        let mut table_quoting = table.quoting.unwrap_or_default();
        table_quoting.default_to(&source_quoting);
        let quoting_ignore_case = table_quoting.snowflake_ignore_case.unwrap_or(false);

        let (database, schema, identifier, quoting) = normalize_quoting(
            &table_quoting.try_into()?,
            adapter_type,
            &database,
            &schema,
            &table
                .identifier
                .clone()
                .unwrap_or_else(|| table_name.to_owned()),
        );

        let parse_adapter = jinja_env
            .get_adapter()
            .expect("Failed to get parse adapter");

        let relation_name =
            generate_relation_name(parse_adapter, &database, &schema, &identifier, quoting)?;

        let source_tags: Option<Vec<String>> = source_properties_config
            .tags
            .clone()
            .map(|tags| tags.into());
        let table_tags: Option<Vec<String>> = table_config.tags.clone().map(|tags| tags.into());

        let merged_tags = merge_tags(source_tags, table_tags);
        let merged_meta = merge_meta(
            source_properties_config.meta.clone(),
            table_config.meta.clone(),
        );

        let merged_event_time = merge_event_time(
            source_properties_config.event_time.clone(),
            table_config.event_time.clone(),
        );

        let columns = if let Some(ref cols) = table.columns {
            process_columns(
                Some(cols),
                source_properties_config.meta.clone(),
                source_properties_config
                    .tags
                    .clone()
                    .map(|tags| tags.into()),
            )?
        } else {
            vec![]
        };

        // Validate local sources have data types defined
        use dbt_schemas::schemas::common::SchemaOrigin;
        if merged_schema_origin == SchemaOrigin::Local {
            if columns.is_empty() {
                return err!(
                    ErrorCode::InvalidConfig,
                    "source:{}.{} has schema_origin: local but no columns defined. \
                     Local sources must have column definitions with data_type specified.",
                    source_name,
                    table_name
                );
            }

            for col in &columns {
                if col.data_type.is_none() {
                    return err!(
                        ErrorCode::InvalidConfig,
                        "Column '{}' in source:{}.{} has schema_origin: local but no data_type defined. \
                         All columns must have data_type specified for local sources.",
                        col.name,
                        source_name,
                        table_name
                    );
                }
            }
        }

        if let Some(freshness) = merged_freshness.as_ref() {
            // F2: a partially-populated freshness rule (only one of
            // {count, period}) is tolerated by Mantle at parse time and only
            // enforced when `dbt source freshness` actually consumes the rule.
            // Match that behavior here — demote the validation failure to a
            // warning so `parse` / `run` / `build` are not aborted, while the
            // safety net inside `dbt-freshness` still produces a hard error
            // when the rule is consumed. F1 (fully-empty rule) is already
            // accepted silently by `FreshnessRules::validate`.
            if let Err(err) = FreshnessRules::validate(freshness.error_after.as_ref()) {
                emit_warn_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
            }
            if let Err(err) = FreshnessRules::validate(freshness.warn_after.as_ref()) {
                emit_warn_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
            }
        }

        // Add any other non-standard dbt keys that might be used by dbt packages under
        // the "other" key. This needs to be untyped since it's up to the packages to define
        // what is a valid configuration entry.
        //
        // For example, dbt-external-tables have their own definition of what are valid
        // values for the `external` property of a source: https://github.com/dbt-labs/dbt-external-tables
        let other = match &table.external {
            None => BTreeMap::new(),
            Some(external) => BTreeMap::from([("external".to_owned(), external.clone())]),
        };

        let static_analysis =
            if let Some(static_analysis) = source_properties_config.clone().static_analysis {
                check_deprecated_static_analysis_kind(
                    static_analysis.clone().into_inner(),
                    StaticAnalysisDeprecationOrigin::NodeConfig {
                        unique_id: unique_id.as_str(),
                    },
                    dependency_package_name,
                    arg.io.status_reporter.as_ref(),
                );
                static_analysis
            } else {
                // If global override is set, use it. Otherwise default
                arg.static_analysis.unwrap_or_default().into()
            };
        // Create a config that respects the table-level overrides of
        // the source-level config.
        // See: https://github.com/dbt-labs/dbt-fusion/issues/767
        // In resolve_sources method, merging is done which is basically
        // overriding the source-level config with the table-level config.
        // The overriden members are scattered across the DbtSource struct
        // and we need to merge them back into the config that will be
        // serialized in the manifest.
        let mut merged_configs = source_properties_config.clone();
        merged_configs.enabled = Some(is_enabled);
        merged_configs.freshness = merged_freshness.clone();
        merged_configs.loaded_at_field = merged_loaded_at_field.clone();
        merged_configs.loaded_at_query = merged_loaded_at_query.clone().into();
        merged_configs.event_time = merged_event_time.clone();
        merged_configs.schema_origin = Some(merged_schema_origin);
        merged_configs.sync = merged_sync.clone();
        // node.meta and node.tags receive the merged values via __common_attr__ below;
        // node.config.meta and node.config.tags need them too to match dbt-core. (#1393)
        merged_configs.meta = merged_meta.clone();
        merged_configs.tags = merged_tags
            .clone()
            .map(StringOrArrayOfStrings::ArrayOfStrings);

        let dbt_source = DbtSource {
            __common_attr__: CommonAttributes {
                name: table_name.to_owned(),
                package_name: package_name.to_owned(),
                // original_file_path: dbt_asset.base_path.join(&dbt_asset.path),
                // path: dbt_asset.base_path.join(&dbt_asset.path),
                original_file_path: mpe.relative_path.clone(),
                path: mpe.relative_path.clone(),
                name_span: dbt_common::Span::from_serde_span(
                    mpe.name_span,
                    mpe.relative_path.clone(),
                ),
                unique_id: unique_id.to_owned(),
                fqn,
                description: table.description.to_owned(),
                // todo: columns code gen missing
                patch_path: Some(mpe.relative_path.clone()),
                meta: merged_meta.unwrap_or_default(),
                tags: merged_tags.unwrap_or_default(),
                raw_code: None,
                checksum: DbtChecksum::default(),
                language: None,
            },
            __base_attr__: NodeBaseAttributes {
                database: database.to_owned(),
                schema: schema.to_owned(),
                alias: identifier.to_owned(),
                relation_name: Some(relation_name),
                quoting,
                quoting_ignore_case,
                enabled: is_enabled,
                extended_model: false,
                persist_docs: None,
                materialized: DbtMaterialization::External,
                static_analysis_off_reason: (static_analysis.clone().into_inner()
                    == StaticAnalysisKind::Off)
                    .then_some(StaticAnalysisOffReason::ConfiguredOff),
                static_analysis,
                columns,
                refs: vec![],
                sources: vec![],
                functions: vec![],
                depends_on: NodeDependsOn::default(),
                metrics: vec![],
                unrendered_config: {
                    let mut uc = BTreeMap::new();
                    if let Some(db) = raw_source_database {
                        uc.insert("database".to_string(), db);
                    }
                    if let Some(sch) = raw_source_schema {
                        uc.insert("schema".to_string(), sch);
                    }
                    uc
                },
            },
            __source_attr__: DbtSourceAttr {
                freshness: merged_freshness.clone(),
                identifier,
                source_name: source_name.to_owned(),
                source_description: source.description.clone().unwrap_or_default(), // needs to be some or empty string per dbt spec
                loader: source.loader.clone().unwrap_or_default(),
                loaded_at_field: merged_loaded_at_field.clone(),
                loaded_at_query: merged_loaded_at_query.clone(),
                schema_origin: merged_schema_origin,
                sync: merged_sync,
            },
            deprecated_config: merged_configs,
            __other__: other,
        };
        let status = if is_enabled {
            ModelStatus::Enabled
        } else {
            ModelStatus::Disabled
        };

        match node_resolver.insert_source(package_name, &dbt_source, adapter_type, status) {
            Ok(_) => (),
            Err(e) => {
                let err_with_loc = e.with_location(mpe.relative_path.clone());
                emit_error_log_from_fs_error(&err_with_loc, io_args.status_reporter.as_ref());
            }
        }

        match status {
            ModelStatus::Enabled => {
                sources.insert(unique_id, Arc::new(dbt_source));

                if !arg.skip_creating_generic_tests {
                    TestableTable {
                        source_name: source_name.clone(),
                        table: &table.clone(),
                    }
                    .as_testable()
                    .persist(
                        package_name,
                        root_package_name,
                        collected_generic_tests,
                        test_name_truncations,
                        adapter_type,
                        io_args,
                        &mpe.relative_path,
                    )?;
                }
            }
            ModelStatus::Disabled => {
                disabled_sources.insert(unique_id, Arc::new(dbt_source));
            }
            ModelStatus::ParsingFailed => {}
        }
    }
    Ok((sources, disabled_sources))
}

fn merge_event_time(
    source_event_time: Option<String>,
    table_event_time: Option<String>,
) -> Option<String> {
    // If table_config.event_time is set (Some), use it regardless of its value
    // Only use source_event_time if table_event_time is None
    table_event_time.or(source_event_time)
}

/// Resolved (`loaded_at_field`, `loaded_at_query`) pair after merging
/// table-level config over source-level config. Either value may be empty
/// (downstream treats `""` and `None` as "no freshness on this dimension").
#[derive(Debug)]
pub(crate) struct MergedLoadedAt {
    pub field: String,
    pub query: String,
}

/// Merge `loaded_at_field` and `loaded_at_query` across source-level and
/// table-level config.
///
/// `loaded_at_field` and `loaded_at_query` are mutually exclusive peers.
/// dbt-core treats a table-level override of either as implicitly clearing
/// the inherited *other* — that's how patterns like a source-wide
/// `loaded_at_query` plus a per-table `loaded_at_field` override (e.g. the
/// merge-log table the query references) resolve cleanly. Without this,
/// the per-key `or_else` chain would leak the source-level value of the
/// un-overridden key and falsely trip the same-block validation.
///
/// Returns `Err` only when, after merging, BOTH peers are non-empty —
/// which happens in two cases:
///   1. Both peers set in the same `config:` block at table level, OR
///   2. Both peers set at the source level with no table-level override
///      to clear them.
fn merge_loaded_at_pair(
    source_field: Option<&str>,
    source_query: Option<&str>,
    table_field: Option<&str>,
    table_query: Option<&str>,
) -> Result<MergedLoadedAt, &'static str> {
    let table_has_field = table_field.is_some();
    let table_has_query = table_query.is_some();

    let merged_field = if table_has_query {
        // Table override of `loaded_at_query` clears any inherited
        // `loaded_at_field` from the source level.
        table_field.unwrap_or("").to_string()
    } else {
        table_field.or(source_field).unwrap_or("").to_string()
    };

    let merged_query = if table_has_field {
        // Table override of `loaded_at_field` clears any inherited
        // `loaded_at_query` from the source level.
        table_query.unwrap_or("").to_string()
    } else {
        table_query.or(source_query).unwrap_or("").to_string()
    };

    if !merged_field.is_empty() && !merged_query.is_empty() {
        return Err("loaded_at_field and loaded_at_query cannot be set at the same time");
    }
    Ok(MergedLoadedAt {
        field: merged_field,
        query: merged_query,
    })
}

fn merge_freshness(
    base: Option<&FreshnessDefinition>,
    update: &Omissible<Option<FreshnessDefinition>>,
) -> Option<FreshnessDefinition> {
    match update {
        // A present but 'null' freshness does not inherit from the base and inhibits freshness by returning None.
        Omissible::Present(None) => None,
        Omissible::Present(update) => update
            .as_ref()
            .and_then(|update| merge_freshness_unwrapped(base, Some(update))),
        // If there is no freshness present in the update then it is inherited (merged) from the base.
        Omissible::Omitted => merge_freshness_unwrapped(base, None),
    }
}

fn merge_freshness_unwrapped(
    base: Option<&FreshnessDefinition>,
    update: Option<&FreshnessDefinition>,
) -> Option<FreshnessDefinition> {
    match (base, update) {
        // As long as a single element is present in update, override all of the elements in the base
        // with the elements in the update.
        // See mantle logic: https://github.com/dbt-labs/dbt-mantle/blob/847ab93f830d745c1c3d6609ead642b2bd07139a/core/dbt/parser/sources.py#L532-L542
        // The mantle logic looks complicated but it is basically doing the same thing as the first
        // statement of this comment. Especially look at the merge_freshness_time_thresholds function,
        // which states that if an element of update is None, just return None for the specific element.
        (_, Some(update)) => Some(update.clone()),
        (Some(base), None) => Some(base.clone()),
        (None, None) => Some(FreshnessDefinition::default()), // Provide default value if user never defined freshness https://dbtlabs.atlassian.net/browse/META-5461
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_jinja_utils::serde::Omissible;
    use dbt_schemas::schemas::common::{FreshnessDefinition, FreshnessPeriod, FreshnessRules};

    #[test]
    fn test_merge_event_time_table_overrides_source() {
        // When table_event_time is Some, it should always be used
        let source_event_time = Some("source_timestamp".to_string());
        let table_event_time = Some("table_timestamp".to_string());
        let result = merge_event_time(source_event_time, table_event_time);
        assert_eq!(result, Some("table_timestamp".to_string()));
    }

    #[test]
    fn test_merge_event_time_uses_source_when_table_none() {
        // When table_event_time is None, source_event_time should be used
        let source_event_time = Some("source_timestamp".to_string());
        let table_event_time = None;
        let result = merge_event_time(source_event_time, table_event_time);
        assert_eq!(result, Some("source_timestamp".to_string()));
    }

    #[test]
    fn test_merge_event_time_both_none() {
        // When both are None, result should be None
        let source_event_time = None;
        let table_event_time = None;
        let result = merge_event_time(source_event_time, table_event_time);
        assert_eq!(result, None);
    }

    #[test]
    fn test_merge_event_time_empty_table_overrides() {
        // Even empty string in table_event_time should override source
        let source_event_time = Some("source_timestamp".to_string());
        let table_event_time = Some("".to_string());
        let result = merge_event_time(source_event_time, table_event_time);
        assert_eq!(result, Some("".to_string()));
    }

    #[test]
    fn test_merge_freshness_unwrapped_update_overrides_base() {
        // When both base and update have values, update should override completely
        let base = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(5),
                period: Some(FreshnessPeriod::hour),
            }),
            warn_after: Some(FreshnessRules {
                count: Some(3),
                period: Some(FreshnessPeriod::hour),
            }),
            filter: Some("base_filter".to_string()),
        };
        let update = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(10),
                period: Some(FreshnessPeriod::day),
            }),
            warn_after: None,
            filter: None,
        };

        let result = merge_freshness_unwrapped(Some(&base), Some(&update));
        assert_eq!(result, Some(update));
    }

    #[test]
    fn test_merge_freshness_unwrapped_inherit_from_base() {
        // When update is None, base should be inherited
        let base = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(5),
                period: Some(FreshnessPeriod::hour),
            }),
            warn_after: Some(FreshnessRules {
                count: Some(3),
                period: Some(FreshnessPeriod::hour),
            }),
            filter: None,
        };

        let result = merge_freshness_unwrapped(Some(&base), None);
        assert_eq!(result, Some(base));
    }

    #[test]
    fn test_merge_freshness_unwrapped_no_base() {
        // When base is None but update has value, use update
        let update = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(10),
                period: Some(FreshnessPeriod::day),
            }),
            warn_after: None,
            filter: None,
        };

        let result = merge_freshness_unwrapped(None, Some(&update));
        assert_eq!(result, Some(update));
    }

    #[test]
    fn test_merge_freshness_unwrapped_both_none() {
        // When both are None, result should be None
        let result = merge_freshness_unwrapped(None, None);
        assert_eq!(result, Some(FreshnessDefinition::default()));
    }

    #[test]
    fn test_merge_freshness_present_null_inhibits() {
        // Present but null freshness should return None (inhibits freshness)
        let base = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(5),
                period: Some(FreshnessPeriod::hour),
            }),
            warn_after: None,
            filter: None,
        };

        let update = Omissible::Present(None);
        let result = merge_freshness(Some(&base), &update);
        assert_eq!(result, None);
    }

    #[test]
    fn test_merge_freshness_present_with_value() {
        // Present with value should use the value
        let base = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(5),
                period: Some(FreshnessPeriod::hour),
            }),
            warn_after: None,
            filter: None,
        };

        let update_value = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(10),
                period: Some(FreshnessPeriod::day),
            }),
            warn_after: Some(FreshnessRules {
                count: Some(5),
                period: Some(FreshnessPeriod::day),
            }),
            filter: None,
        };

        let update = Omissible::Present(Some(update_value.clone()));
        let result = merge_freshness(Some(&base), &update);
        assert_eq!(result, Some(update_value));
    }

    #[test]
    fn test_merge_freshness_omitted_inherits_base() {
        // Omitted freshness should inherit from base
        let base = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(5),
                period: Some(FreshnessPeriod::hour),
            }),
            warn_after: None,
            filter: None,
        };

        let update = Omissible::Omitted;
        let result = merge_freshness(Some(&base), &update);
        assert_eq!(result, Some(base));
    }

    #[test]
    fn test_merge_freshness_omitted_no_base() {
        // Omitted freshness with no base should return None
        let update = Omissible::Omitted;
        let result = merge_freshness(None, &update);
        assert_eq!(result, Some(FreshnessDefinition::default()));
    }

    #[test]
    fn test_merge_freshness_partial_update_overrides_completely() {
        // Test that partial updates in the update completely override base
        // This validates the comment about mantle logic
        let base = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(5),
                period: Some(FreshnessPeriod::hour),
            }),
            warn_after: Some(FreshnessRules {
                count: Some(3),
                period: Some(FreshnessPeriod::hour),
            }),
            filter: Some("base_filter".to_string()),
        };

        // Update only has error_after, but it should still completely replace base
        let update_value = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(10),
                period: Some(FreshnessPeriod::day),
            }),
            warn_after: None,
            filter: None,
        };

        let update = Omissible::Present(Some(update_value.clone()));
        let result = merge_freshness(Some(&base), &update);

        // The result should be exactly the update, not a merge
        assert_eq!(result, Some(update_value));
        // Specifically verify that warn_after and filter are None, not inherited from base
        assert!(result.as_ref().unwrap().warn_after.is_none());
        assert!(result.as_ref().unwrap().filter.is_none());
    }

    // ── merge_loaded_at_pair ──────────────────────────────────────────────
    //
    // These unit tests pin the RIGHT (`field`, `query`) pair after merge for
    // every state-relevant input combination. The e2e tests in
    // `crates/dbt-cli/tests/dbt_conformance/regression.rs` cannot make this
    // assertion because parse-success/parse-failure goldens only see the CLI
    // summary, not the per-row resolved config. This block addresses that
    // exactly: any merge-logic regression that produces a different result
    // for any of these inputs trips one or more of these tests.

    /// Table-level `loaded_at_field` overrides source-level `loaded_at_query`.
    /// The *inherited* peer must be cleared on the override row.
    #[test]
    fn test_merge_loaded_at_pair_table_field_clears_inherited_query() {
        let result = merge_loaded_at_pair(
            None,                               // source_field
            Some("select max(load_ts) from x"), // source_query
            Some("LOAD_TIMESTAMP"),             // table_field — override
            None,                               // table_query
        )
        .expect("must not error: cross-level peer-clearing is valid");
        assert_eq!(
            result.field, "LOAD_TIMESTAMP",
            "table-level loaded_at_field override must survive the merge"
        );
        assert_eq!(
            result.query, "",
            "inherited source-level loaded_at_query must be cleared by table-level field override"
        );
    }

    /// Mirror direction: table-level `loaded_at_query` overrides source-level
    /// `loaded_at_field`. Guards against an asymmetric fix that handles only
    /// one direction.
    #[test]
    fn test_merge_loaded_at_pair_table_query_clears_inherited_field() {
        let result = merge_loaded_at_pair(
            Some("SRC_LOADED_AT"),                // source_field
            None,                                 // source_query
            None,                                 // table_field
            Some("select max(custom_ts) from y"), // table_query — override
        )
        .expect("must not error: cross-level peer-clearing is valid");
        assert_eq!(
            result.field, "",
            "inherited source-level loaded_at_field must be cleared by table-level query override"
        );
        assert_eq!(
            result.query, "select max(custom_ts) from y",
            "table-level loaded_at_query override must survive the merge"
        );
    }

    /// Sibling table with NO table-level override on the SAME source: must
    /// inherit source-level values untouched. Together with the override
    /// tests above, proves the peer-clearing is selective to override rows
    /// rather than a broad clear.
    #[test]
    fn test_merge_loaded_at_pair_no_table_override_inherits_source() {
        let result = merge_loaded_at_pair(
            Some("SRC_LOADED_AT"), // source_field
            None,                  // source_query
            None,                  // table_field
            None,                  // table_query
        )
        .expect("must not error: only source-level field is set");
        assert_eq!(
            result.field, "SRC_LOADED_AT",
            "non-override table must inherit source-level loaded_at_field"
        );
        assert_eq!(result.query, "", "no query anywhere → empty");
    }

    /// Genuine misuse — both peers set in the SAME (table) `config:` block.
    /// Must error with the exact validation message; the merge cannot
    /// silently swallow the conflict.
    #[test]
    fn test_merge_loaded_at_pair_both_in_same_table_block_errors() {
        let err = merge_loaded_at_pair(
            None,
            None,
            Some("TS"),                             // table_field
            Some("select max(ts) from {{ this }}"), // table_query — same block!
        )
        .expect_err("both peers in the same table block must error");
        assert!(
            err.contains("loaded_at_field and loaded_at_query cannot be set at the same time"),
            "error must name the conflict; got: {err}"
        );
    }

    /// Same-block conflict at the SOURCE level (no table-level override).
    /// Even without table-level config the validation must fire if the
    /// inherited pair conflicts. Discriminates against a buggy fix that
    /// only checks for table-level conflicts.
    #[test]
    fn test_merge_loaded_at_pair_both_at_source_level_errors() {
        let err = merge_loaded_at_pair(
            Some("SRC_LOADED_AT"),         // source_field
            Some("select max(ts) from x"), // source_query
            None,
            None,
        )
        .expect_err("both peers inherited at the source level must error");
        assert!(
            err.contains("loaded_at_field and loaded_at_query cannot be set at the same time"),
            "error must name the conflict; got: {err}"
        );
    }

    /// Empty everywhere → both empty, no error. Pin the no-op case so a
    /// future bug that spuriously errors on absent freshness is caught.
    #[test]
    fn test_merge_loaded_at_pair_all_none() {
        let result =
            merge_loaded_at_pair(None, None, None, None).expect("absent peers must not error");
        assert_eq!(result.field, "");
        assert_eq!(result.query, "");
    }
}
