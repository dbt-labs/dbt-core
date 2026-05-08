//! Module containing the entrypoint for the resolve phase.
use crate::args::ResolveArgs;
use crate::dbt_project_config::{ProjectConfigResolver, RootProjectConfigs, init_project_config};
use crate::utils::get_node_fqn;
use crate::validation::check_node_static_analysis;

use dbt_adapter_core::AdapterType;
use dbt_common::io_args::{StaticAnalysisKind, StaticAnalysisOffReason};
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
use dbt_schemas::schemas::project::{DbtProject, SourceConfig};
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
    root_project: &DbtProject,
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
                source_default_quoting,
                dependency_package_name,
            )
        },
    )?
    .with_resolve_defaults((
        arg.static_analysis.unwrap_or_default(),
        root_project.sync.clone(),
    ));
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

        let normalized_table_name = special_chars.replace_all(&table_name, "__");
        let unique_id = format!(
            "source.{}.{}.{}",
            &package_name, source_name, &normalized_table_name
        );

        let table_config = table.config.clone().unwrap_or_default();

        // Sources have two config layers: source-level and table-level. The config_resolver
        // handles project-level → source-level propagation via `default_to`. Table-level config
        // is NOT passed as an override to the resolver, so its fields must be merged manually
        // in the closure below. Tags and meta are additive (union/merge) rather than simple
        // overrides, but they are still handled here so that source_config carries the fully
        // merged state (used by process_columns and deprecated_config).
        // See: https://github.com/dbt-labs/dbt-fusion/issues/767
        let source_config = config_resolver.try_resolve_with_overrides(
            &fqn,
            &fqn,
            &[source.config.as_ref()],
            |c: &mut SourceConfig| -> FsResult<()> {
                c.enabled = Some(
                    table_config
                        .enabled
                        .unwrap_or_else(|| c.enabled.unwrap_or(true)),
                );
                c.freshness = merge_freshness(c.freshness.as_ref(), &table_config.freshness);
                c.event_time =
                    merge_event_time(c.event_time.clone(), table_config.event_time.clone());
                c.schema_origin = Some(
                    table_config
                        .schema_origin
                        .or(c.schema_origin)
                        .unwrap_or_default(),
                );
                c.sync = table_config.sync.clone().or_else(|| c.sync.clone());
                let source_tags: Option<Vec<String>> = c.tags.take().map(|t| t.into());
                let table_tags: Option<Vec<String>> = table_config.tags.clone().map(|t| t.into());
                c.tags =
                    merge_tags(source_tags, table_tags).map(StringOrArrayOfStrings::ArrayOfStrings);
                c.meta = merge_meta(c.meta.take(), table_config.meta.clone());
                let merged = merge_loaded_at_pair(
                    c.loaded_at_field.as_deref(),
                    c.loaded_at_query.0.as_deref(),
                    table_config.loaded_at_field.as_deref(),
                    table_config.loaded_at_query.0.as_deref(),
                )
                .map_err(|msg| {
                    dbt_common::fs_err!(
                        ErrorCode::Unexpected,
                        "{} on source `{}.{}`",
                        msg,
                        source_name,
                        table_name
                    )
                })?;
                c.loaded_at_field = Some(merged.field);
                c.loaded_at_query = Some(merged.query).into();
                Ok(())
            },
        )?;

        check_node_static_analysis(
            &source_config,
            arg.static_analysis,
            &unique_id,
            dependency_package_name,
            arg.io.status_reporter.as_ref(),
        );

        // This should be set due to propagation from the resolved root project
        let properties_quoting = source_config.quoting;

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

        let columns = if let Some(ref cols) = table.columns {
            process_columns(
                Some(cols),
                source_config.meta.clone(),
                source_config.tags.clone().map(|tags| tags.into()),
            )?
        } else {
            vec![]
        };

        // Validate local sources have data types defined
        use dbt_schemas::schemas::common::SchemaOrigin;
        if source_config.schema_origin == SchemaOrigin::Local {
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

        if let Some(freshness) = source_config.freshness.as_ref() {
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

        let static_analysis = source_config.static_analysis.clone();

        let dbt_source = DbtSource {
            __common_attr__: CommonAttributes {
                name: table_name.to_owned(),
                package_name: package_name.to_owned(),
                original_file_path: mpe.relative_path.clone(),
                path: mpe.relative_path.clone(),
                name_span: dbt_common::Span::from_serde_span(
                    mpe.name_span,
                    mpe.relative_path.clone(),
                ),
                unique_id: unique_id.to_owned(),
                fqn,
                description: table.description.to_owned(),
                patch_path: Some(mpe.relative_path.clone()),
                meta: source_config.meta.clone().unwrap_or_default(),
                tags: source_config
                    .tags
                    .clone()
                    .map(|t| t.into())
                    .unwrap_or_default(),
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
                enabled: source_config.enabled,
                extended_model: false,
                persist_docs: None,
                materialized: DbtMaterialization::External,
                static_analysis_off_reason: (*static_analysis == StaticAnalysisKind::Off)
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
                freshness: source_config.freshness.clone(),
                identifier,
                source_name: source_name.to_owned(),
                source_description: source.description.clone().unwrap_or_default(), // needs to be some or empty string per dbt spec
                loader: source.loader.clone().unwrap_or_default(),
                loaded_at_field: source_config.loaded_at_field.clone(),
                loaded_at_query: source_config.loaded_at_query.0.clone(),
                schema_origin: source_config.schema_origin,
                sync: source_config.sync.clone(),
            },
            deprecated_config: source_config.clone().into(),
            __other__: other,
        };
        let status = if source_config.enabled {
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
struct MergedLoadedAt {
    field: String,
    query: String,
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
        (_, Some(update)) => {
            // Mantle uses field-level merging: each field uses the update value if set,
            // otherwise inherits from base.
            // https://github.com/dbt-labs/dbt-mantle/blob/6bcac392d653a5c8a35da01bc94d93a45b882629/core/dbt/parser/sources.py#L545-L555
            Some(FreshnessDefinition {
                error_after: update
                    .error_after
                    .clone()
                    .or_else(|| base.and_then(|b| b.error_after.clone())),
                warn_after: update
                    .warn_after
                    .clone()
                    .or_else(|| base.and_then(|b| b.warn_after.clone())),
                filter: update
                    .filter
                    .clone()
                    .or_else(|| base.and_then(|b| b.filter.clone())),
            })
        }
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
        // When both base and update have values, update fields win; unset fields inherit from base.
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

        let result = merge_freshness_unwrapped(Some(&base), Some(&update)).unwrap();
        // error_after comes from update
        assert_eq!(result.error_after.as_ref().unwrap().count, Some(10));
        // warn_after and filter inherit from base
        assert_eq!(result.warn_after.as_ref().unwrap().count, Some(3));
        assert_eq!(result.filter.as_deref(), Some("base_filter"));
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
    fn test_merge_freshness_partial_update_inherits_unset_fields() {
        // Test that partial updates inherit unset fields from base (field-level merge).
        // Matches Mantle's behavior where FreshnessThreshold.merged() uses last-non-None-wins.
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

        // Update only has error_after; warn_after and filter should be inherited from base.
        let update_value = FreshnessDefinition {
            error_after: Some(FreshnessRules {
                count: Some(10),
                period: Some(FreshnessPeriod::day),
            }),
            warn_after: None,
            filter: None,
        };

        let update = Omissible::Present(Some(update_value));
        let result = merge_freshness(Some(&base), &update);

        let merged = result.unwrap();
        // error_after comes from update
        assert_eq!(merged.error_after.as_ref().unwrap().count, Some(10));
        // warn_after and filter are inherited from base
        assert_eq!(merged.warn_after.as_ref().unwrap().count, Some(3));
        assert_eq!(merged.filter.as_deref(), Some("base_filter"));
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
