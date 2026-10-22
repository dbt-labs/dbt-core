use crate::AdapterEngine;
use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::relation::{RelationObject, do_create_relation};
use crate::value::empty_mutable_vec_value;

use dbt_adapter_core::AdapterType;
use dbt_common::FsError;
use dbt_common::collections::{SccHashMap, SccHashSet};
use dbt_schemas::schemas::dbt_catalogs_deprecated::DbtCatalogs;
use dbt_schemas::schemas::relations::base::{BaseRelation, RelationPattern};
use minijinja::constants::TARGET_UNIQUE_ID;
use minijinja::{State, Value};
use serde::Deserialize;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

type RelationsToFetch = (
    Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, FsError>,
    Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, FsError>,
    BTreeMap<String, Vec<RelationPattern>>,
);

/// Downcast every recorded relation value, stopping at the first failure.
fn collect_relations(
    calls: &SccHashMap<String, Vec<Value>>,
) -> Result<BTreeMap<String, Vec<Arc<dyn BaseRelation>>>, minijinja::Error> {
    let mut relations = BTreeMap::new();
    let mut error = None;
    calls.iter_sync(|unique_id, values| {
        match values
            .iter()
            .map(downcast_value_to_dyn_base_relation)
            .collect::<Result<Vec<Arc<dyn BaseRelation>>, minijinja::Error>>()
        {
            Ok(downcast) => {
                relations.insert(unique_id.to_owned(), downcast);
                true
            }
            Err(e) => {
                error = Some(e);
                false
            }
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(relations),
    }
}

#[derive(Clone)]
pub struct ParseAdapterState {
    pub adapter_type: AdapterType,
    /// The engine for the parse phase
    ///
    /// Not actually used to run SQL queries during parse, but needed since
    /// this object carries useful dependencies.
    pub engine: Arc<dyn AdapterEngine>,
    /// The call_get_relation method calls found during parse
    pub call_get_relation: SccHashMap<String, Vec<Value>>,
    /// The call_get_columns_in_relation method calls found during parse
    call_get_columns_in_relation: SccHashMap<String, Vec<Value>>,
    /// A patterned relation may turn to many dangling sources
    patterned_dangling_sources: SccHashMap<String, Vec<RelationPattern>>,
    /// A list of unsafe nodes detected during parse (unsafe nodes are nodes that have introspection qualities that make them non-deterministic / stateful)
    pub unsafe_nodes: SccHashSet<String>,
    unsafe_node_counts: SccHashMap<String, usize>,
    /// SQLs that are found passed in to adapter.execute in the hidden Parse phase
    pub execute_sqls: SccHashSet<String>,
    execute_sql_counts: SccHashMap<String, usize>,
    /// catalogs.yml stored when found and loaded
    pub catalogs: Option<Arc<DbtCatalogs>>,
}

impl ParseAdapterState {
    pub fn new(
        adapter_type: AdapterType,
        engine: Arc<dyn AdapterEngine>,
        catalogs: Option<Arc<DbtCatalogs>>,
    ) -> Self {
        ParseAdapterState {
            adapter_type,
            engine,
            call_get_relation: SccHashMap::default(),
            call_get_columns_in_relation: SccHashMap::default(),
            patterned_dangling_sources: SccHashMap::default(),
            unsafe_nodes: SccHashSet::default(),
            unsafe_node_counts: SccHashMap::default(),
            execute_sqls: SccHashSet::default(),
            execute_sql_counts: SccHashMap::default(),
            catalogs,
        }
    }

    pub fn debug_fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParseAdapter")
            .field("adapter_type", &self.adapter_type)
            .field("call_get_relation", &self.call_get_relation)
            .field(
                "call_get_columns_in_relation",
                &self.call_get_columns_in_relation,
            )
            .field(
                "patterned_dangling_sources",
                &self.patterned_dangling_sources,
            )
            .field("unsafe_nodes", &self.unsafe_nodes)
            .field("execute_sqls", &self.execute_sqls)
            .field("quoting", &self.engine.quoting())
            .finish()
    }

    /// Merge parse-time adapter side effects from another render context.
    ///
    /// Adapter-specific node renders use a dedicated parse adapter for dispatch
    /// and relation construction, but the resolver drains one invocation-wide
    /// state. Preserve every relation fetch and safety signal in that sink.
    pub fn merge_from(&self, other: &Self) {
        other.call_get_relation.iter_sync(|key, values| {
            self.call_get_relation
                .entry_sync(key.clone())
                .or_default()
                .extend(values.iter().cloned());
            true
        });
        other.call_get_columns_in_relation.iter_sync(|key, values| {
            self.call_get_columns_in_relation
                .entry_sync(key.clone())
                .or_default()
                .extend(values.iter().cloned());
            true
        });
        other.patterned_dangling_sources.iter_sync(|key, values| {
            self.patterned_dangling_sources
                .entry_sync(key.clone())
                .or_default()
                .extend(values.iter().cloned());
            true
        });
        other.unsafe_nodes.iter_sync(|key| {
            *self.unsafe_node_counts.entry_sync(key.clone()).or_default() += 1;
            let _already_present = self.unsafe_nodes.insert_sync(key.clone());
            true
        });
        other.execute_sqls.iter_sync(|key| {
            *self.execute_sql_counts.entry_sync(key.clone()).or_default() += 1;
            let _already_present = self.execute_sqls.insert_sync(key.clone());
            true
        });
    }

    /// Replace one render's recorded relation requests with an authoritative
    /// replay while preserving contributions from other renders sharing a key.
    pub fn replace_from(&self, previous: &Self, current: &Self) {
        previous
            .call_get_relation
            .iter_sync(|key, previous_values| {
                self.call_get_relation.update_sync(key, |_, values| {
                    for previous_value in previous_values {
                        if let Some(index) = values.iter().position(|value| value == previous_value)
                        {
                            values.remove(index);
                        }
                    }
                });
                true
            });
        previous
            .call_get_columns_in_relation
            .iter_sync(|key, previous_values| {
                self.call_get_columns_in_relation
                    .update_sync(key, |_, values| {
                        for previous_value in previous_values {
                            if let Some(index) =
                                values.iter().position(|value| value == previous_value)
                            {
                                values.remove(index);
                            }
                        }
                    });
                true
            });
        previous
            .patterned_dangling_sources
            .iter_sync(|key, previous_values| {
                self.patterned_dangling_sources
                    .update_sync(key, |_, values| {
                        for previous_value in previous_values {
                            if let Some(index) =
                                values.iter().position(|value| value == previous_value)
                            {
                                values.remove(index);
                            }
                        }
                    });
                true
            });
        previous.unsafe_nodes.iter_sync(|key| {
            let should_remove = self
                .unsafe_node_counts
                .update_sync(key, |_, count| {
                    if *count > 1 {
                        *count -= 1;
                        false
                    } else {
                        true
                    }
                })
                .unwrap_or(false);
            if should_remove {
                self.unsafe_node_counts.remove_sync(key);
                self.unsafe_nodes.remove_sync(key);
            }
            true
        });
        previous.execute_sqls.iter_sync(|key| {
            let should_remove = self
                .execute_sql_counts
                .update_sync(key, |_, count| {
                    if *count > 1 {
                        *count -= 1;
                        false
                    } else {
                        true
                    }
                })
                .unwrap_or(false);
            if should_remove {
                self.execute_sql_counts.remove_sync(key);
                self.execute_sqls.remove_sync(key);
            }
            true
        });
        self.merge_from(current);
    }

    pub fn record_get_relation_call(
        &self,
        state: &State,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> Result<(), minijinja::Error> {
        let relation = RelationObject::new(Arc::from(do_create_relation(
            self.adapter_type,
            database.to_string(),
            schema.to_string(),
            Some(identifier.to_string()),
            None,
            self.engine.quoting(),
        )?))
        .into_value();

        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID, &[]) {
                self.call_get_relation
                    .entry_sync(unique_id.to_string())
                    .or_default()
                    .push(relation);
            } else {
                println!("'TARGET_UNIQUE_ID' while get_relation is unset");
            }
        }
        Ok(())
    }

    pub(crate) fn record_unsafe_node(&self, unique_id: String) {
        *self
            .unsafe_node_counts
            .entry_sync(unique_id.clone())
            .or_default() += 1;
        let _already_present = self.unsafe_nodes.insert_sync(unique_id);
    }

    pub(crate) fn record_execute_sql(&self, sql: String) {
        *self.execute_sql_counts.entry_sync(sql.clone()).or_default() += 1;
        let _already_present = self.execute_sqls.insert_sync(sql);
    }

    pub fn record_get_columns_in_relation_call(
        &self,
        state: &State,
        relation: &dyn BaseRelation,
    ) -> Result<(), minijinja::Error> {
        if !relation.is_database_relation() {
            return Ok(());
        }
        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID, &[]) {
                let relation_value = RelationObject::new(relation.to_owned()).into_value();
                self.call_get_columns_in_relation
                    .entry_sync(unique_id.to_string())
                    .or_default()
                    .push(relation_value);
            } else {
                println!("'TARGET_UNIQUE_ID' while get_columns_in_relation is unset");
            }
        }
        Ok(())
    }

    /// Returns a tuple of (dangling_sources, patterned_dangling_sources)
    /// dangling_sources is a vector of dangling source relations
    /// patterned_dangling_sources is a vector of patterned dangling source relations
    #[allow(clippy::type_complexity)]
    pub fn relations_to_fetch(&self) -> RelationsToFetch {
        let relations_to_fetch = collect_relations(&self.call_get_relation)
            .map_err(|e| FsError::from_jinja_err(e, "Failed to collect get_relation"));

        let relations_to_fetch_columns = collect_relations(&self.call_get_columns_in_relation)
            .map_err(|e| FsError::from_jinja_err(e, "Failed to collect get_columns_in_relation"));

        let mut patterned_dangling_sources: BTreeMap<String, Vec<RelationPattern>> =
            BTreeMap::new();
        self.patterned_dangling_sources.iter_sync(|k, v| {
            patterned_dangling_sources.insert(k.to_owned(), v.to_owned());
            true
        });
        (
            relations_to_fetch,
            relations_to_fetch_columns,
            patterned_dangling_sources,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_relations_by_pattern(
        &self,
        state: &State,
        schema_pattern: &str,
        table_pattern: &str,
        _exclude: Option<&str>,
        database: Option<&str>,
        _quote_table: Option<bool>,
        excluded_schemas: Option<Value>,
    ) -> Result<Value, minijinja::Error> {
        // Validate excluded_schemas if provided
        if let Some(ref schemas) = excluded_schemas {
            let _: Vec<String> = Vec::<String>::deserialize(schemas.clone()).map_err(|e| {
                minijinja::Error::new(minijinja::ErrorKind::SerdeDeserializeError, e.to_string())
            })?;
        }

        let target = state
            .lookup("target", &[])
            .expect("target is set in parse")
            .get_attr("database")
            .unwrap_or_default();
        let default_database = target.as_str().unwrap_or_default();
        let database = database.unwrap_or(default_database);

        let patterned_relation = RelationPattern::new(
            database.to_string(),
            schema_pattern.to_string(),
            table_pattern.to_string(),
        );

        if state.is_execute() {
            if let Some(unique_id) = state.lookup(TARGET_UNIQUE_ID, &[]) {
                self.patterned_dangling_sources
                    .entry_sync(unique_id.to_string())
                    .or_default()
                    .push(patterned_relation);
            } else {
                println!("'TARGET_UNIQUE_ID' while get_relations_by_pattern is unset");
            }
        }

        // Seen methods like 'append' being used on the result in internaly-analytics
        Ok(empty_mutable_vec_value())
    }

    pub fn unsafe_nodes(&self) -> &SccHashSet<String> {
        &self.unsafe_nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Adapter, sql_types::DefaultTypeOps};
    use dbt_schemas::schemas::common::{DbtQuoting, ResolvedQuoting};

    fn parse_adapter() -> Adapter {
        Adapter::new_parse_phase_adapter(
            AdapterType::Postgres,
            dbt_yaml::Mapping::default(),
            DbtQuoting {
                database: Some(true),
                schema: Some(true),
                identifier: Some(true),
                snowflake_ignore_case: Some(false),
            },
            Arc::new(DefaultTypeOps::new(AdapterType::Postgres)),
            None,
        )
    }

    fn relation_value(database: &str) -> Value {
        RelationObject::new(Arc::from(
            do_create_relation(
                AdapterType::Postgres,
                database.to_string(),
                "analytics".to_string(),
                Some("orders".to_string()),
                None,
                ResolvedQuoting {
                    database: true,
                    schema: true,
                    identifier: true,
                },
            )
            .unwrap(),
        ))
        .into_value()
    }

    #[test]
    fn replace_from_preserves_other_contributions_for_the_same_node() {
        let sink_adapter = parse_adapter();
        let previous_adapter = parse_adapter();
        let current_adapter = parse_adapter();
        let other_adapter = parse_adapter();
        let sink = sink_adapter.parse_adapter_state().unwrap();
        let previous = previous_adapter.parse_adapter_state().unwrap();
        let current = current_adapter.parse_adapter_state().unwrap();
        let other = other_adapter.parse_adapter_state().unwrap();
        let node = "model.project.orders".to_string();
        let only_previous_node = "model.project.discovery_only".to_string();
        let shared_sql = "select 'shared'".to_string();
        let only_previous_sql = "select 'discovery'".to_string();
        let discovery = relation_value("discovery");

        previous
            .call_get_relation
            .upsert_sync(node.clone(), vec![discovery]);
        previous.unsafe_nodes.insert_sync(node.clone()).unwrap();
        previous
            .unsafe_nodes
            .insert_sync(only_previous_node.clone())
            .unwrap();
        previous
            .execute_sqls
            .insert_sync(shared_sql.clone())
            .unwrap();
        previous
            .execute_sqls
            .insert_sync(only_previous_sql.clone())
            .unwrap();
        other.unsafe_nodes.insert_sync(node.clone()).unwrap();
        other.execute_sqls.insert_sync(shared_sql.clone()).unwrap();
        sink.merge_from(other);
        sink.merge_from(previous);
        sink.call_get_relation.update_sync(&node, |_, values| {
            values.insert(0, relation_value("other"));
        });
        current
            .call_get_relation
            .upsert_sync(node.clone(), vec![relation_value("authoritative")]);

        sink.replace_from(previous, current);

        let databases = sink
            .call_get_relation
            .read_sync(&node, |_, values| {
                values
                    .iter()
                    .map(|value| {
                        downcast_value_to_dyn_base_relation(value)
                            .unwrap()
                            .database()
                            .unwrap()
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap();
        assert_eq!(databases, ["other", "authoritative"]);
        assert!(sink.unsafe_nodes.contains_sync(&node));
        assert!(!sink.unsafe_nodes.contains_sync(&only_previous_node));
        assert!(sink.execute_sqls.contains_sync(&shared_sql));
        assert!(!sink.execute_sqls.contains_sync(&only_previous_sql));
    }
}
