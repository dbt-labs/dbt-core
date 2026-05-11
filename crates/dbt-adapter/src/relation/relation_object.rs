use dbt_adapter_core::AdapterType;
use dbt_common::{FsError, FsResult};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::filter::RunFilter;
use dbt_schemas::schemas::common::{DbtQuoting, ResolvedQuoting};
use dbt_schemas::schemas::relations::base::{BaseRelation, Policy, RelationPath, TableFormat};
use dbt_schemas::schemas::serde::minijinja_value_to_typed_struct;
use dbt_schemas::schemas::{InternalDbtNodeAttributes, InternalDbtNodeWrapper};
use minijinja::arg_utils::{ArgParser, ArgsIter};
use minijinja::value::{Enumerator, Object, ValueKind};
use minijinja::{State, Value, listener::RenderingEventListener};
use serde::Deserialize;

use crate::relation::Relation;
use crate::relation::bigquery::*;
use crate::relation::databricks::typed_constraint::TypedConstraint;
use crate::relation::duckdb_should_include_database;
use crate::relation::snowflake::SnowflakeRelation;
use crate::value::none_value;

use std::sync::Arc;
use std::{fmt, ops::Deref};

/// A Wrapper type for BaseRelation
/// for any concrete Relation type to be used as Object in Jinja
#[derive(Clone)]
pub struct RelationObject {
    relation: Arc<dyn BaseRelation>,
    run_filter: Option<RunFilter>,
    event_time: Option<String>,
}

impl RelationObject {
    pub fn new(relation: Arc<dyn BaseRelation>) -> Self {
        Self {
            relation,
            run_filter: None,
            event_time: None,
        }
    }

    pub fn new_with_filter(
        relation: Arc<dyn BaseRelation>,
        run_filter: RunFilter,
        event_time: Option<String>,
    ) -> Self {
        Self {
            relation,
            run_filter: Some(run_filter),
            event_time,
        }
    }

    pub fn into_value(self) -> Value {
        Value::from_object(self)
    }

    pub fn inner(&self) -> Arc<dyn BaseRelation> {
        self.relation.clone()
    }

    /// Create a new RelationObject with a run filter applied.
    ///
    /// This is used for microbatch execution to filter refs by event_time.
    pub fn with_filter(&self, run_filter: RunFilter, event_time: Option<String>) -> Self {
        Self {
            relation: self.relation.clone(),
            run_filter: Some(run_filter),
            event_time,
        }
    }

    /// Check if this relation has a filter applied.
    pub fn has_filter(&self) -> bool {
        self.run_filter.is_some()
    }

    /// Get the event_time column name if configured.
    pub fn event_time(&self) -> Option<&str> {
        self.event_time.as_deref()
    }

    /// Databricks: enrich relation with constraints (for get_column_and_constraints_sql)
    fn relation_enrich(self: &Arc<Self>, args: &[Value]) -> Result<Value, minijinja::Error> {
        let dbx = self
            .relation
            .as_any()
            .downcast_ref::<Relation>()
            .ok_or_else(|| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "enrich is only available for Databricks relations",
                )
            })?;
        let constraints_val = args.first().cloned().unwrap_or_default();
        let constraints: Vec<TypedConstraint> = constraints_val
            .try_iter()
            .map_err(|e| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!("enrich constraints must be iterable: {e}"),
                )
            })?
            .map(|v| {
                minijinja_value_to_typed_struct::<TypedConstraint>(v).map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!("enrich constraint item: {e}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let enriched = dbx.enrich(&constraints);
        Ok(RelationObject::new(Arc::new(enriched)).into_value())
    }

    /// Databricks: render constraints DDL for CREATE TABLE
    fn relation_render_constraints_for_create(self: &Arc<Self>) -> Result<Value, minijinja::Error> {
        let dbx = self
            .relation
            .as_any()
            .downcast_ref::<Relation>()
            .ok_or_else(|| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "render_constraints_for_create is only available for Databricks relations",
                )
            })?;
        Ok(Value::from(dbx.render_constraints_for_create()))
    }

    /// Databricks: get create_constraints for get_column_and_constraints_sql
    fn relation_create_constraints(self: &Arc<Self>) -> Option<Value> {
        let dbx = self.relation.as_any().downcast_ref::<Relation>()?;
        Some(Value::from_serialize(&dbx.create_constraints))
    }
}

/// Always returns the unfiltered relation string (via [`BaseRelation::render_self_as_str`]),
/// reference: https://github.com/dbt-labs/dbt-adapters/blob/616a8d3cb595605872c011070c240e7a2b825d79/dbt-adapters/src/dbt/adapters/base/relation.py#L268-L269
fn render_without_filter(ro: &Arc<RelationObject>) -> Value {
    let rendered = ro.render_self_as_str();
    if rendered.is_empty() {
        none_value()
    } else {
        Value::from(rendered)
    }
}

impl fmt::Debug for RelationObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.render_self_as_str())
    }
}

impl Deref for RelationObject {
    type Target = dyn BaseRelation;

    fn deref(&self) -> &Self::Target {
        self.relation.as_ref()
    }
}

impl From<Arc<dyn BaseRelation>> for RelationObject {
    fn from(relation: Arc<dyn BaseRelation>) -> Self {
        RelationObject::new(relation)
    }
}

impl From<Box<dyn BaseRelation>> for RelationObject {
    fn from(relation: Box<dyn BaseRelation>) -> Self {
        RelationObject::new(Arc::from(relation))
    }
}

impl Object for RelationObject {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        match name {
            "create_from" => self
                .create_from()
                .map(|r| Value::from_object(RelationObject::new(r))),
            "replace_path" => {
                let mut args = ArgParser::new(args, None);
                let database: Option<String> = args.consume_optional_only_from_kwargs("database");
                let schema: Option<String> = args.consume_optional_only_from_kwargs("schema");
                let identifier: Option<String> =
                    args.consume_optional_only_from_kwargs("identifier");
                self.replace_path(database, schema, identifier)
                    .map(|r| Value::from_object(RelationObject::new(r)))
            }
            "get" => {
                let mut args = ArgParser::new(args, None);
                let key: String = args.get("key").unwrap();
                let default: Option<Value> = args.get("default").ok();
                self.get(&key, default)
            }
            "render" => Ok(render_without_filter(self)),
            "without_identifier" => self
                .without_identifier()
                .map(|r| Value::from_object(RelationObject::new(r))),
            "include" => {
                let mut args = ArgParser::new(args, None);
                let database: Option<bool> = args.consume_optional_only_from_kwargs("database");
                let schema: Option<bool> = args.consume_optional_only_from_kwargs("schema");
                let identifier: Option<bool> = args.consume_optional_only_from_kwargs("identifier");
                self.include(database, schema, identifier)
                    .map(|r| Value::from_object(RelationObject::new(r)))
            }
            "quote" => {
                let mut args = ArgParser::new(args, None);
                let database: Option<bool> = args.consume_optional_only_from_kwargs("database");
                let schema: Option<bool> = args.consume_optional_only_from_kwargs("schema");
                let identifier: Option<bool> = args.consume_optional_only_from_kwargs("identifier");
                self.quote(database, schema, identifier)
                    .map(|r| Value::from_object(RelationObject::new(r)))
            }
            "incorporate" => {
                let mut args = ArgParser::new(args, None);
                let path: Option<Value> = args.consume_optional_only_from_kwargs("path");
                let relation_type_val: Option<Value> =
                    args.consume_optional_only_from_kwargs("type");
                let location: Option<String> = args.consume_optional_only_from_kwargs("location");
                let relation_type = relation_type_val.and_then(|v| {
                    if v.is_none() || v.is_undefined() {
                        None
                    } else {
                        v.as_str().map(RelationType::from)
                    }
                });
                self.incorporate(path, relation_type, location)
                    .map(|r| Value::from_object(RelationObject::new(r)))
            }
            "information_schema" => {
                let iter = ArgsIter::new("information_schema", &["view_name"], args);
                let view_name =
                    iter.next_kwarg_aliased::<Option<String>>("view_name", &["identifier"])?;
                iter.finish()?;
                self.information_schema(view_name)
                    .map(|r| Value::from_object(RelationObject::new(r)))
            }
            "relation_max_name_length" => self.relation_max_name_length().map(Value::from),
            // Below are available for Snowflake
            "get_ddl_prefix_for_create" => {
                let iter = ArgsIter::new(
                    "get_ddl_prefix_for_create",
                    &["model_config", "temporary"],
                    args,
                );
                let model_config = iter.next_arg::<Value>()?;
                let temporary = iter.next_arg::<bool>()?;
                iter.finish()?;
                self.get_ddl_prefix_for_create(model_config, temporary)
                    .map(Value::from)
            }
            "get_ddl_prefix_for_alter" => self.get_ddl_prefix_for_alter().map(Value::from),
            "needs_to_drop" => {
                let iter = ArgsIter::new("needs_to_drop", &["old_relation"], args);
                let value = iter.next_arg::<Value>()?;
                iter.finish()?;
                let old_relation = value
                    .downcast_object::<RelationObject>()
                    .map(|ro| ro.inner());
                self.needs_to_drop(old_relation).map(Value::from)
            }
            "get_iceberg_ddl_options" => {
                let iter = ArgsIter::new("get_iceberg_ddl_options", &["config"], args);
                let config = iter.next_arg::<Value>()?;
                iter.finish()?;
                self.get_iceberg_ddl_options(config).map(|opts| {
                    if opts.is_empty() {
                        none_value()
                    } else {
                        Value::from(opts)
                    }
                })
            }
            "dynamic_table_config_changeset" => {
                let iter = ArgsIter::new(
                    "dynamic_table_config_changeset",
                    &["relation_results", "relation_config"],
                    args,
                );
                let relation_results = iter.next_arg::<Value>()?;
                let relation_config = iter.next_arg::<Value>()?;
                iter.finish()?;
                self.dynamic_table_config_changeset(&relation_results, &relation_config)
            }
            "from_config" => {
                let iter = ArgsIter::new("from_config", &["config"], args);
                let config = iter.next_arg::<Value>()?;
                iter.finish()?;
                self.from_config(&config)
            }
            // Below are available for Databricks
            "is_hive_metastore" => Ok(Value::from(self.is_hive_metastore())),
            "enrich" => self.relation_enrich(args),
            "render_constraints_for_create" => self.relation_render_constraints_for_create(),
            // Below are available for BigQuery and Redshift
            "materialized_view_config_changeset" => {
                let iter = ArgsIter::new(
                    "materialized_view_config_changeset",
                    &["relation_results", "relation_config"],
                    args,
                );
                let relation_results = iter.next_arg::<Value>()?;
                let relation_config = iter.next_arg::<Value>()?;
                iter.finish()?;
                self.materialized_view_config_changeset(&relation_results, &relation_config)
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::UnknownMethod,
                format!("Unknown method on BaseRelationObject: '{name}'"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("database") => Some(Value::from(self.database())),
            Some("schema") => Some(Value::from(self.schema())),
            Some("identifier") | Some("name") | Some("table") => {
                Some(Value::from(self.identifier()))
            }

            Some("is_table") => Some(Value::from(self.is_table())),
            Some("is_delta") => Some(Value::from(self.is_delta())),
            Some("create_constraints") => self.relation_create_constraints(),
            Some("is_view") => Some(Value::from(self.is_view())),
            Some("is_materialized_view") => Some(Value::from(self.is_materialized_view())),
            Some("is_streaming_table") => Some(Value::from(self.is_streaming_table())),
            Some("is_dynamic_table") => Some(Value::from(self.is_dynamic_table())),
            Some("is_iceberg_format") => Some(Value::from(self.is_iceberg_format())),
            Some("is_cte") => Some(Value::from(self.is_cte())),
            Some("is_pointer") => Some(Value::from(self.is_pointer())),
            Some("temporary") => Some(Value::from(self.is_temporary())),
            Some("type") => Some(Value::from_serialize(self.relation_type())),
            Some("can_be_renamed") => Some(Value::from(self.can_be_renamed())),
            Some("can_be_replaced") => Some(Value::from(self.can_be_replaced())),
            Some("MaterializedView") => {
                Some(Value::from(RelationType::MaterializedView.to_string()))
            }
            Some("Table") => Some(Value::from(RelationType::Table.to_string())),
            Some("DynamicTable") => Some(Value::from(RelationType::DynamicTable.to_string())),
            Some("StreamingTable") => Some(Value::from(RelationType::StreamingTable.to_string())),
            // the Jinja logics `if resolved.render is defined and resolved.render is callable `
            // in `macro build_ref_function` depends on this
            Some("render") => {
                let this = Arc::clone(self);
                Some(Value::from_func_func("render", move |_state, _args| {
                    Ok(render_without_filter(&this))
                }))
            }
            // BigQuery
            Some("location") => Some(Value::from(self.location())),
            Some("project") => Some(Value::from(self.database())),
            Some("dataset") => Some(Value::from(self.schema())),

            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&[
            "database",
            "schema",
            "identifier",
            "is_table",
            "is_view",
            "is_materialized_view",
            "is_streaming_table",
            "is_cte",
            "is_pointer",
            "can_be_renamed",
            "can_be_replaced",
            "name",
        ])
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result
    where
        Self: Sized + 'static,
    {
        let rendered = match self.run_filter {
            Some(ref run_filter) if run_filter.enabled() => {
                self.render_with_run_filter(run_filter, &self.event_time)
            }
            _ => self.render_self_as_str(),
        };

        let jinja_render = if rendered.is_empty() {
            "None"
        } else {
            &rendered
        };

        write!(f, "{}", jinja_render)
    }
}

/// Creates a relation based on the adapter type
///
/// This is supposed to be used in places that are invoked by the Jinja rendering process
pub fn do_create_relation(
    adapter_type: AdapterType,
    database: String,
    schema: String,
    identifier: Option<String>,
    relation_type: Option<RelationType>,
    custom_quoting: ResolvedQuoting,
) -> Result<Box<dyn BaseRelation>, minijinja::Error> {
    use AdapterType::*;
    let relation = match adapter_type {
        Postgres => Box::new(Relation::new_with_policy(
            Postgres,
            RelationPath {
                database: Some(database).filter(|s| !s.is_empty()),
                schema: Some(schema),
                identifier,
            },
            relation_type,
            Policy::trues(),
            custom_quoting,
            None,
            false,
            false,
        )?) as Box<dyn BaseRelation>,
        DuckDB => {
            // Local DuckDB uses schema.table; attached catalogs need database.schema.table.
            let include_policy = Policy::new(
                duckdb_should_include_database(Some(database.as_str())),
                true,
                true,
            );
            Box::new(Relation::new_with_policy(
                DuckDB,
                RelationPath {
                    database: Some(database).filter(|s| !s.is_empty()),
                    schema: Some(schema),
                    identifier,
                },
                relation_type,
                include_policy,
                custom_quoting,
                None,
                false,
                false,
            )?) as Box<dyn BaseRelation>
        }
        Snowflake => Box::new(SnowflakeRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            TableFormat::Default,
            custom_quoting,
        )) as Box<dyn BaseRelation>,
        Bigquery => Box::new(BigqueryRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )) as Box<dyn BaseRelation>,
        Redshift => Box::new(Relation::new_with_policy(
            Redshift,
            RelationPath {
                database: Some(database).filter(|s| !s.is_empty()),
                schema: Some(schema),
                identifier,
            },
            relation_type,
            Policy::trues(),
            custom_quoting,
            None,
            false,
            false,
        )?) as Box<dyn BaseRelation>,
        Databricks | Spark | Fabric => Box::new(Relation::new(
            adapter_type,
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
            None,
            false,
            false,
        )) as Box<dyn BaseRelation>,
        Exasol => Box::new(Relation::new_with_policy(
            Exasol,
            RelationPath {
                database: Some(database).filter(|s| !s.is_empty()),
                schema: Some(schema),
                identifier,
            },
            relation_type,
            Policy::new(false, true, true),
            custom_quoting,
            None,
            false,
            false,
        )?) as Box<dyn BaseRelation>,
        Salesforce => Box::new(Relation::new_with_policy(
            Salesforce,
            RelationPath {
                database: Some(database).filter(|s| !s.is_empty()),
                schema: Some(schema),
                identifier,
            },
            relation_type,
            Policy::new(false, false, true),
            Policy::enabled(),
            None,
            false,
            false,
        )?) as Box<dyn BaseRelation>,
        ClickHouse => todo!("ClickHouse"),
        Starburst => todo!("Starburst"),
        Athena => todo!("Athena"),
        Trino => todo!("Trino"),
        Dremio => todo!("Dremio"),
        Oracle => todo!("Oracle"),
        Datafusion => todo!("Datafusion"),
    };
    Ok(relation)
}

/// Creates a relation based on the adapter type
///
/// This is a wrapper around the [create_relation] function
/// that is supposed to be used outside the context of Jinja
pub fn create_relation(
    adapter_type: AdapterType,
    database: String,
    schema: String,
    identifier: Option<String>,
    relation_type: Option<RelationType>,
    custom_quoting: ResolvedQuoting,
) -> FsResult<Box<dyn BaseRelation>> {
    let result = do_create_relation(
        adapter_type,
        database,
        schema,
        identifier,
        relation_type,
        custom_quoting,
    )
    .map_err(|e| FsError::from_jinja_err(e, "Failed to create relation"))?;
    Ok(result)
}

pub fn create_relation_from_node(
    adapter_type: AdapterType,
    node: &dyn InternalDbtNodeAttributes,
    _sample_config: Option<RunFilter>,
) -> FsResult<Box<dyn BaseRelation>> {
    create_relation(
        adapter_type,
        node.database(),
        node.schema(),
        Some(node.base().alias.clone()), // all identifiers are consolidated to alias in InternalDbtNode
        Some(RelationType::from(node.materialized())),
        node.quoting(),
    )
}

/// A Wrapper type for StaticBaseRelation
/// for any concrete StaticBaseRelation type to be used as Object in Jinja
/// to expose static methods via api.Relation
#[derive(Debug, Clone)]
pub struct StaticBaseRelationObject(Arc<dyn StaticBaseRelation>);

impl StaticBaseRelationObject {
    pub fn new(relation: Arc<dyn StaticBaseRelation>) -> Self {
        Self(relation)
    }
}

impl Deref for StaticBaseRelationObject {
    type Target = dyn StaticBaseRelation;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl Object for StaticBaseRelationObject {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        match name {
            "create" => self.create(args),
            "scd_args" => self.scd_args(args),
            // // The following is required by BigQuery materialized_views
            "materialized_view_from_relation_config" => {
                if self.0.get_adapter_type() != AdapterType::Bigquery.as_ref() {
                    return Err(minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        "'materialized_view_from_relation_config' can only be invoked using the BigQuery adapter",
                    ));
                }

                let iter = ArgsIter::new(
                    "Relation.materialized_view_from_relation_config",
                    &["local_config"],
                    args,
                );
                let local_config_value = iter.next_arg::<&Value>()?;
                iter.finish()?;

                let local_config = minijinja_value_to_typed_struct::<InternalDbtNodeWrapper>(
                    local_config_value.clone(),
                )
                .map_err(|e| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::SerdeDeserializeError,
                        format!(
                            "get_table_options: Failed to deserialize InternalDbtNodeWrapper: {e}"
                        ),
                    )
                })?;

                let loader = config::relation_types::materialized_view::new_loader();
                let relation_config = loader
                    .from_local_config(local_config.as_internal_node())
                    .map_err(|err| {
                        minijinja::Error::new(
                            minijinja::ErrorKind::InvalidOperation,
                            format!("error while loading local materialized view config: {err}"),
                        )
                    })?;
                Ok(Value::from_object(relation_config))
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::UnknownMethod,
                format!("Unknown method on StaticBaseRelationObject: '{name}'"),
            )),
        }
    }
}

/// Trait for static methods on relations
pub trait StaticBaseRelation: fmt::Debug + Send + Sync {
    /// Create a new relation from the given arguments
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
        temporary: Option<bool>,
    ) -> Result<Value, minijinja::Error>;

    fn get_adapter_type(&self) -> String;

    /// Create a new relation from the given arguments
    /// impl for api.Relation.create
    fn create(&self, args: &[Value]) -> Result<Value, minijinja::Error> {
        let iter = ArgsIter::new("Relation.create", &[], args);
        let database = iter.next_kwarg::<Option<String>>("database")?;
        let schema = iter.next_kwarg::<Option<String>>("schema")?;
        let identifier = iter.next_kwarg::<Option<String>>("identifier")?;
        let relation_type = iter.next_kwarg::<Option<Value>>("type")?;
        let custom_quoting = iter.next_kwarg::<Option<Value>>("quote_policy")?;
        let temporary = iter.next_kwarg::<Option<bool>>("temporary")?;
        iter.finish()?;

        // error is intentionally silenced
        let custom_quoting = custom_quoting
            .and_then(|v| DbtQuoting::deserialize(v).ok())
            // when missing, defaults to be non-quoted
            .map(|v| ResolvedQuoting {
                database: v.database.unwrap_or_default(),
                identifier: v.identifier.unwrap_or_default(),
                schema: v.schema.unwrap_or_default(),
            });

        self.try_new(
            database,
            schema,
            identifier,
            relation_type.and_then(|v: Value| {
                if v.is_none() || v.is_undefined() {
                    None
                } else {
                    Some(RelationType::from(v.as_str().unwrap_or_default()))
                }
            }),
            custom_quoting,
            temporary,
        )
    }

    /// Get the SCD arguments for the relation
    fn scd_args(&self, args: &[Value]) -> Result<Value, minijinja::Error> {
        let iter = ArgsIter::new("Relation.scd_args", &[], args);
        let primary_key = iter.next_kwarg::<Value>("primary_key")?;
        let updated_at = iter.next_kwarg::<String>("updated_at")?;
        iter.finish()?;

        let mut scd_args = vec![];
        // Check if minijinja value is a vector
        match primary_key.kind() {
            ValueKind::Seq => {
                scd_args.extend(primary_key.try_iter()?.enumerate().map(|s| s.1.to_string()));
            }
            ValueKind::String => {
                scd_args.push(primary_key.as_str().unwrap().to_string());
            }
            _ => {
                return Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!(
                        "'primary_key' has a wrong type in StaticBaseRelationObject: '{primary_key}'"
                    ),
                ));
            }
        }
        scd_args.push(updated_at);
        Ok(Value::from(scd_args))
    }
}
