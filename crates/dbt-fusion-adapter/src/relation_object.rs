use dbt_common::{FsError, FsResult};
use dbt_schemas::{
    dbt_types::RelationType,
    schemas::{
        common::ResolvedQuoting,
        relations::{
            base::{BaseRelation, TableFormat},
            DEFAULT_RESOLVED_QUOTING,
        },
    },
};
use minijinja::{
    arg_utils::ArgParser,
    value::{Enumerator, Object, ValueKind},
};
use minijinja::{listener::RenderingEventListener, Value};
use minijinja::{Error as MinijinjaError, State};

use crate::bigquery::relation::BigqueryRelation;
use crate::databricks::relation::DatabricksRelation;
use crate::postgres::relation::PostgresRelation;
use crate::redshift::relation::RedshiftRelation;
use crate::snowflake::relation::SnowflakeRelation;

use std::sync::Arc;
use std::{fmt, ops::Deref};

#[derive(Debug, Clone)]
pub struct RelationObject(Arc<dyn BaseRelation>);

impl RelationObject {
    pub fn new(relation: Arc<dyn BaseRelation>) -> Self {
        Self(relation)
    }

    pub fn into_value(self) -> Value {
        Value::from_object(self)
    }

    pub fn inner(&self) -> Arc<dyn BaseRelation> {
        self.0.clone()
    }
}

impl Deref for RelationObject {
    type Target = dyn BaseRelation;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl Object for RelationObject {
    fn call_method(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match name {
            "create_from" => self.create_from(state, args),
            "replace_path" => self.replace_path(args),
            "get" => self.get(args),
            "render" => self.render_self(),
            "without_identifier" => self.without_identifier(args),
            "include" => self.include(args),
            "incorporate" => self.incorporate(args),
            "information_schema" => self.information_schema(args),
            "relation_max_name_length" => self.relation_max_name_length(args),
            // Below are available for Snowflake
            "get_ddl_prefix_for_create" => self.get_ddl_prefix_for_create(args),
            "get_ddl_prefix_for_alter" => self.get_ddl_prefix_for_alter(),
            "needs_to_drop" => self.needs_to_drop(args),
            "get_iceberg_ddl_options" => self.get_iceberg_ddl_options(args),
            "dynamic_table_config_changeset" => self.dynamic_table_config_changeset(args),
            "from_config" => self.from_config(args),
            // Below are available for Databricks
            "is_hive_metastore" => Ok(self.is_hive_metastore()),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Unknown method on BaseRelationObject: '{}'", name),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("database") => Some(self.database()),
            Some("schema") => Some(self.schema()),
            Some("identifier") | Some("name") | Some("table") => Some(self.identifier()),
            Some("is_table") => Some(Value::from(self.is_table())),
            Some("is_view") => Some(Value::from(self.is_view())),
            Some("is_materialized_view") => Some(Value::from(self.is_materialized_view())),
            Some("is_cte") => Some(Value::from(self.is_cte())),
            Some("is_pointer") => Some(Value::from(self.is_pointer())),
            Some("type") => Some(self.relation_type_as_value()),
            Some("can_be_renamed") => Some(Value::from(self.can_be_renamed())),
            Some("can_be_replaced") => Some(Value::from(self.can_be_replaced())),
            Some("MaterializedView") => {
                Some(Value::from(RelationType::MaterializedView.to_string()))
            }
            Some("Table") => Some(Value::from(RelationType::Table.to_string())),
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
        let text = self.render_self().expect("could not render self");
        write!(f, "{}", text)
    }
}

/// Creates a relation based on the adapter type
///
/// Unlike [internal_create_relation]
/// This is supposed to be used in places that are invoked by the Jinja rendering process
pub fn create_relation(
    adapter_type: String,
    database: String,
    schema: String,
    identifier: Option<String>,
    relation_type: Option<RelationType>,
    custom_quoting: ResolvedQuoting,
) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
    let relation = match adapter_type.to_lowercase().as_str() {
        "postgres" => Arc::new(PostgresRelation::try_new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            custom_quoting,
        )?) as Arc<dyn BaseRelation>,
        "snowflake" => Arc::new(SnowflakeRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            TableFormat::Default,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        "bigquery" => Arc::new(BigqueryRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        "redshift" => Arc::new(RedshiftRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        "databricks" => Arc::new(DatabricksRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
            None,
            false,
        )) as Arc<dyn BaseRelation>,
        _ => panic!("not supported"),
    };
    Ok(relation)
}

/// Creates a relation based on the adapter type
///
/// This is a wrapper around the [create_relation] function
/// that is supposed to be used outside the context of Jinja
pub fn create_relation_internal(
    adapter_type: String,
    database: String,
    schema: String,
    identifier: Option<String>,
    relation_type: Option<RelationType>,
    custom_quoting: ResolvedQuoting,
) -> FsResult<Arc<dyn BaseRelation>> {
    let result = create_relation(
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

impl Object for &dyn StaticBaseRelation {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match name {
            "create" => self.create(args),
            "scd_args" => Ok(Value::from(self.scd_args(args))),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Unknown method on StaticBaseRelationObject: '{}'", name),
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
        custom_quoting: ResolvedQuoting,
    ) -> Result<Value, MinijinjaError>;

    fn get_adapter_type(&self) -> String;

    /// Create a new relation from the given arguments
    /// impl for api.Relation.create
    fn create(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let database: Option<String> = args.get("database").ok();
        let schema: Option<String> = args.get("schema").ok();
        let identifier: Option<String> = args.get("identifier").ok();
        let relation_type: Option<String> = args.get("type").ok();

        self.try_new(
            database,
            schema,
            identifier,
            relation_type.map(|s| RelationType::from(s.as_str())),
            DEFAULT_RESOLVED_QUOTING,
        )
    }

    /// Get the SCD arguments for the relation
    fn scd_args(&self, args: &[Value]) -> Vec<String> {
        let mut args = ArgParser::new(args, None);
        let primary_key: Value = args.get("primary_key").unwrap();
        let updated_at: String = args.get("updated_at").unwrap();
        let mut scd_args = vec![];
        // Check if minijinja value is a vector
        match primary_key.kind() {
            ValueKind::Seq => {
                scd_args.extend(
                    primary_key
                        .as_object()
                        .unwrap()
                        .downcast_ref::<Vec<String>>()
                        .unwrap()
                        .iter()
                        .map(|s| s.to_string()),
                );
            }
            ValueKind::String => {
                scd_args.push(primary_key.as_str().unwrap().to_string());
            }
            _ => {
                panic!("Invalid primary key type");
            }
        }
        scd_args.push(updated_at);
        scd_args
    }
}
