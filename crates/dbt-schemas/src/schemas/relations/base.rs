//! Reference: dbt-adapters/src/dbt/adapters/contracts/relation.py
use crate::dbt_types::RelationType;
use crate::filter::RunFilter;
use crate::schemas::common::ResolvedQuoting;

use dbt_common::constants::DBT_CTE_PREFIX;
use dbt_common::{FsResult, current_function_name};
use minijinja::arg_utils::{ArgParser, check_num_args};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};
use minijinja::{invalid_argument, invalid_argument_inner, jinja_err};
use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use core::fmt;
use std::any::Any;
use std::collections::BTreeMap;
use std::option::Option;
use std::sync::Arc;

/// A pattern to match relations
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelationPattern {
    /// The database
    pub database: String,
    /// The schema pattern to match %, _ etc
    pub schema_pattern: String,
    /// The table pattern to match %, _ etc
    pub table_pattern: String,
}

impl RelationPattern {
    pub fn new(database: String, schema_pattern: String, table_pattern: String) -> Self {
        Self {
            database,
            schema_pattern,
            table_pattern,
        }
    }
}

/// The format of the table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// The default table format
    Default,
    /// The iceberg table format
    Iceberg,
}

/// dbt-adapters/src/dbt/adapters/contracts/relation.py
pub type Policy = ResolvedQuoting;

impl Policy {
    pub fn disabled() -> Self {
        Self {
            database: false,
            schema: false,
            identifier: false,
        }
    }

    pub fn enabled() -> Self {
        Self {
            database: true,
            schema: true,
            identifier: true,
        }
    }
}

impl Policy {
    pub fn get_part(&self, component: &ComponentName) -> bool {
        match component {
            ComponentName::Database => self.database,
            ComponentName::Schema => self.schema,
            ComponentName::Identifier => self.identifier,
        }
    }
}

/// dbt-adapters/src/dbt/adapters/contracts/relation.py
#[derive(Debug, EnumString, Display)]
#[strum(serialize_all = "lowercase")]
pub enum ComponentName {
    Database,
    Schema,
    Identifier,
}

/// A struct representing the path of a relation
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct RelationPath {
    /// The database name
    pub database: Option<String>,
    /// The schema name
    pub schema: Option<String>,
    /// The identifier name
    pub identifier: Option<String>,
}

pub trait BaseRelationProperties {
    fn is_database_relation(&self) -> bool {
        true
    }

    fn include_policy(&self) -> Policy;

    fn quote_policy(&self) -> Policy;

    /// quoting character to be used when rendering the relation
    fn quote_character(&self) -> char;

    fn get_database(&self) -> FsResult<String>;

    fn get_schema(&self) -> FsResult<String>;

    fn get_identifier(&self) -> FsResult<String>;
}

/// Base trait for all fs adapter objects
pub trait BaseRelation: BaseRelationProperties + Any + Send + Sync + fmt::Debug {
    /// Whether the relation is a system table or not
    fn is_system(&self) -> bool {
        false
    }

    /// as_any
    fn as_any(&self) -> &dyn Any;

    /// Create a new relation from the given state and arguments
    fn create_from(&self, state: &State, args: &[Value]) -> Result<Value, MinijinjaError>;

    /// Get the database name
    fn database(&self) -> Value;

    /// Database as string or error
    fn database_as_str(&self) -> Result<String, MinijinjaError> {
        match self.database().as_str() {
            Some(val) => Ok(val.to_string()),
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect database as string"
            ),
        }
    }

    /// Get the database name as a string literal
    /// the same as how a database provider resolves and stores a database component for a relation
    /// given how it's quoted
    fn database_as_resolved_str(&self) -> Result<String, MinijinjaError> {
        match self.database().as_str() {
            Some(val) => {
                if !self.quote_policy().database {
                    Ok(self.normalize_component(val))
                } else {
                    Ok(val.to_string())
                }
            }
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect database as string"
            ),
        }
    }

    fn database_as_quoted_str(&self) -> Result<String, MinijinjaError> {
        match self.database().as_str() {
            Some(val) => Ok(self.quoted(val)),
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect database as string"
            ),
        }
    }

    /// Get the schema name
    fn schema(&self) -> Value;

    /// Schema as string or error
    fn schema_as_str(&self) -> Result<String, MinijinjaError> {
        match self.schema().as_str() {
            Some(val) => Ok(val.to_string()),
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect schema as string"
            ),
        }
    }

    fn schema_as_quoted_str(&self) -> Result<String, MinijinjaError> {
        match self.schema().as_str() {
            Some(val) => Ok(self.quoted(val)),
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect schema as string"
            ),
        }
    }

    /// Get the schema name as a string literal
    /// the same as how a database provider resolves and stores a schema component for a relation
    /// given how it's quoted
    fn schema_as_resolved_str(&self) -> Result<String, MinijinjaError> {
        match self.schema().as_str() {
            Some(val) => {
                if !self.quote_policy().schema {
                    Ok(self.normalize_component(val))
                } else {
                    Ok(val.to_string())
                }
            }
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect schema as string"
            ),
        }
    }

    /// Get the identifier
    fn identifier(&self) -> Value;

    /// Identifiers as string or error
    fn identifier_as_str(&self) -> Result<String, MinijinjaError> {
        match self.identifier().as_str() {
            Some(val) => Ok(val.to_string()),
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect identifier as string"
            ),
        }
    }

    /// Get the identifier as a string literal
    /// the same as how a database provider resolves and stores an identifier component for a relation
    /// given how it's quoted
    fn identifier_as_resolved_str(&self) -> Result<String, MinijinjaError> {
        match self.identifier().as_str() {
            Some(val) => {
                if !self.quote_policy().identifier {
                    Ok(self.normalize_component(val))
                } else {
                    Ok(val.to_string())
                }
            }
            None => jinja_err!(
                MinijinjaErrorKind::InvalidOperation,
                "expect identifier as string"
            ),
        }
    }
    /// Return the relation type if available, defaulting to None.
    fn relation_type(&self) -> Option<RelationType> {
        None
    }

    /// Return the relation type as a Value
    fn relation_type_as_value(&self) -> Value {
        Value::from_serialize(self.relation_type())
    }

    /// Get adapter type
    fn adapter_type(&self) -> Option<String>;

    /// Helper: check if the relation is a table
    fn is_table(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::Table))
    }

    /// Helper: check if the relation is a CTE
    fn is_cte(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::CTE) | Some(RelationType::Ephemeral)
        )
    }

    /// Helper: check if the relation is a view
    fn is_view(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::View))
    }

    /// Helper: check if the relation is a materialized view
    fn is_materialized_view(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::MaterializedView))
    }

    /// Helper: check if the relation is a streaming table
    fn is_streaming_table(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::StreamingTable))
    }

    /// Helper: check if the relation is a dynamic table
    fn is_dynamic_table(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::DynamicTable))
    }

    /// Helper: check if the relation is for a pointer table
    fn is_pointer(&self) -> bool {
        matches!(self.relation_type(), Some(RelationType::PointerTable))
    }

    /// Helper: is this relation renamable?
    fn can_be_renamed(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View)
        )
    }

    /// Helper: is this relation replaceable?
    fn can_be_replaced(&self) -> bool {
        matches!(
            self.relation_type(),
            Some(RelationType::Table) | Some(RelationType::View)
        )
    }

    /// Returns this relation as an object Value
    fn as_value(&self) -> Value;

    /// Get a metadata field from the relation
    /// If key is "metadata", returns a map with type information
    /// Otherwise simulate the behavior of a python dataclass
    fn get(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let key: String = args.get("key").unwrap();
        let default: Option<Value> = args.get("default").ok();

        if key == "metadata" {
            let mut map = BTreeMap::new();
            map.insert("type", Value::from(std::any::type_name::<Self>()));
            Ok(Value::from(map))
        } else {
            match key.as_str() {
                "database" => Ok(self.database()),
                "schema" => Ok(self.schema()),
                "identifier" => Ok(self.identifier()),
                _ => Ok(default.unwrap_or(Value::UNDEFINED)),
            }
        }
    }

    /// Replace path
    fn replace_path(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let database: Option<String> = args.consume_optional_only_from_kwargs("database");
        let schema: Option<String> = args.consume_optional_only_from_kwargs("schema");
        let identifier: Option<String> = args.consume_optional_only_from_kwargs("identifier");

        Ok(self
            .create_relation(
                Some(database.unwrap_or(self.database().as_str().unwrap().to_string())),
                Some(schema.unwrap_or(self.schema().as_str().unwrap().to_string())),
                Some(identifier.unwrap_or(self.identifier().as_str().unwrap().to_string())),
                self.relation_type(),
                self.quote_policy(),
            )?
            .as_value())
    }

    /// Quote a relation component (database, schema, or identifier)
    fn quoted(&self, s: &str) -> String {
        format!("{}{}{}", self.quote_character(), s, self.quote_character())
    }

    /// Get the semantic name fully qualified for a Relation
    ///
    /// A semantic name is meant to uniquely identify a relation
    /// agnostic of the literal values initially set for a Relation's component
    ///
    /// Implement [BaseRelation::normalize_relation_component] to complete the functionality
    /// ```
    fn semantic_fqn(&self) -> String {
        let mut parts = vec![];

        if let Ok(database) = self.database_as_str() {
            if self.quote_policy().database {
                parts.push(self.quoted(&database));
            } else {
                parts.push(self.quoted(&self.normalize_component(&database)));
            }
        }

        if let Ok(schema) = self.schema_as_str() {
            if self.quote_policy().schema {
                parts.push(self.quoted(&schema));
            } else {
                parts.push(self.quoted(&self.normalize_component(&schema)));
            }
        }

        if let Ok(identifier) = self.identifier_as_str() {
            if self.quote_policy().identifier {
                parts.push(self.quoted(&identifier));
            } else {
                parts.push(self.quoted(&self.normalize_component(&identifier)));
            }
        }

        parts.join(".")
    }

    /// Helper for
    ///
    /// * [BaseRelation::semantic_fqn]
    /// * [BaseRelation::schema_as_resolved_str]
    /// * [BaseRelation::identifier_as_resolved_str]
    /// * [BaseRelation::database_as_resolved_str]
    ///
    /// This is how a specific database provider resolve and store an object's name if quoting is not used
    /// For example, they'll be upper case in Snowflake https://docs.snowflake.com/en/sql-reference/identifiers-syntax#unquoted-identifiers
    fn normalize_component(&self, component: &str) -> String;

    /// Render this relation as a string
    fn render_self_as_str(&self) -> String {
        if let Some(RelationType::Ephemeral) = self.relation_type() {
            return format!("{}{}", DBT_CTE_PREFIX, self.identifier());
        }

        let include_policy = self.include_policy();
        let quote_policy = self.quote_policy();
        let mut parts: Vec<String> = Vec::new();

        let quote_part = |val: &str, quote_policy: bool| {
            if quote_policy {
                self.quoted(val)
            } else {
                val.to_string()
            }
        };

        if include_policy.database {
            if let Some(database) = self.database().as_str() {
                parts.push(quote_part(database, quote_policy.database));
            }
        }

        if include_policy.schema {
            if let Some(schema) = self.schema().as_str() {
                parts.push(quote_part(schema, quote_policy.schema));
            }
        }

        if include_policy.identifier {
            if let Some(identifier) = self.identifier().as_str() {
                parts.push(quote_part(identifier, quote_policy.identifier));
            }
        }

        parts.join(".")
    }

    /// Render this relation
    fn render_self(&self) -> Result<Value, MinijinjaError> {
        Ok(Value::from(self.render_self_as_str()))
    }

    /// Render this relation with a run filter
    fn render_with_run_filter(
        &self,
        run_filter: &RunFilter,
        event_time: &Option<String>,
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(render_with_run_filter_as_str(
            self.render_self_as_str(),
            run_filter,
            event_time,
        )))
    }

    /// Relation without any identifier
    fn without_identifier(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        let result = self
            .create_relation(
                Some(self.database().as_str().unwrap().to_string()),
                Some(self.schema().as_str().unwrap().to_string()),
                None,
                self.relation_type(),
                self.quote_policy(),
            )?
            .as_value();
        Ok(result)
    }

    /// Include a relation component (database, schema, or identifier)
    fn include(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);

        let database: bool = args
            .consume_optional_only_from_kwargs::<bool>("database")
            .unwrap_or(self.include_policy().database);
        let schema: bool = args
            .consume_optional_only_from_kwargs("schema")
            .unwrap_or(self.include_policy().schema);
        let identifier: bool = args
            .consume_optional_only_from_kwargs("identifier")
            .unwrap_or(self.include_policy().identifier);

        let include_policy = Policy {
            database,
            schema,
            identifier,
        };
        self.include_inner(include_policy)
    }

    /// Implement this to support `include`
    /// Replace the `include_policy` field with the input policy, and return that an update relation value
    fn include_inner(&self, _policy: Policy) -> Result<Value, MinijinjaError>;

    /// Incorporate
    fn incorporate(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let path: Option<Value> = args.consume_optional_only_from_kwargs("path");
        let relation_type: Option<Value> = args.consume_optional_only_from_kwargs("type");

        let (database, schema, identifier) = match path {
            Some(val) => match val.as_object() {
                Some(obj) => {
                    let database_value = obj.get_value(&Value::from("database"));
                    let schema_value = obj.get_value(&Value::from("schema"));
                    let identifier_value = obj.get_value(&Value::from("identifier"));

                    // Differentiate between "not provided" vs "provided but none"
                    let database = match database_value {
                        None => {
                            // Case 1: 'database' key was never provided in path
                            Some(self.database_as_str().unwrap())
                        }
                        Some(val) if val.is_none() => {
                            // Case 2: 'database' key was provided but set to none
                            None
                        }
                        Some(val) => {
                            // Case 3: 'database' key was provided with an actual value
                            Some(
                                val.as_str()
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| self.database_as_str().unwrap()),
                            )
                        }
                    };

                    // Similar logic for schema
                    let schema = match schema_value {
                        None => Some(self.schema_as_str().unwrap()), // Key not provided
                        Some(val) if val.is_none() => None,          // Key provided but none
                        Some(val) => Some(
                            val.as_str()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| self.schema_as_str().unwrap()),
                        ),
                    };

                    let identifier = match identifier_value {
                        None => Some(self.identifier_as_str().unwrap()), // Key not provided
                        Some(val) if val.is_none() => Some(self.identifier_as_str().unwrap()), // Key provided but none
                        Some(val) => Some(
                            val.as_str()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| self.identifier_as_str().unwrap()),
                        ),
                    };

                    (database, schema, identifier)
                }
                None => return invalid_argument!("incorrect 'path' value for incorporate"),
            },
            None => (
                Some(self.database_as_str()?),
                Some(self.schema_as_str()?),
                Some(self.identifier_as_str()?),
            ),
        };

        let relation_type = match relation_type {
            Some(val) => match val.as_str() {
                Some(type_str) => Some(RelationType::from(type_str)),
                None => self.relation_type(),
            },
            None => self.relation_type(),
        };

        Ok(self
            .create_relation(
                database,
                schema,
                identifier,
                relation_type,
                self.quote_policy(),
            )?
            .as_value())
    }

    /// Create a new relation with the specified components and policies.
    ///
    /// This method is used to create a new relation instance with the given database, schema,
    /// identifier, relation type, and quoting policy. It is a core method that enables several
    /// other relation operations:
    ///
    /// * [`BaseRelation::without_identifier`] - Clones a relation by setting its identifier to None
    /// * [`BaseRelation::replace_path`] - Clones a relation with updated path components
    /// * [`BaseRelation::incorporate`] - Clones a relation incorporating new path components
    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        quote_policy: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError>;

    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/relation.py#L183-L184
    fn information_schema(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &args, 0, 1)?;

        let view_name = args.get::<Value>("view_name")?;
        let view_name = match view_name.as_str() {
            Some(view_name) => view_name,
            None => return invalid_argument!("view_name must exist"),
        };

        self.information_schema_inner(self.database_as_str().ok(), view_name)
    }

    fn information_schema_inner(
        &self,
        database: Option<String>,
        view_name: &str,
    ) -> Result<Value, MinijinjaError>;

    /// needs_to_drop
    fn needs_to_drop(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        jinja_err!(
            MinijinjaErrorKind::InvalidOperation,
            "Only available for snowflake"
        )
    }

    /// get_ddl_prefix_for_create
    fn get_ddl_prefix_for_create(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        jinja_err!(
            MinijinjaErrorKind::InvalidOperation,
            "Only available for snowflake"
        )
    }

    /// get_ddl_prefix_for_alter
    fn get_ddl_prefix_for_alter(&self) -> Result<Value, MinijinjaError> {
        jinja_err!(
            MinijinjaErrorKind::InvalidOperation,
            "Only available for snowflake"
        )
    }

    /// get_iceberg_ddl_options
    fn get_iceberg_ddl_options(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        jinja_err!(
            MinijinjaErrorKind::InvalidOperation,
            "Only available for snowflake"
        )
    }

    /// dynamic_table_config_changeset
    fn dynamic_table_config_changeset(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        jinja_err!(
            MinijinjaErrorKind::InvalidOperation,
            "Only available for snowflake"
        )
    }

    /// from_config
    #[allow(clippy::wrong_self_convention)]
    fn from_config(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        jinja_err!(
            MinijinjaErrorKind::InvalidOperation,
            "from_config: Only available for Snowflake and Redshift"
        )
    }

    /// Get max name length
    fn relation_max_name_length(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Available only for postgres and redshift")
    }

    fn is_hive_metastore(&self) -> Value {
        unimplemented!("Available only for databricks")
    }

    /// materialized_view_config_changeset
    fn materialized_view_config_changeset(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("Available only for BigQuery and Redshift")
    }
}

/// Render this relation with a run filter.
///
// FIXME: for Postgres, we need to support _render_subquery_alias for the returned result
// reference: https://github.com/dbt-labs/dbt-adapters/blob/d2f725651c05be0de07f3152d5b4842feae8a18a/dbt-adapters/src/dbt/adapters/base/relation.py#L222
pub fn render_with_run_filter_as_str(
    rendered: String,
    run_filter: &RunFilter,
    event_time: &Option<String>,
) -> String {
    let rendered = if run_filter.empty {
        format!("(select * from {rendered} limit 0)")
    } else {
        rendered
    };

    // TODO(harry): warn? error is not a good idea here since this is used by Object::render method
    // the caller returns a fmt::Error that cannot carry extra error message, and suggested to be infallible
    if let Some(ref sample) = run_filter.sample {
        if (sample.start.is_some() || sample.end.is_some()) && event_time.is_none() {
            return rendered;
        }
    }

    let start = run_filter
        .sample
        .as_ref()
        .and_then(|s| s.start.map(|s| PyDateTime::new_naive(s.naive_utc())));
    let end = run_filter
        .sample
        .as_ref()
        .and_then(|s| s.end.map(|s| PyDateTime::new_naive(s.naive_utc())));

    let filter = match (end, start) {
        (Some(end), Some(start)) => {
            format!(
                "{} >= '{}' and {} < '{}'",
                event_time.as_ref().unwrap(),
                start.isoformat(),
                event_time.as_ref().unwrap(),
                end.isoformat()
            )
        }
        (Some(end), None) => {
            format!("{} < '{}'", event_time.as_ref().unwrap(), end.isoformat())
        }
        (None, Some(start)) => {
            format!(
                "{} >= '{}'",
                event_time.as_ref().unwrap(),
                start.isoformat()
            )
        }
        (None, None) => return rendered,
    };

    format!("(select * from {rendered} where {filter})")
}

#[cfg(test)]
mod tests {
    use crate::filter::Sample;
    use chrono::{DateTime, NaiveDate, Utc};
    use minijinja::value::Kwargs;

    use super::*;
    use std::collections::BTreeMap;

    #[derive(Debug)]
    struct TestRelation {
        database: String,
        schema: String,
        identifier: String,
        quote_policy: Policy,
    }

    impl BaseRelationProperties for TestRelation {
        fn include_policy(&self) -> Policy {
            Policy::enabled()
        }

        fn quote_policy(&self) -> Policy {
            self.quote_policy
        }

        fn quote_character(&self) -> char {
            '"'
        }

        fn get_database(&self) -> FsResult<String> {
            Ok(self.database.clone())
        }

        fn get_schema(&self) -> FsResult<String> {
            Ok(self.schema.clone())
        }

        fn get_identifier(&self) -> FsResult<String> {
            Ok(self.identifier.clone())
        }
    }

    impl BaseRelation for TestRelation {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn database(&self) -> Value {
            Value::from(self.database.clone())
        }

        fn schema(&self) -> Value {
            Value::from(self.schema.clone())
        }

        fn identifier(&self) -> Value {
            Value::from(self.identifier.clone())
        }

        fn create_from(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
            Ok(Value::from("test"))
        }

        fn adapter_type(&self) -> Option<String> {
            Some("test".to_string())
        }

        fn as_value(&self) -> Value {
            Value::from("test")
        }

        fn without_identifier(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
            Ok(Value::from("test"))
        }

        fn include(&self, _args: &[Value]) -> Result<Value, MinijinjaError> {
            Ok(Value::from("test"))
        }

        fn include_inner(&self, _policy: Policy) -> Result<Value, MinijinjaError> {
            Ok(Value::from("test"))
        }

        fn normalize_component(&self, component: &str) -> String {
            component.to_lowercase()
        }

        fn create_relation(
            &self,
            _database: Option<String>,
            _schema: Option<String>,
            _identifier: Option<String>,
            _relation_type: Option<RelationType>,
            _quote_policy: Policy,
        ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
            unimplemented!("base relation creation from components")
        }

        fn information_schema_inner(
            &self,
            _database: Option<String>,
            _view_name: &str,
        ) -> Result<Value, MinijinjaError> {
            unimplemented!("InformationSchema")
        }
    }

    #[test]
    fn test_get_database() {
        let relation = TestRelation {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            identifier: "test_table".to_string(),
            quote_policy: Policy::enabled(),
        };
        let mut map = BTreeMap::new();
        map.insert("key", Value::from("database"));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = relation.get(&args).unwrap();
        assert_eq!(result, Value::from("test_db"));
    }

    #[test]
    fn test_get_schema() {
        let relation = TestRelation {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            identifier: "test_table".to_string(),
            quote_policy: Policy::enabled(),
        };
        let mut map = BTreeMap::new();
        map.insert("key", Value::from("schema"));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = relation.get(&args).unwrap();
        assert_eq!(result, Value::from("test_schema"));
    }

    #[test]
    fn test_get_identifier() {
        let relation = TestRelation {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            identifier: "test_table".to_string(),
            quote_policy: Policy::enabled(),
        };
        let mut map = BTreeMap::new();
        map.insert("key", Value::from("identifier"));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = relation.get(&args).unwrap();
        assert_eq!(result, Value::from("test_table"));
    }

    #[test]
    fn test_get_metadata() {
        let relation = TestRelation {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            identifier: "test_table".to_string(),
            quote_policy: Policy::enabled(),
        };
        let mut map = BTreeMap::new();
        map.insert("key", Value::from("metadata"));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = relation.get(&args).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(
            "type",
            Value::from("dbt_schemas::schemas::relations::base::tests::TestRelation"),
        );
        assert_eq!(result, Value::from(expected));
    }

    #[test]
    fn test_get_nonexistent_with_default() {
        let relation = TestRelation {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            identifier: "test_table".to_string(),
            quote_policy: Policy::enabled(),
        };
        let mut map = BTreeMap::new();
        map.insert("key", Value::from("nonexistent"));
        map.insert("default", Value::from("default_value"));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = relation.get(&args).unwrap();
        assert_eq!(result, Value::from("default_value"));
    }

    #[test]
    fn test_get_nonexistent_without_default() {
        let relation = TestRelation {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            identifier: "test_table".to_string(),
            quote_policy: Policy::enabled(),
        };
        let mut map = BTreeMap::new();
        map.insert("key", Value::from("nonexistent"));
        let args = vec![Value::from(Kwargs::from_iter(map))];
        let result = relation.get(&args).unwrap();
        assert_eq!(result, Value::UNDEFINED);
    }

    #[test]
    fn test_normalized_fqn_all_quoted() {
        let relation = TestRelation {
            database: "MyDB".to_string(),
            schema: "MySchema".to_string(),
            identifier: "MyTable".to_string(),
            quote_policy: Policy {
                database: true,
                schema: true,
                identifier: true,
            },
        };

        assert_eq!(relation.semantic_fqn(), "\"MyDB\".\"MySchema\".\"MyTable\"");
    }

    #[test]
    fn test_normalized_fqn_none_quoted() {
        let relation = TestRelation {
            database: "MyDB".to_string(),
            schema: "MySchema".to_string(),
            identifier: "MyTable".to_string(),
            quote_policy: Policy {
                database: false,
                schema: false,
                identifier: false,
            },
        };

        assert_eq!(relation.semantic_fqn(), "\"mydb\".\"myschema\".\"mytable\"");
    }

    #[test]
    fn test_normalized_fqn_mixed_quoted() {
        let relation = TestRelation {
            database: "MyDB".to_string(),
            schema: "MySchema".to_string(),
            identifier: "MyTable".to_string(),
            quote_policy: Policy {
                database: true,
                schema: false,
                identifier: true,
            },
        };

        assert_eq!(relation.semantic_fqn(), "\"MyDB\".\"myschema\".\"MyTable\"");
    }

    #[test]
    fn test_render_with_run_filter_empty() {
        let run_filter = RunFilter {
            empty: true,
            sample: None,
        };
        let rendered = "my_table".to_string();
        let event_time = None;

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(result, "(select * from my_table limit 0)");
    }

    #[test]
    fn test_render_with_run_filter_no_sample() {
        let run_filter = RunFilter {
            empty: false,
            sample: None,
        };
        let rendered = "my_table".to_string();
        let event_time = Some("created_at".to_string());

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(result, "my_table");
    }

    #[test]
    fn test_render_with_run_filter_both_start_and_end() {
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 7, 8)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: Some(DateTime::<Utc>::from_naive_utc_and_offset(end, Utc)),
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let rendered = "my_table".to_string();
        let event_time = Some("created_at".to_string());

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from my_table where created_at >= '2024-07-01T00:00:00' and created_at < '2024-07-08T18:00:00')"
        );
    }

    #[test]
    fn test_render_with_run_filter_start_only() {
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: None,
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let rendered = "my_table".to_string();
        let event_time = Some("created_at".to_string());

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from my_table where created_at >= '2024-07-01T00:00:00')"
        );
    }

    #[test]
    fn test_render_with_run_filter_end_only() {
        let end = NaiveDate::from_ymd_opt(2024, 7, 8)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap();

        let sample = Sample {
            start: None,
            end: Some(DateTime::<Utc>::from_naive_utc_and_offset(end, Utc)),
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let rendered = "my_table".to_string();
        let event_time = Some("created_at".to_string());

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from my_table where created_at < '2024-07-08T18:00:00')"
        );
    }

    #[test]
    fn test_render_with_run_filter_sample_none_values() {
        let sample = Sample {
            start: None,
            end: None,
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let rendered = "my_table".to_string();
        let event_time = Some("created_at".to_string());

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(result, "my_table");
    }

    #[test]
    fn test_render_with_run_filter_no_event_time_error() {
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: None,
        };

        let run_filter = RunFilter {
            empty: false,
            sample: Some(sample),
        };
        let rendered = "my_table".to_string();
        let event_time = None;

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(result, "my_table");
    }

    #[test]
    fn test_render_with_run_filter_empty_and_sample() {
        let start = NaiveDate::from_ymd_opt(2024, 7, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 7, 8)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap();

        let sample = Sample {
            start: Some(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc)),
            end: Some(DateTime::<Utc>::from_naive_utc_and_offset(end, Utc)),
        };

        let run_filter = RunFilter {
            empty: true,
            sample: Some(sample),
        };
        let rendered = "my_table".to_string();
        let event_time = Some("created_at".to_string());

        let result = render_with_run_filter_as_str(rendered, &run_filter, &event_time);
        assert_eq!(
            result,
            "(select * from (select * from my_table limit 0) where created_at >= '2024-07-01T00:00:00' and created_at < '2024-07-08T18:00:00')"
        );
    }
}
