use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    sync::Arc,
};

use crate::{
    AdapterResult, AdapterType,
    errors::{AdapterError, AdapterErrorKind},
};
use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use dbt_common::FsResult;
use dbt_schemas::schemas::{
    common::ResolvedQuoting,
    relations::base::{BaseRelation, BaseRelationProperties},
};
use minijinja::{State, Value};

pub(crate) mod bigquery;
pub(crate) mod databricks;
pub(crate) mod metadata_adapter;
pub(crate) mod redshift;
pub mod snowflake; // XXX: temporarily pub before the refactor is complete

// Re-export `metadata_adapter` symbols
// NOTE: this is temporary until all the metadata-releated code
// is verticalized and moved to the metadata module.
pub use metadata_adapter::*;

pub const ARROW_FIELD_COMMENT_METADATA_KEY: &str = "comment";
// TODO(jason): This needs to be updated to match the driver convention once available
pub const ARROW_FIELD_ORIGINAL_TYPE_METADATA_KEY: &str = "type_text";

pub type WhereClausesByDb = BTreeMap<String, Vec<String>>;
pub type RelationsByDb = BTreeMap<String, Vec<Arc<dyn BaseRelation>>>;

/// The two ways of representing a relation in a pair.
pub type RelationSchemaPair = (Arc<dyn BaseRelation>, Arc<Schema>);

/// A collection of relations
pub type RelationVec = Vec<Arc<dyn BaseRelation>>;

/// A struct representing a catalog and a schema
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct CatalogAndSchema {
    pub rendered_catalog: String,
    pub rendered_schema: String,
    pub resolved_catalog: String,
    pub resolved_schema: String,
}

impl fmt::Display for CatalogAndSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.rendered_catalog.is_empty() {
            write!(f, "{}", self.rendered_schema)
        } else if self.rendered_schema.is_empty() {
            write!(f, "{}", self.rendered_catalog)
        } else {
            write!(f, "{}.{}", self.rendered_catalog, self.rendered_schema)
        }
    }
}

impl From<&Arc<dyn BaseRelation>> for CatalogAndSchema {
    fn from(relation: &Arc<dyn BaseRelation>) -> Self {
        let rendered_catalog =
            relation.quoted(&relation.database_as_resolved_str().unwrap_or_default());
        let rendered_schema =
            relation.quoted(&relation.schema_as_resolved_str().unwrap_or_default());

        let resolved_catalog = relation.database_as_resolved_str().unwrap_or_default();
        let resolved_schema = relation.schema_as_resolved_str().unwrap_or_default();

        assert!(
            !(rendered_catalog.is_empty() && rendered_schema.is_empty()),
            "Either rendered_catalog or rendered_schema must be present"
        );

        Self {
            rendered_catalog,
            rendered_schema,
            resolved_catalog,
            resolved_schema,
        }
    }
}

pub struct MetadataFreshness {
    pub last_altered: i128,
    pub is_view: bool,
}

/// Allows serializing record batches into maps and Arrow schemas
pub trait MetadataProcessor {
    // Implementers can choose the map key/value
    type Key: Ord + Clone;
    type Value: Clone;

    fn into_metadata(self) -> BTreeMap<Self::Key, Self::Value>;
    fn from_record_batch(batch: Arc<RecordBatch>) -> AdapterResult<Self>
    where
        Self: Sized;
    fn to_arrow_schema(&self) -> AdapterResult<Arc<Schema>>;
}

/// This represents a UDF downloaded from a remote data warehouse
#[derive(Debug, Clone)]
pub struct UDF {
    pub name: String,
    pub description: String,
    pub signature: String,
    pub adapter_type: AdapterType,
    pub kind: UDFKind,
}

#[derive(Debug, Clone, Copy)]
pub enum UDFKind {
    Scalar,
    Aggregate,
    Table,
}

/// Map a cell from a two-value array of strings into a boolean
///
/// Postcondition: either true or false or an propogate error for unexpected values
pub fn try_canonicalize_bool_column_field(column_value: &str) -> Result<bool, AdapterError> {
    const TRUTH_VALUES: [&str; 3] = ["1", "y", "yes"];
    const FALSE_VALUES: [&str; 3] = ["0", "n", "no"];

    if TRUTH_VALUES
        .iter()
        .any(|s| column_value.eq_ignore_ascii_case(s))
    {
        return Ok(true);
    }
    if FALSE_VALUES
        .iter()
        .any(|s| column_value.eq_ignore_ascii_case(s))
    {
        return Ok(false);
    }

    Err(AdapterError::new(
        AdapterErrorKind::UnexpectedResult,
        format!("Cannot convert unexpected column value '{column_value}' to boolean."),
    ))
}

pub fn new_arrow_field_with_metadata(
    col_name: &str,
    data_type: DataType,
    nullable: bool,
    original_type_text: Option<String>,
    comment: Option<String>,
    additional_metadata: Option<HashMap<String, String>>,
) -> Field {
    let field = Field::new(col_name, data_type, nullable);
    let mut field_metadata = additional_metadata.unwrap_or_default();

    if let Some(original_type_text) = original_type_text {
        field_metadata.insert(
            ARROW_FIELD_ORIGINAL_TYPE_METADATA_KEY.to_string(),
            original_type_text,
        );
    }
    if let Some(comment) = comment {
        field_metadata.insert(ARROW_FIELD_COMMENT_METADATA_KEY.to_string(), comment);
    }
    field.with_metadata(field_metadata)
}

pub fn get_input_schema_database_and_table(
    relation: &Arc<dyn BaseRelation>,
) -> AdapterResult<(String, String, String)> {
    let table_name = relation.semantic_fqn();

    let parts: Vec<&str> = table_name.split('.').collect();
    if parts.len() != 3 {
        return Err(AdapterError::new(
            AdapterErrorKind::UnexpectedResult,
            format!("Invalid table name format: {table_name}"),
        ));
    }
    // database will be used as an identifier
    let database = table_name.split('.').next().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::UnexpectedResult,
            "relation database should not be None",
        )
    })?;

    // schema and table will be used as string literals
    let input_schema = relation.schema_as_resolved_str().map_err(|_| {
        AdapterError::new(
            AdapterErrorKind::UnexpectedResult,
            "relation schema should not be None",
        )
    })?;
    let input_table = relation.identifier_as_resolved_str().map_err(|e| {
        AdapterError::new(
            AdapterErrorKind::UnexpectedResult,
            format!("relation identifier should not be None: {e}"),
        )
    })?;

    Ok((input_schema, database.to_owned(), input_table))
}

/// Builds and returns ([WhereClausesByDb], [RelationsByDb]) from a list of [BaseRelation]
/// [WhereClausesByDb] maps databases to statements that select their schema+tables in the relation
/// [RelationsByDb] keys the database to the cloned [BaseRelation]
/// We expect a fqn from the relation in format <database>.<schema>.<table>
pub fn build_relation_clauses(
    relations: &[Arc<dyn BaseRelation>],
) -> AdapterResult<(WhereClausesByDb, RelationsByDb)> {
    // Build the where clause for all relations grouped by databases
    let mut where_clauses_by_database = BTreeMap::new();
    let mut relations_by_database = BTreeMap::new();
    for relation in relations {
        let (input_schema, database, input_table) = get_input_schema_database_and_table(relation)?;

        where_clauses_by_database
            .entry(database.to_owned())
            .or_insert_with(Vec::new)
            .push(format!(
                "table_schema = '{input_schema}' and table_name = '{input_table}'"
            ));
        relations_by_database
            .entry(database.to_owned())
            .or_insert_with(Vec::new)
            .push(relation.clone());
    }
    Ok((where_clauses_by_database, relations_by_database))
}

pub fn find_matching_relation(
    schema: &str,
    table: &str,
    relations: &Vec<Arc<dyn BaseRelation>>,
) -> AdapterResult<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    // Find the matching relation
    for relation in relations {
        let table_name = relation.semantic_fqn();
        // schema and table will be used as string literals
        let input_schema = relation.schema_as_resolved_str().map_err(|_| {
            AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "relation schema should not be None",
            )
        })?;
        let input_table = relation.identifier_as_resolved_str().map_err(|_| {
            AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "relation identifier should not be None",
            )
        })?;
        if schema == input_schema && table == input_table {
            out.insert(table_name);
            break;
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct MockBaseRelation {
    database: String,
    schema: String,
    identifier: String,
    quote_policy: ResolvedQuoting,
}

impl MockBaseRelation {
    #[allow(dead_code)]
    fn new(database: String, schema: String, identifier: String) -> Self {
        Self {
            database,
            schema,
            identifier,
            quote_policy: ResolvedQuoting {
                database: true,
                schema: true,
                identifier: true,
            },
        }
    }
}

impl BaseRelationProperties for MockBaseRelation {
    fn is_database_relation(&self) -> bool {
        false
    }

    fn include_policy(&self) -> ResolvedQuoting {
        ResolvedQuoting {
            database: true,
            schema: true,
            identifier: true,
        }
    }

    fn quote_policy(&self) -> ResolvedQuoting {
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

impl BaseRelation for MockBaseRelation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn create_from(&self, _state: &State, _args: &[Value]) -> Result<Value, minijinja::Error> {
        Ok(Value::from("test"))
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

    fn adapter_type(&self) -> Option<String> {
        Some("test".to_string())
    }

    fn as_value(&self) -> Value {
        Value::from("test")
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_lowercase()
    }

    fn create_relation(
        &self,
        _database: Option<String>,
        _schema: Option<String>,
        _identifier: Option<String>,
        _relation_type: Option<dbt_schemas::dbt_types::RelationType>,
        _quote_policy: ResolvedQuoting,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("relation creation in metadata adapter")
    }

    fn information_schema_inner(
        &self,
        _database: Option<String>,
        _view_name: &str,
    ) -> Result<Value, minijinja::Error> {
        unimplemented!("information schema query generation in metadata adapter")
    }

    fn include_inner(&self, _policy: ResolvedQuoting) -> Result<Value, minijinja::Error> {
        Ok(Value::from("test"))
    }

    fn semantic_fqn(&self) -> String {
        let mut parts = vec![];

        if self.quote_policy().database {
            parts.push(self.quoted(&self.database));
        } else {
            parts.push(self.quoted(&self.normalize_component(&self.database)));
        }

        if self.quote_policy().schema {
            parts.push(self.quoted(&self.schema));
        } else {
            parts.push(self.quoted(&self.normalize_component(&self.schema)));
        }

        if self.quote_policy().identifier {
            parts.push(self.quoted(&self.identifier));
        } else {
            parts.push(self.quoted(&self.normalize_component(&self.identifier)));
        }

        parts.join(".")
    }

    fn schema_as_resolved_str(&self) -> Result<String, minijinja::Error> {
        Ok(self.schema.clone())
    }

    fn identifier_as_resolved_str(&self) -> Result<String, minijinja::Error> {
        Ok(self.identifier.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_relation_clauses() {
        let relations = vec![
            Arc::new(MockBaseRelation::new(
                "db1".to_string(),
                "schema1".to_string(),
                "table1".to_string(),
            )) as Arc<dyn BaseRelation>,
            Arc::new(MockBaseRelation::new(
                "db1".to_string(),
                "schema2".to_string(),
                "table2".to_string(),
            )) as Arc<dyn BaseRelation>,
            Arc::new(MockBaseRelation::new(
                "db2".to_string(),
                "schema1".to_string(),
                "table3".to_string(),
            )) as Arc<dyn BaseRelation>,
        ];

        let (where_clauses, relations_by_db) = build_relation_clauses(&relations).unwrap();

        // Test where clauses
        assert_eq!(where_clauses.len(), 2);
        assert_eq!(
            where_clauses.get("\"db1\"").unwrap(),
            &vec![
                "table_schema = 'schema1' and table_name = 'table1'",
                "table_schema = 'schema2' and table_name = 'table2'"
            ]
        );
        assert_eq!(
            where_clauses.get("\"db2\"").unwrap(),
            &vec!["table_schema = 'schema1' and table_name = 'table3'"]
        );

        // Test relations by database
        assert_eq!(relations_by_db.len(), 2);
        assert_eq!(relations_by_db.get("\"db1\"").unwrap().len(), 2);
        assert_eq!(relations_by_db.get("\"db2\"").unwrap().len(), 1);
    }

    #[test]
    fn test_build_relation_clauses_invalid_fqn() {
        let relations = vec![Arc::new(MockBaseRelation::new(
            "invalid.fqn".to_string(), // This will cause an error as it contains a dot
            "schema1".to_string(),
            "table1".to_string(),
        )) as Arc<dyn BaseRelation>];

        let result = build_relation_clauses(&relations);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid table name format")
        );
    }

    #[test]
    fn test_find_matching_relation() {
        let relations = vec![
            Arc::new(MockBaseRelation::new(
                "db1".to_string(),
                "schema1".to_string(),
                "table1".to_string(),
            )) as Arc<dyn BaseRelation>,
            Arc::new(MockBaseRelation::new(
                "db1".to_string(),
                "schema2".to_string(),
                "table2".to_string(),
            )) as Arc<dyn BaseRelation>,
        ];

        let result = find_matching_relation("schema1", "table1", &relations).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("\"db1\".\"schema1\".\"table1\""));

        let result = find_matching_relation("schema2", "table2", &relations).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("\"db1\".\"schema2\".\"table2\""));

        let result = find_matching_relation("nonexistent", "table", &relations).unwrap();
        assert_eq!(result.len(), 0);
    }
}
