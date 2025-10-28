use crate::base_adapter::{AdapterType, AdapterTyping};
use crate::bigquery::relation::BigqueryRelation;
use crate::bigquery::relation_config::BigqueryMaterializedViewConfigObject;
use crate::bigquery::relation_config::{
    BigqueryMaterializedViewConfig, BigqueryPartitionConfigExt, cluster_by_from_schema,
    partitions_match,
};
use crate::cast_util::downcast_value_to_dyn_base_relation;
use crate::column::ColumnBuilder;
use crate::columns::{BigqueryColumnMode, StdColumn};
use crate::errors::{
    AdapterError, AdapterErrorKind, AdapterResult, adbc_error_to_adapter_error,
    arrow_error_to_adapter_error,
};
use crate::funcs::{execute_macro, none_value};
use crate::metadata::*;
use crate::query_ctx::query_ctx_from_state;
use crate::record_batch_utils::get_column_values;
use crate::relation_object::RelationObject;
use crate::render_constraint::render_column_constraint;
use crate::sql_engine::SqlEngine;
use crate::typed_adapter::TypedBaseAdapter;
use adbc_core::options::OptionValue;
use arrow::array::StringArray;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use dashmap::DashMap;
use dbt_agate::AgateTable;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::CommonAttributes;
use dbt_schemas::schemas::common::{ConstraintSupport, ConstraintType};
use dbt_schemas::schemas::dbt_column::DbtColumn;
use dbt_schemas::schemas::manifest::{
    BigqueryClusterConfig, BigqueryPartitionConfig, PartitionConfig,
};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::schemas::serde::minijinja_value_to_typed_struct;
use dbt_xdbc::bigquery::{
    INGEST_FILE_DELIMITER, INGEST_PATH, INGEST_SCHEMA, QUERY_DESTINATION_TABLE,
    UPDATE_DATASET_AUTHORIZE_VIEW_TO_DATASETS, UPDATE_TABLE_COLUMNS_DESCRIPTION,
};
use dbt_xdbc::{Connection, QueryCtx};
use indexmap::IndexMap;
use minijinja::value::mutable_vec::MutableVec;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::sync::{Arc, LazyLock};

pub const ADBC_EXECUTE_INVOCATION_OPTION: &str = "dbt_invocation_id";

/// An adapter for interacting with Bigquery.
#[derive(Clone)]
pub struct BigqueryAdapter {
    engine: Arc<SqlEngine>,
}

impl Debug for BigqueryAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl BigqueryAdapter {
    pub fn new(engine: Arc<SqlEngine>) -> Self {
        Self { engine }
    }

    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L750-L751
    pub fn get_common_table_options(
        &self,
        _state: &State,
        config: ModelConfig,
        common_attr: &CommonAttributes,
        temporary: bool,
    ) -> BTreeMap<String, Value> {
        let mut result = BTreeMap::new();

        if let Some(hours) = config.__warehouse_specific_config__.hours_to_expiration
            && !temporary
        {
            let expiration = format!("TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {hours} hour)");
            result.insert("expiration_timestamp".to_string(), Value::from(expiration));
        }

        // Handle description if persist_docs is enabled
        if let Some(persist_docs) = &config.persist_docs
            && persist_docs.relation.unwrap_or(false)
            && let Some(description) = &common_attr.description
        {
            let escaped_description = description.replace('\\', "\\\\").replace('"', "\\\"");
            result.insert(
                "description".to_string(),
                Value::from(format!("\"\"\"{escaped_description}\"\"\"")),
            );
        }

        let mut labels = config
            .__warehouse_specific_config__
            .labels
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| Value::from_iter(vec![Value::from(key), Value::from(value)]))
            .collect::<Vec<_>>();

        // https://github.com/dbt-labs/dbt-adapters/pull/890
        // Merge with priority to labels
        if config
            .__warehouse_specific_config__
            .labels_from_meta
            .unwrap_or_default()
            && let Some(meta) = &config.meta
        {
            // Convert meta values to strings
            for (key, value) in meta {
                labels.push(Value::from_iter(vec![
                    Value::from(key),
                    value.as_str().map(Value::from).unwrap_or_default(),
                ]));
            }
        }

        // Add labels to opts if any exist
        if !labels.is_empty() {
            result.insert(
                "labels".to_string(),
                Value::from_object(MutableVec::from_iter(labels)),
            );
        }

        result
    }
}

impl AdapterTyping for BigqueryAdapter {
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        Some(self)
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<SqlEngine> {
        &self.engine
    }
}

impl TypedBaseAdapter for BigqueryAdapter {
    /// Bigquery does not support add_query
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L476-L477
    #[allow(clippy::too_many_arguments)]
    fn add_query(
        &self,
        _ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        _sql: &str,
        _auto_begin: bool,
        _bindings: Option<&Value>,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            "bigquery.add_query",
        ))
    }

    /// BigQuery does not support truncate_relation
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L173-L174
    fn truncate_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        Err(AdapterError::new(
            AdapterErrorKind::NotSupported,
            "bigquery.truncate_relation",
        ))
    }

    fn quote(&self, _state: &State, identifier: &str) -> AdapterResult<String> {
        Ok(format!("`{identifier}`"))
    }

    fn get_relation(
        &self,
        state: &State,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        let query_database = if self.quoting().database {
            self.quote(state, database)?
        } else {
            database.to_string()
        };
        let query_schema = if self.quoting().schema {
            self.quote(state, schema)?
        } else {
            schema.to_string()
        };

        let query_identifier = if self.quoting().identifier {
            identifier.to_string()
        } else {
            identifier.to_lowercase()
        };

        let sql = format!(
            "SELECT table_catalog,
                    table_schema,
                    table_name,
                    table_type
                FROM {query_database}.{query_schema}.INFORMATION_SCHEMA.TABLES
                WHERE table_name = '{query_identifier}';",
        );

        let result = self.engine.execute(Some(state), conn, ctx, &sql);
        let batch = match result {
            Ok(batch) => batch,
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("Dataset") && err_msg.contains("was not found") {
                    return Ok(None);
                } else {
                    return Err(err);
                }
            }
        };

        if batch.num_rows() == 0 {
            // If there are no rows, then we did not find the object
            return Ok(None);
        }

        let column = batch.column_by_name("table_type").unwrap();
        let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();

        let relation_type_name = string_array.value(0).to_uppercase();
        let relation_type =
            RelationType::from_adapter_type(AdapterType::Bigquery, &relation_type_name);

        let mut relation = BigqueryRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            Some(relation_type),
            None,
            self.quoting(),
        );
        let relation_value = Value::from_object(RelationObject::new(Arc::new(relation.clone())));
        let location = self.get_dataset_location(state, conn, relation_value)?;
        relation.location = location;
        Ok(Some(Arc::new(relation)))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L246-L255
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<StdColumn>> {
        // TODO(serramatutu): once this is moved over to Arrow, let's remove the fallback to DbtCoreBaseColumn
        // from StdColumn::vec_from_jinja_value for BigQuery
        // FIXME(harry): the Python version uses googleapi GetTable, that doesn't return pseudocolumn like _PARTITIONDATE or _PARTITIONTIME
        let result = match execute_macro(
            state,
            &[RelationObject::new(relation).as_value()],
            "get_columns_in_relation",
        ) {
            Ok(result) => result,
            Err(err) => {
                // Handle NotFound errors
                if err.kind() == AdapterErrorKind::NotFound
                    || (err.kind() == AdapterErrorKind::UnexpectedResult
                        && err.message().contains("Error 404: Not found"))
                {
                    return Ok(Vec::new());
                }
                return Err(err);
            }
        };
        Ok(StdColumn::vec_from_jinja_value(
            AdapterType::Bigquery,
            result,
        )?)
    }

    /// This only supports non-nested columns additions
    ///
    /// Since internally this is only used by snapshot materialization macro where newly added columns all have non-nested data types,
    /// Read from [here](https://github.com/sdf-labs/fs/blob/9b87be839f6aa54cab1ab91cde2c77855758c396/crates/dbt-loader/src/dbt_macro_assets/dbt-adapters/macros/materializations/snapshots/snapshot.sql#L32-L33).
    /// This builds sql that creates the snapshot relation, and this relation only adds non-nested columns to the source relation
    /// it is supposed to work well for this use case
    /// due to limitation: https://cloud.google.com/bigquery/docs/managing-table-schemas#add_a_nested_column_to_a_record_column
    fn alter_table_add_columns(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: Value,
        columns: Value,
    ) -> AdapterResult<Value> {
        let relation = downcast_value_to_dyn_base_relation(&relation)?;
        let table = relation.identifier_as_str()?;
        let schema = relation.schema_as_str()?;

        let columns = StdColumn::vec_from_jinja_value(AdapterType::Bigquery, columns)?;
        if columns.is_empty() {
            return Ok(none_value());
        }

        let add_columns: Vec<String> = columns
            .iter()
            .map(|col| format!("ADD COLUMN {} {}", col.name(), &col.dtype()))
            .collect();

        let sql = format!(
            "ALTER TABLE {schema}.{table}
            {}",
            add_columns.join("\n,")
        );
        let ctx = query_ctx_from_state(state)?.with_desc("alter_table_add_columns adapter call");
        self.engine.execute_with_options(
            Some(state),
            &ctx,
            conn,
            &sql,
            self.get_adbc_execute_options(state),
            false,
        )?;

        Ok(none_value())
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/4b3966efc50b1d013907a88bee4ab8ebd022d17a/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L668
    // TODO: Because we don't support custom materialization yet, we're breaking this one. Later we can document
    // to end users that their old way of using this macro is bugged. The fix will be trivial for
    // any power user relying on this adapter method and we can provide clear guidance for
    // migration.
    fn load_dataframe(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        database: &str,
        schema: &str,
        table_name: &str,
        agate_table: Arc<AgateTable>,
        file_path: &str,
        field_delimiter: &str,
    ) -> AdapterResult<Value> {
        let serialized_ingest_schema: Vec<u8> = {
            // serialize the Arrow schema as an Arrow IPC byte blob
            let mut buf = Vec::<u8>::new();
            let () = StreamWriter::try_new(
                &mut buf,
                agate_table.original_record_batch().schema().as_ref(),
            )
            .and_then(|mut w| w.finish())
            .map_err(arrow_error_to_adapter_error)?;
            Ok(buf) as AdapterResult<Vec<u8>>
        }?;

        self.engine.execute_with_options(
            None,
            ctx,
            conn,
            sql,
            vec![
                (
                    QUERY_DESTINATION_TABLE.to_string(),
                    OptionValue::String(format!("{database}.{schema}.{table_name}")),
                ),
                (
                    INGEST_FILE_DELIMITER.to_string(),
                    OptionValue::String(field_delimiter.to_string()),
                ),
                (
                    INGEST_PATH.to_string(),
                    OptionValue::String(file_path.to_string()),
                ),
                (
                    INGEST_SCHEMA.to_string(),
                    OptionValue::Bytes(serialized_ingest_schema),
                ),
            ],
            false,
        )?;

        Ok(none_value())
    }

    /// This was update_columns method from bigquery-adapter where googleapi is used to update/merge columns in general
    ///
    /// But since internally this is is only used to update columns descriptions, by bigquery__alter_column_comment macro
    /// and due to limitation of bigquery, we cannot update nested columns using SQL
    /// the implementation here only supports columns descriptions update
    fn update_columns_descriptions(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: Value,
        columns: IndexMap<String, DbtColumn>,
    ) -> AdapterResult<Value> {
        let relation = downcast_value_to_dyn_base_relation(&relation)?;
        let database = relation.database_as_str()?;
        let table = relation.identifier_as_str()?;
        let schema = relation.schema_as_str()?;

        let columns = self.nest_column_data_types(columns, None)?;

        let column_to_description = columns
            .iter()
            .filter_map(|(name, col)| {
                col.description
                    .as_ref()
                    .map(|desc| (name.to_string(), desc.to_string()))
            })
            .collect::<BTreeMap<String, String>>();

        // The heavy lift is delegated to the driver via googleapi Table.update
        // since ALTER TABLE ... ALTER COLUMNS doesn't support updating a view
        let mut options = self.get_adbc_execute_options(state);
        options.extend(vec![
            (
                QUERY_DESTINATION_TABLE.to_string(),
                OptionValue::String(format!("{database}.{schema}.{table}")),
            ),
            (
                UPDATE_TABLE_COLUMNS_DESCRIPTION.to_string(),
                OptionValue::String(
                    serde_json::to_string(&column_to_description)
                        .expect("Failed to serialize column_to_description"),
                ),
            ),
        ]);

        let ctx = query_ctx_from_state(state)?;
        let sql = "none";
        self.engine
            .execute_with_options(Some(state), &ctx, conn, sql, options, false)?;
        Ok(none_value())
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L924
    fn render_raw_columns_constraints(
        &self,
        columns_map: IndexMap<String, DbtColumn>,
    ) -> AdapterResult<Vec<String>> {
        let mut rendered_constraints: BTreeMap<String, String> = BTreeMap::new();
        for (_, column) in columns_map.iter() {
            for constraint in &column.constraints {
                if let Some(rendered) =
                    render_column_constraint(self.adapter_type(), constraint.clone())
                {
                    if let Some(s) = rendered_constraints.get_mut(&rendered) {
                        s.push_str(&format!(" {rendered}"));
                    } else {
                        rendered_constraints.insert(column.name.clone(), rendered);
                    }
                }
            }
        }
        let nested_columns =
            self.nest_column_data_types(columns_map, Some(rendered_constraints))?;
        let result = nested_columns
            .into_values()
            .map(|column| format!("{} {}", column.name, column.data_type.unwrap_or_default()))
            .collect();
        Ok(result)
    }

    /// Example:
    /// columns: {
    ///     "a": {"name": "a", "data_type": "string", "description": ...},
    ///     "b.nested": {"name": "b.nested", "data_type": "string"},
    ///     "b.nested2": {"name": "b.nested2", "data_type": "string"}
    ///     }
    /// returns: {
    ///     "a": {"name": "a", "data_type": "string"},
    ///     "b": {"name": "b": "data_type": "struct<nested string, nested2 string>}
    /// }
    ///
    /// arbitrarily nested struct/array types are allowed, for more details check out the tests/data/nest_column_data_types example
    /// reference: https://github.com/dbt-labs/dbt-core/blob/main/env/lib/python3.12/site-packages/dbt/adapters/bigquery/column.py#L131-L132
    /// The implementation is purely based on the pydoc and the limited observations of how dbt compile behehaves on the test example
    /// so there probably exist corner cases not handled properly
    /// TODO: support constraints
    fn nest_column_data_types(
        &self,
        columns: IndexMap<String, DbtColumn>,
        _constraints: Option<BTreeMap<String, String>>,
    ) -> AdapterResult<IndexMap<String, DbtColumn>> {
        let mut result = NestedColumnDataTypes::default();
        for (column_name, column) in &columns {
            result.insert(column_name, column.data_type.as_ref())
        }
        let column_to_data_type = result.format_top_level_columns_data_types();
        let mut result = IndexMap::new();
        for (column_name, data_type) in &column_to_data_type {
            match columns.get(column_name) {
                Some(column) => result.insert(
                    column_name.clone(),
                    DbtColumn {
                        name: column.name.clone(),
                        data_type: Some(data_type.clone()),
                        description: column.description.clone(),
                        constraints: column.constraints.clone(),
                        meta: column.meta.clone(),
                        tags: column.tags.clone(),
                        policy_tags: column.policy_tags.clone(),
                        quote: column.quote,
                        deprecated_config: column.deprecated_config.clone(),
                    },
                ),
                None => result.insert(
                    column_name.clone(),
                    DbtColumn {
                        name: column_name.to_owned(),
                        data_type: Some(data_type.to_owned()),
                        description: None,
                        constraints: vec![],
                        meta: BTreeMap::new(),
                        tags: vec![],
                        policy_tags: None,
                        quote: None,
                        deprecated_config: Default::default(),
                    },
                ),
            };
        }
        Ok(result)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/f4dfd350942cce11ff25e3d22f2bee9e60b12b6d/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L444
    fn get_column_schema_from_query(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        ctx: &QueryCtx,
        sql: &str,
    ) -> AdapterResult<Vec<StdColumn>> {
        let batch = self.engine().execute(Some(state), conn, ctx, sql)?;
        let schema = batch.schema();

        let type_ops = self.engine.type_ops();
        let builder = ColumnBuilder::new(self.adapter_type());

        let fields = schema.fields();

        let mut columns = Vec::<StdColumn>::with_capacity(fields.len());
        for field in fields {
            let column = builder.build(field, type_ops)?;
            columns.push(column);
        }

        let flattened_columns = columns.iter().flat_map(|column| column.flatten()).collect();
        Ok(flattened_columns)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L834
    fn grant_access_to(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        entity: Arc<dyn BaseRelation>,
        entity_type: &str,
        // _role is not used since this method only supports view
        // and googleapi doesn't require role if the entity is view, it'll be default to READ always
        _role: Option<&str>,
        database: &str,
        schema: &str,
    ) -> AdapterResult<Value> {
        /// reference: https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L85
        /// but instead of locking the thread, put the lock on the dataset
        static DATASET_LOCK: LazyLock<DashMap<String, bool>> = LazyLock::new(DashMap::new);

        // adapter.grant_access_to when seen in Jinja macros, `entity_type` is always set to view
        // https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L842
        // Besides, there is a deserialization bug in the existing py impl when entity_type is not `view`
        if entity_type != "view" {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Only views are supported for grant_access_to".to_string(),
            ));
        }

        #[derive(Serialize, Deserialize)]
        struct Dataset {
            project: String,
            dataset: String,
        }
        let mut payload = BTreeMap::new();
        payload.insert(
            format!(
                "{}.{}.{}",
                entity.database_as_str()?,
                entity.schema_as_str()?,
                entity.identifier_as_str()?
            ),
            vec![Dataset {
                project: database.to_string(),
                dataset: schema.to_string(),
            }],
        );

        let _lock = DATASET_LOCK
            .entry(format!("{database}.{schema}"))
            .or_insert_with(|| true);

        let ctx = query_ctx_from_state(state)?;
        let sql = "none"; // empty sql that won't really be executed
        let mut options = self.get_adbc_execute_options(state);
        options.push((
            UPDATE_DATASET_AUTHORIZE_VIEW_TO_DATASETS.to_string(),
            OptionValue::String(serde_json::to_string(&payload)?),
        ));
        self.engine
            .execute_with_options(Some(state), &ctx, conn, sql, options, false)?;
        Ok(none_value())
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L853-L854
    fn get_dataset_location(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        relation: Value,
    ) -> AdapterResult<Option<String>> {
        let relation = downcast_value_to_dyn_base_relation(&relation)?;

        // https://cloud.google.com/bigquery/docs/information-schema-datasets-schemata
        let sql = format!(
            "SELECT
                location
            FROM `{}.INFORMATION_SCHEMA.SCHEMATA` WHERE schema_name = '{}'",
            relation.database_as_str()?,
            relation.schema_as_str()?
        );

        let ctx = query_ctx_from_state(state)?.with_desc("get_dataset_location adapter call");
        let batch = self.engine.execute(Some(state), conn, &ctx, &sql)?;

        let location = get_column_values::<StringArray>(&batch, "location")?;
        debug_assert!(batch.num_rows() <= 1);
        if batch.num_rows() == 1 {
            let loc = location.value(0).to_owned();
            Ok(Some(loc))
        } else {
            Ok(None)
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L626-L627
    fn update_table_description(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
        description: &str,
    ) -> AdapterResult<Value> {
        // https://cloud.google.com/bigquery/docs/managing-tables#updating_a_tables_description
        let sql = format!(
            "ALTER TABLE {database}.{schema}.{identifier}
             SET OPTIONS (description = '{description}')"
        );

        let ctx = query_ctx_from_state(state)?.with_desc("update_table_description adapter call");
        self.engine.execute_with_options(
            Some(state),
            &ctx,
            conn,
            &sql,
            self.get_adbc_execute_options(state),
            false,
        )?;
        Ok(none_value())
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L260-L261
    /// This method is a noop for BigQuery
    fn expand_target_column_types(
        &self,
        _state: &State,
        _from_relation: Arc<dyn BaseRelation>,
        _to_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        Ok(none_value())
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L579-L586
    fn parse_partition_by(&self, raw_partition_by: Value) -> AdapterResult<Value> {
        if raw_partition_by.is_none() {
            return Ok(none_value());
        }

        let partition_by = minijinja_value_to_typed_struct::<PartitionConfig>(
            raw_partition_by.clone(),
        )
        .map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!("adapter.parse_partition_by failed on {raw_partition_by:?}: {e}"),
            )
        })?;

        let validated_config = partition_by.into_bigquery().ok_or_else(|| {
            MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                "Expect a BigqueryPartitionConfigStruct",
            )
        })?;

        Ok(Value::from_object(validated_config))
    }

    /// get_table_options
    fn get_table_options(
        &self,
        state: &State,
        config: ModelConfig,
        common_attr: &CommonAttributes,
        temporary: bool,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        // Get common options first
        let mut opts = self.get_common_table_options(state, config.clone(), common_attr, temporary);

        // Handle KMS key name if present
        if let Some(kms_key_name) = config.__warehouse_specific_config__.kms_key_name {
            opts.insert(
                "kms_key_name".to_string(),
                Value::from(format!("'{kms_key_name}'")),
            );
        }

        if temporary {
            // For temporary tables, set 12-hour expiration
            opts.insert(
                "expiration_timestamp".to_string(),
                Value::from("TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)"),
            );
        } else {
            // Handle partition filter requirements for non-temporary tables
            if config
                .__warehouse_specific_config__
                .require_partition_filter
                .unwrap_or(false)
                && config.__warehouse_specific_config__.partition_by.is_some()
            {
                opts.insert(
                    "require_partition_filter".to_string(),
                    Value::from(
                        config
                            .__warehouse_specific_config__
                            .require_partition_filter,
                    ),
                );
            }
        }

        // Handle partition expiration if specified
        if let Some(days) = config
            .__warehouse_specific_config__
            .partition_expiration_days
        {
            opts.insert("partition_expiration_days".to_string(), Value::from(days));
        }

        Ok(opts)
    }

    /// get_view_options
    fn get_view_options(
        &self,
        state: &State,
        config: ModelConfig,
        common_attr: &CommonAttributes,
    ) -> AdapterResult<BTreeMap<String, Value>> {
        let result = self.get_common_table_options(state, config, common_attr, false);
        Ok(result)
    }

    /// Add time ingestion partition column to columns list
    fn add_time_ingestion_partition_column(
        &self,
        columns: Value,
        partition_config: BigqueryPartitionConfig,
    ) -> AdapterResult<Value> {
        let mut result = StdColumn::vec_from_jinja_value(AdapterType::Bigquery, columns.clone())?;

        if result
            .iter()
            .any(|c| c.name() == BigqueryPartitionConfig::PARTITION_TIME)
        {
            return Ok(columns);
        }

        result.push(StdColumn::new_bigquery(
            partition_config
                .insertable_time_partitioning_field()?
                .as_str()
                .expect("must be a str")
                .to_owned(),
            partition_config.data_type,
            &[],
            // TODO(serramatutu): proper mode
            BigqueryColumnMode::Nullable,
        ));

        Ok(Value::from(result))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L415
    fn copy_table(
        &self,
        state: &State,
        conn: &'_ mut dyn Connection,
        source: Arc<dyn BaseRelation>,
        dest: Arc<dyn BaseRelation>,
        materialization: String,
    ) -> AdapterResult<()> {
        let append = materialization == "incremental";
        let truncate = materialization == "table";
        if !append && !truncate {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "copy_table 'materialization' must be either 'table' or 'incremental'".to_string(),
            ));
        }

        let source_fqn = source.render_self_as_str();

        let dest_schema = dest.schema_as_str()?;
        let dest_identifier = dest.identifier_as_str()?;
        let dest_fqn = dest.render_self_as_str();

        // CREATE TABLE CLONE errors when the dest relation already exists
        // CREATE TABLE CLONE is the same as `bq cp` but with writeDisposition set to "WRITE_EMPTY",
        // more details see https://cloud.google.com/bigquery/docs/table-clones-create#api
        //
        // but the existing bigquery-adapter uses other options WRITE_TRUNCATE and WRITE_APPEND depending on the materialization value
        // it uses https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client.html#google_cloud_bigquery_client_Client_copy_table
        // more details see
        // a copy_table parameter - https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.CopyJobConfig
        // TODO: revisit the quoting used here after https://github.com/dbt-labs/fs/pull/2724 is merged
        let sql = if truncate {
            // truncate
            format!(
                "IF EXISTS (SELECT 1 FROM {dest_schema}.INFORMATION_SCHEMA.TABLES WHERE table_name = '{dest_identifier}') THEN
                    DELETE FROM {dest_fqn} WHERE 1=1;
                    INSERT INTO {dest_fqn}
                    SELECT * FROM {source_fqn};
                ELSE
                    CREATE TABLE {dest_fqn} CLONE {source_fqn};
                END IF;",
            )
        } else {
            // append
            format!(
                "IF EXISTS (SELECT 1 FROM {dest_schema}.INFORMATION_SCHEMA.TABLES WHERE table_name = '{dest_identifier}') THEN
                    INSERT INTO {dest_fqn}
                    SELECT * FROM {source_fqn};
                ELSE
                    CREATE TABLE {dest_fqn} CLONE {source_fqn};
                END IF;"
            )
        };

        let ctx = query_ctx_from_state(state)?.with_desc("copy_table adapter call");
        self.engine.execute_with_options(
            Some(state),
            &ctx,
            conn,
            &sql,
            self.get_adbc_execute_options(state),
            false,
        )?;

        Ok(())
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L818
    fn describe_relation(
        &self,
        conn: &'_ mut dyn Connection,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Option<Value>> {
        let adbc_schema = get_table_schema(conn, relation.clone())?;
        if let Some(relation_type) = relation.relation_type() {
            if relation_type == RelationType::MaterializedView {
                return Ok(Some(Value::from_object(
                    BigqueryMaterializedViewConfigObject::new(
                        <dyn BigqueryMaterializedViewConfig>::try_from_schema(
                            &adbc_schema,
                            self.engine().type_ops(),
                        )
                        .map_err(|err| {
                            AdapterError::new(AdapterErrorKind::UnexpectedResult, err)
                        })?,
                    ),
                )));
            } else {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!(
                        "The method `BigQueryAdapter.describe_relation` is not implemented for this relation type: {relation_type}"
                    ),
                ));
            }
        }

        Ok(None)
    }

    /// Check if a given partition and clustering column spec for a table
    /// can replace an existing relation in the database. BigQuery does not
    /// allow tables to be replaced with another table that has a different
    /// partitioning spec. This method returns True if the given config spec is
    /// identical to that of the existing table.
    ///
    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L541
    fn is_replaceable(
        &self,
        conn: &'_ mut dyn Connection,
        relation: Arc<dyn BaseRelation>,
        local_partition_by: Option<BigqueryPartitionConfig>,
        local_cluster_by: Option<BigqueryClusterConfig>,
    ) -> AdapterResult<bool> {
        match get_table_schema(conn, relation.clone()) {
            Ok(schema) => {
                let is_partition_match = partitions_match(
                    BigqueryPartitionConfig::try_from_schema(&schema, self.engine().type_ops())
                        .map_err(|err| {
                            AdapterError::new(AdapterErrorKind::UnexpectedResult, err)
                        })?,
                    local_partition_by,
                );

                let local_cluster_by = local_cluster_by
                    .map(|c| c.into_fields())
                    .unwrap_or_default();
                let remote_cluster_by = cluster_by_from_schema(&schema)
                    .map_err(|err| AdapterError::new(AdapterErrorKind::UnexpectedResult, err))?;
                let is_cluster_match = local_cluster_by == remote_cluster_by;

                Ok(is_partition_match && is_cluster_match)
            }
            Err(e) => {
                if e.kind() == AdapterErrorKind::NotFound {
                    Ok(true)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/4a00354a497214d9043bf4122810fe2d04de17bb/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L132
    fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        match ct {
            ConstraintType::Check | ConstraintType::Unique => ConstraintSupport::NotSupported,
            ConstraintType::NotNull => ConstraintSupport::Enforced,
            ConstraintType::PrimaryKey | ConstraintType::ForeignKey => {
                ConstraintSupport::NotEnforced
            }
            _ => ConstraintSupport::NotSupported,
        }
    }
}

fn get_table_schema(
    conn: &'_ mut dyn Connection,
    relation: Arc<dyn BaseRelation>,
) -> AdapterResult<Schema> {
    conn.get_table_schema(
        Some(&relation.database_as_str()?),
        Some(&relation.schema_as_str()?),
        &relation.identifier_as_str()?,
    )
    .map_err(adbc_error_to_adapter_error)
}

/// Represent nested data types (struct/array) for BigQuery
/// Leaf nodes are primitive types
/// For example column names "a.b", "a.c", "a.c.d" will be
///  a (struct)
///  /\
/// b  c (struct)
///     \
///      d
#[derive(Debug, Default)]
struct NestedColumnDataTypes {
    root: TrieNode,
}

#[derive(Debug, Default)]
struct TrieNode {
    pub children: IndexMap<String, TrieNode>,
    pub data_type: Option<String>,
}

impl NestedColumnDataTypes {
    fn insert(&mut self, column_name: &str, column_type: Option<&String>) {
        let names = column_name.split(".");
        let mut node = &mut self.root;
        for name in names {
            node = node.children.entry(name.to_owned()).or_default();
        }
        node.data_type = column_type.map(String::from);
    }

    fn format_top_level_columns_data_types(&self) -> IndexMap<String, String> {
        let mut result = IndexMap::new();
        for (column_name, node) in &self.root.children {
            let data_type = match &node.data_type {
                None => {
                    let inner_data_type = node.format_data_type();
                    format!("struct<{inner_data_type}>")
                }
                Some(data_type) => match data_type.as_str() {
                    "struct" => {
                        let inner_data_type = node.format_data_type();
                        format!("struct<{inner_data_type}>")
                    }
                    "array" => {
                        let inner_data_type = node.format_data_type();
                        format!("array<struct<{inner_data_type}>>")
                    }
                    // assume any struct or array type is a primitive type
                    _ => {
                        // ensure no sub fields
                        if node.children.is_empty() {
                            data_type.to_owned()
                        }
                        // sub fields exist -> it's actually not a primitive type -> default to struct
                        // this is to be consistent with dbt compile behavior
                        else {
                            let inner_data_type = node.format_data_type();
                            format!("struct<{inner_data_type}>")
                        }
                    }
                },
            };
            result.insert(column_name.to_owned(), data_type);
        }
        result
    }
}

impl TrieNode {
    // TODO: refactor since this method is very much overlapped with `format_top_level_columns_data_types`
    fn format_data_type(&self) -> String {
        let mut result = vec![];
        for (column_name, node) in &self.children {
            let data_type = match &node.data_type {
                None => {
                    let inner_data_type = node.format_data_type();
                    if inner_data_type.is_empty() {
                        column_name.to_owned()
                    } else {
                        format!("{column_name} struct<{inner_data_type}>")
                    }
                }
                Some(data_type) => match data_type.as_str() {
                    "struct" => {
                        let inner_data_type = node.format_data_type();
                        format!("{column_name} struct<{inner_data_type}>")
                    }
                    "array" => {
                        let inner_data_type = node.format_data_type();
                        format!("{column_name} array<struct<{inner_data_type}>>")
                    }
                    _ => {
                        if node.children.is_empty() {
                            format!("{column_name} {data_type}")
                        } else {
                            let inner_data_type = node.format_data_type();
                            format!("{column_name} struct<{inner_data_type}>")
                        }
                    }
                },
            };
            result.push(data_type);
        }
        result.join(", ")
    }
}

#[cfg(test)] // win ADBC
mod tests {
    use super::*;
    use crate::stmt_splitter::NaiveStmtSplitter;

    use crate::config::AdapterConfig;
    use crate::query_comment::QueryCommentConfig;
    use crate::sql_types::NaiveTypeOpsImpl;
    use dbt_auth::auth_for_backend;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;
    use dbt_serde_yaml::Mapping;
    use dbt_xdbc::Backend;

    fn engine() -> Arc<SqlEngine> {
        let config = Mapping::default();
        let auth = auth_for_backend(Backend::BigQuery);
        SqlEngine::new(
            AdapterType::Bigquery,
            auth.into(),
            AdapterConfig::new(config),
            DEFAULT_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter), // XXX: may cause bugs if these tests run SQL
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Bigquery, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Postgres)),
            never_cancels(),
        )
    }

    #[test]
    fn test_quote() {
        let env = minijinja::Environment::new();
        let state = State::new_for_env(&env);
        let adapter = BigqueryAdapter::new(engine());
        assert_eq!(adapter.quote(&state, "abc").unwrap(), "`abc`");
    }

    #[test]
    fn test_format_top_level_columns_data_types() {
        // Test case 1: Simple primitive types
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("id", Some(&"integer".to_string()));
            nested.insert("name", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("id").unwrap(), "integer");
            assert_eq!(result.get("name").unwrap(), "string");
        }

        // Test case 2: Nested struct
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("user.id", Some(&"integer".to_string()));
            nested.insert("user.name", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("user").unwrap(),
                "struct<id integer, name string>"
            );
        }

        // Test case 3: Array of structs
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("addresses", Some(&"array".to_string()));
            nested.insert("addresses.street", Some(&"string".to_string()));
            nested.insert("addresses.city", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("addresses").unwrap(),
                "array<struct<street string, city string>>"
            );
        }

        // Test case 4: Mixed types with deep nesting
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("id", Some(&"integer".to_string()));
            nested.insert("user.name", Some(&"string".to_string()));
            nested.insert("user.contact.email", Some(&"string".to_string()));
            nested.insert("user.contact.phone", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("id").unwrap(), "integer");
            assert_eq!(
                result.get("user").unwrap(),
                "struct<name string, contact struct<email string, phone string>>"
            );
        }

        // Test case 5: Empty struct (no data type)
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("empty_struct", None);
            nested.insert("empty_struct.field1", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("empty_struct").unwrap(), "struct<field1 string>");
        }

        // Test case 6: Struct marked as primitive but has children
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("metadata", Some(&"json".to_string()));
            nested.insert("metadata.key1", Some(&"string".to_string()));
            nested.insert("metadata.key2", Some(&"integer".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("metadata").unwrap(),
                "struct<key1 string, key2 integer>"
            );
        }
    }
}
