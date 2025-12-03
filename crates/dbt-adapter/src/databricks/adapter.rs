use crate::databricks::describe_table::DatabricksTableMetadata;
use crate::databricks::version::DbrVersion;

use crate::databricks::relation_configs::base::{
    DatabricksRelationConfigBase, DatabricksRelationConfigBaseObject,
};
use crate::databricks::relation_configs::incremental::IncrementalTableConfig;
use crate::databricks::relation_configs::materialized_view::MaterializedViewConfig;
use crate::databricks::relation_configs::relation_api::get_from_relation_config;
use crate::databricks::relation_configs::streaming_table::StreamingTableConfig;
use crate::databricks::relation_configs::view::ViewConfig;

use crate::adapter_engine::AdapterEngine;
use crate::catalog_relation::CatalogRelation;
use crate::column::Column;
use crate::databricks::relation::DatabricksRelation;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::funcs::execute_macro_wrapper_with_package;
use crate::load_catalogs;
use crate::metadata::*;
use crate::query_ctx::query_ctx_from_state;
use crate::record_batch_utils::get_column_values;
use crate::relation_object::RelationObject;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterType, AdapterTyping};
use arrow::array::{Array, StringArray};
use dbt_agate::AgateTable;
use dbt_common::behavior_flags::BehaviorFlag;

use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::{ConstraintSupport, ConstraintType, DbtMaterialization};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::schemas::{BaseRelationConfig, InternalDbtNodeAttributes, InternalDbtNodeWrapper};
use dbt_xdbc::{Connection, QueryCtx};
use indexmap::IndexMap;
use minijinja::{State, Value};
use once_cell::sync::Lazy;
use regex::Regex;
use uuid::Uuid;

use std::collections::BTreeMap;
use std::fmt;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::OnceLock;

static CREDENTIAL_IN_COPY_INTO_REGEX: Lazy<Regex> = Lazy::new(|| {
    // This is NOT the same as the Python regex used in dbt-databricks. Rust lacks lookaround.
    // This achieves the same result for the proper structure.  See original at time of port:
    // https://github.com/databricks/dbt-databricks/blob/66f513b960c62ee21c4c399264a41a56853f3d82/dbt/adapters/databricks/utils.py#L19
    Regex::new(r"credential\s*(\(\s*'[\w\-]+'\s*=\s*'.*?'\s*(?:,\s*'[\w\-]+'\s*=\s*'.*?'\s*)*\))")
        .expect("CREDENTIALS_IN_COPY_INTO_REGEX invalid")
});

/// An adapter for interacting with Databricks.
#[derive(Clone)]
pub struct DatabricksAdapter {
    engine: Arc<AdapterEngine>,
}

impl fmt::Debug for DatabricksAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl DatabricksAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }

    /// Get the Databricks Runtime version, caching the result for subsequent calls.
    ///
    /// To bypass the cache, use [`get_dbr_version()`](Self::get_dbr_version) directly.
    pub(crate) fn dbr_version(&self) -> AdapterResult<DbrVersion> {
        static CACHED_DBR_VERSION: OnceLock<AdapterResult<DbrVersion>> = OnceLock::new();

        CACHED_DBR_VERSION
            .get_or_init(|| {
                let query_ctx = QueryCtx::default().with_desc("get_dbr_version adapter call");
                let mut conn = self.engine.new_connection(None, None)?;
                self.get_dbr_version(&query_ctx, conn.deref_mut())
            })
            .clone()
    }

    /// Get the Databricks Runtime version without caching.
    fn get_dbr_version(
        &self,
        ctx: &QueryCtx,
        conn: &mut dyn Connection,
    ) -> AdapterResult<DbrVersion> {
        // https://docs.databricks.com/aws/en/sql/language-manual/functions/current_version
        // dbr_version is a null string if this query runs in non-cluster mode

        // It appears that this is a divergence from the dbt-databricks implementation,
        // which uses `SET spark.databricks.clusterUsageTags.sparkVersion` to read out the version instead.
        // They only do this if `is_cluster` is True, otherwise it would error.
        let batch = self.engine.execute(
            None,
            conn,
            ctx,
            "select current_version().dbr_version as dbr_version",
        )?;

        let dbr_version = get_column_values::<StringArray>(&batch, "dbr_version")?;
        debug_assert_eq!(dbr_version.len(), 1);

        // if dbr_version is null, then we are not on a cluster and we can assume the version is greater than the requested version
        // dbt is applying a similar logic here: https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/handle.py#L143-L144
        //
        // https://docs.databricks.com/aws/en/sql/language-manual/functions/version#examples
        // in format "[dbr_version] [git_hash]"
        //
        // TODO(cwalden): it looks like this might be wrong?
        //  `current_version().dbr_version` doesn't contain the git hash, so I don't think need this first split.
        dbr_version.value(0).parse::<DbrVersion>()
    }
}

impl AdapterTyping for DatabricksAdapter {
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        Some(self)
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }
}

impl TypedBaseAdapter for DatabricksAdapter {
    /// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L87
    fn behavior(&self) -> Vec<BehaviorFlag> {
        let use_info_schema_for_columns = BehaviorFlag::new(
            "use_info_schema_for_columns",
            false,
            Some(
                "Use info schema to gather column information to ensure complex types are not truncated. Incurs some overhead, so disabled by default.",
            ),
            None,
            None,
        );

        let use_user_folder_for_python = BehaviorFlag::new(
            "use_user_folder_for_python",
            false,
            Some(
                "Use the user's home folder for uploading python notebooks. Shared folder use is deprecated due to governance concerns.",
            ),
            None,
            None,
        );

        let use_materialization_v2 = BehaviorFlag::new(
            "use_materialization_v2",
            false,
            Some(
                "Use revamped materializations based on separating create and insert. This allows more performant column comments, as well as new column features.",
            ),
            None,
            None,
        );

        vec![
            use_info_schema_for_columns,
            use_user_folder_for_python,
            use_materialization_v2,
        ]
    }

    // TODO: add_query does not appear to be necessary (few uses in
    // macros) and should be removed and replaced with `execute`.
    #[allow(clippy::too_many_arguments)]
    fn add_query(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
        _bindings: Option<&Value>,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        let _ = self.execute_inner(
            self.adapter_type().into(),
            self.engine.clone(),
            None,
            conn,
            ctx,
            sql,
            auto_begin,
            false, // default for fetch as in dispatch_adapter_calls()
            None,
            None,
        )?;
        Ok(())
    }

    /// Get the full macro name for check_schema_exists
    ///
    /// # Returns
    ///
    /// Returns (package_name, macro_name)
    fn check_schema_exists_macro(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> AdapterResult<(String, String)> {
        Ok((
            "dbt_spark".to_string(),
            "spark__check_schema_exists".to_string(),
        ))
    }

    fn get_relation(
        &self,
        state: &State,
        ctx: &QueryCtx,
        conn: &mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        // though _needs_information is used in dbt to decide if this may be related from relations cache
        // since we don't implement relations cache, it's ignored for now
        // see https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L418

        let query_catalog = if self.quoting().database {
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
            self.quote(state, identifier)?
        } else {
            identifier.to_string()
        };

        // this deviates from what the existing dbt-adapter impl - here `AS JSON` is used
        // and a critical reason is `DESCRIBE TABLE EXTENDED` returns different results
        // for example, when database is set to the default `hive_metastore` it misses 'Provider' and 'Type' rows
        // and we don't need to parse through rows, see reference https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L433
        //
        // But this will lead to differences the metadata field
        // more details see docs from DatabricksDescribeTableExtended::to_metadata
        // WARNING: system tables from DBX only supports the basic "DESCRIBE TABLE"
        // though I assume it's highly unlikely and unfathomable a user would want to call adapter.get_relation on a system table
        let sql = format!(
            "DESCRIBE TABLE EXTENDED {query_catalog}.{query_schema}.{query_identifier} AS JSON"
        );

        let batch = self.engine.execute(Some(state), conn, ctx, &sql);
        if let Err(e) = &batch
            && (e.to_string().contains("cannot be found")
                || e.to_string().contains("TABLE_OR_VIEW_NOT_FOUND"))
        {
            return Ok(None);
        }
        let batch = batch?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        debug_assert_eq!(batch.num_rows(), 1);

        let json_metadata = DatabricksTableMetadata::from_record_batch(Arc::new(batch))?;
        let is_delta = json_metadata.provider.as_deref() == Some("delta");
        let relation_type = match json_metadata.type_.as_str() {
            "VIEW" => Some(RelationType::View),
            "MATERIALIZED_VIEW" => Some(RelationType::MaterializedView),
            "STREAMING_TABLE" => Some(RelationType::StreamingTable),
            _ => Some(RelationType::Table),
        };

        Ok(Some(Arc::new(DatabricksRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            relation_type,
            None,
            self.quoting(),
            Some(json_metadata.into_metadata()),
            is_delta,
        ))))
    }

    /// Databricks inherits the implementation from the Spark adapter.
    ///
    /// Spark implementation also implicitly filters out known HUDI metadata columns,
    /// which we currently do not.
    ///
    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-spark/src/dbt/adapters/spark/impl.py#L317-L336
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Column>> {
        let result = match execute_macro_wrapper_with_package(
            state,
            &[RelationObject::new(relation).as_value()],
            "get_columns_comments",
            "dbt_databricks",
        ) {
            Ok(result) => result,
            Err(err) => {
                // TODO: switch to checking the vendor error code when available.
                // See https://github.com/dbt-labs/fs/pull/4267#discussion_r2182835729
                // Only checks for the observed Databricks error message, avoiding
                // all messages in the reference python Spark adapter.
                if err.message().contains("[TABLE_OR_VIEW_NOT_FOUND]") {
                    return Ok(Vec::new());
                }
                return Err(err);
            }
        };

        let name_string_array = get_column_values::<StringArray>(&result, "col_name")?;
        let dtype_string_array = get_column_values::<StringArray>(&result, "data_type")?;

        let columns = (0..name_string_array.len())
            .map(|i| {
                Column::new(
                    AdapterType::Databricks,
                    name_string_array.value(i).to_string(),
                    dtype_string_array.value(i).to_string(),
                    None, // char_size
                    None, // numeric_precision
                    None, // numeric_scale
                )
            })
            .collect::<Vec<_>>();
        Ok(columns)
    }

    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/connections.py#L226-L227
    fn compare_dbr_version(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        major: i64,
        minor: i64,
    ) -> AdapterResult<Value> {
        let query_ctx = query_ctx_from_state(state)?.with_desc("compare_dbr_version adapter call");

        let current_version = self.get_dbr_version(&query_ctx, conn)?;
        let expected_version = DbrVersion::Full(major, minor);

        let result = match current_version.cmp(&expected_version) {
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Less => -1,
        };

        Ok(Value::from(result))
    }

    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L208-L209
    fn compute_external_path(
        &self,
        config: ModelConfig,
        node: &dyn InternalDbtNodeAttributes,
        is_incremental: bool,
    ) -> AdapterResult<String> {
        // TODO: dbt seems to allow optional database and schema
        // https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L212-L213
        let location_root = config
            .__warehouse_specific_config__
            .location_root
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "location_root is required for external tables.",
                )
            })?;

        let include_full_name_in_path = config
            .__warehouse_specific_config__
            .include_full_name_in_path
            .unwrap_or_default();

        // Build path using the same logic as posixpath.join
        let path = if include_full_name_in_path {
            format!(
                "{}/{}/{}/{}",
                location_root.trim_end_matches('/'),
                node.database().trim_end_matches('/'),
                node.schema().trim_end_matches('/'),
                node.name()
            )
        } else {
            format!(
                "{}/{}/{}",
                location_root.trim_end_matches('/'),
                node.database().trim_end_matches('/'),
                node.name()
            )
        };

        let path = if is_incremental {
            format!("{path}_tmp")
        } else {
            path
        };
        Ok(path)
    }

    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L187-L188
    ///
    /// squashes featureset of DatabricksAdapter iceberg_table_properties
    /// https://github.com/databricks/dbt-databricks/blob/53cd1a2c1fcb245ef25ecf2e41249335fd4c8e4b/dbt/adapters/databricks/impl.py#L229C9-L229C41
    fn update_tblproperties_for_uniform_iceberg(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        tblproperties: &mut BTreeMap<String, Value>,
    ) -> AdapterResult<()> {
        // TODO(anna): Ideally from_model_config_and_catalogs would just take in an InternalDbtNodeWrapper instead of a Value. This is blocked by a Snowflake hack in `snowflake__drop_table`.
        let node_yml = node.as_internal_node().serialize();
        let catalog_relation = CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type(),
            &Value::from_object(dbt_common::serde_utils::convert_yml_to_value_map(node_yml)),
            load_catalogs::fetch_catalogs(),
        )?;
        // We only have to update tblproperties if using a UniForm Iceberg table
        if catalog_relation.table_format == "iceberg" {
            if self
                .compare_dbr_version(state, conn, 14, 3)?
                .as_i64()
                .expect("dbr_version is a number")
                < 0
            {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "Iceberg support requires Databricks Runtime 14.3 or later.",
                ));
            }

            if catalog_relation.file_format != Some("delta".to_string()) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "When table_format is 'iceberg', file_format must be 'delta'.",
                ));
            }

            let materialized = config.materialized.ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "materialized is required for iceberg tables.",
                )
            })?;

            // TODO(versusfacit): support snapshot
            if materialized != DbtMaterialization::Incremental
                && materialized != DbtMaterialization::Table
                && materialized != DbtMaterialization::Seed
            {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "When table_format is 'iceberg', materialized must be 'incremental', 'table', or 'seed'.",
                ));
            }

            tblproperties
                .entry("delta.enableIcebergCompatV2".to_string())
                .or_insert_with(|| Value::from(true));

            tblproperties
                .entry("delta.universalFormat.enabledFormats".to_string())
                .or_insert_with(|| Value::from("iceberg"));
        }
        Ok(())
    }

    /// https://github.com/databricks/dbt-databricks/blob/8cda62ee19d01e0670e3156e652841e3ffd3ed41/dbt/adapters/databricks/impl.py#L253
    fn is_uniform(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
    ) -> AdapterResult<bool> {
        // TODO(anna): Ideally from_model_config_and_catalogs would just take in an InternalDbtNodeWrapper instead of a Value. This is blocked by a Snowflake hack in `snowflake__drop_table`.
        let node_yml = node.as_internal_node().serialize();
        let catalog_relation = CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type(),
            &Value::from_object(dbt_common::serde_utils::convert_yml_to_value_map(node_yml)),
            load_catalogs::fetch_catalogs(),
        )?;

        if catalog_relation.table_format != "iceberg" {
            return Ok(false);
        }

        if self
            .compare_dbr_version(state, conn, 14, 3)?
            .as_i64()
            .expect("dbr_version is a number")
            < 0
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Iceberg support requires Databricks Runtime 14.3 or later.",
            ));
        }

        let materialized = config.materialized.ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "materialized is required for iceberg tables.",
            )
        })?;

        // TODO(versusfacit): support snapshot
        if materialized != DbtMaterialization::Incremental
            && materialized != DbtMaterialization::Table
            && materialized != DbtMaterialization::Seed
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "When table_format is 'iceberg', materialized must be 'incremental', 'table', or 'seed'.",
            ));
        }

        let use_uniform = if let Some(val) = catalog_relation.adapter_properties.get("use_uniform")
        {
            val.eq_ignore_ascii_case("true")
        } else {
            false
        };

        if use_uniform && catalog_relation.catalog_type != "unity" {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Managed Iceberg tables are only supported in Unity Catalog. Set 'use_uniform' adapter property to true for Hive Metastore.",
            ));
        }

        Ok(use_uniform)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L445
    fn generate_unique_temporary_table_suffix(
        &self,
        suffix_initial: Option<String>,
    ) -> AdapterResult<String> {
        let suffix_initial = suffix_initial.as_deref().unwrap_or("__dbt_tmp");
        let uuid_str = Uuid::new_v4().to_string().replace('-', "_");
        Ok(format!("{suffix_initial}_{uuid_str}"))
    }

    /// Given a relation, fetch its configurations from the remote data warehouse
    /// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L797
    fn get_relation_config(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Arc<dyn BaseRelationConfig>> {
        let relation_type = relation.relation_type().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "relation_type is required for the input relation of adapter.get_relation_config",
            )
        })?;
        let result: Arc<dyn BaseRelationConfig> = match relation_type {
            RelationType::Table => Arc::new(self.get_from_relation::<IncrementalTableConfig>(
                state,
                conn,
                relation.clone(),
            )?) as Arc<dyn BaseRelationConfig>,
            RelationType::View => {
                Arc::new(self.get_from_relation::<ViewConfig>(state, conn, relation.clone())?)
                    as Arc<dyn BaseRelationConfig>
            }
            RelationType::MaterializedView => Arc::new(
                self.get_from_relation::<MaterializedViewConfig>(state, conn, relation.clone())?,
            ) as Arc<dyn BaseRelationConfig>,
            RelationType::StreamingTable => Arc::new(
                self.get_from_relation::<StreamingTableConfig>(state, conn, relation.clone())?,
            ) as Arc<dyn BaseRelationConfig>,
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!("Unsupported relation type: {relation_type:?}"),
                ));
            }
        };
        Ok(result)
    }

    fn build_catalog_relation(&self, model: &Value) -> AdapterResult<Value> {
        Ok(Value::from_object(
            CatalogRelation::from_model_config_and_catalogs(
                &self.adapter_type(),
                model,
                load_catalogs::fetch_catalogs(),
            )?,
        ))
    }

    /// Given a model, parse and build its configurations
    /// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L810
    fn get_config_from_model(&self, model: &dyn InternalDbtNodeAttributes) -> AdapterResult<Value> {
        let result: Arc<dyn DatabricksRelationConfigBase> = match model.materialized() {
            DbtMaterialization::Incremental => {
                Arc::new(get_from_relation_config::<IncrementalTableConfig>(model)?)
                    as Arc<dyn DatabricksRelationConfigBase>
            }
            DbtMaterialization::MaterializedView => {
                Arc::new(get_from_relation_config::<MaterializedViewConfig>(model)?)
                    as Arc<dyn DatabricksRelationConfigBase>
            }
            DbtMaterialization::StreamingTable => {
                Arc::new(get_from_relation_config::<StreamingTableConfig>(model)?)
                    as Arc<dyn DatabricksRelationConfigBase>
            }
            DbtMaterialization::View => Arc::new(get_from_relation_config::<ViewConfig>(model)?)
                as Arc<dyn DatabricksRelationConfigBase>,
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!(
                        "Unsupported materialization type: {:?}",
                        model.materialized()
                    ),
                ));
            }
        };
        let result = DatabricksRelationConfigBaseObject::new(result);
        Ok(Value::from_object(result))
    }

    fn get_column_tags_from_model(
        &self,
        model: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Value> {
        use crate::databricks::relation_configs::base::DatabricksComponentProcessor;
        use crate::databricks::relation_configs::column_tags::{
            ColumnTagsConfig, ColumnTagsProcessor,
        };
        use std::collections::BTreeMap;

        let processor = ColumnTagsProcessor;
        if let Some(
            crate::databricks::relation_configs::base::DatabricksComponentConfig::ColumnTags(
                column_tags,
            ),
        ) = processor.from_relation_config(model)?
        {
            let value = Value::from_serialize(&column_tags);
            return Ok(value);
        }

        Ok(Value::from_serialize(
            ColumnTagsConfig::new(BTreeMap::new()),
        ))
    }

    /// Given existing columns and columns from our model
    /// we determine which columns to update and persist docs for
    fn get_persist_doc_columns(
        &self,
        existing_columns: Vec<Column>,
        model_columns: IndexMap<String, dbt_schemas::schemas::dbt_column::DbtColumnRef>,
    ) -> AdapterResult<IndexMap<String, dbt_schemas::schemas::dbt_column::DbtColumnRef>> {
        // TODO(jasonlin45): grab comment info as well - we should avoid persisting for comments that are the same for performance reasons
        let mut result = IndexMap::new();
        // Intersection of existing columns and model columns that have descriptions
        for existing_col in existing_columns {
            if let Some(model_col) = model_columns.get(existing_col.name())
                && model_col.description.is_some()
            {
                result.insert(existing_col.name().to_string(), model_col.clone());
            }
        }
        Ok(result)
    }

    /// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/constraints.py#L17
    fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        match ct {
            ConstraintType::Check | ConstraintType::NotNull => ConstraintSupport::Enforced,
            ConstraintType::Unique => ConstraintSupport::NotSupported,
            ConstraintType::PrimaryKey | ConstraintType::ForeignKey => {
                ConstraintSupport::NotEnforced
            }
            _ => ConstraintSupport::NotSupported,
        }
    }

    /// https://github.com/databricks/dbt-databricks/blob/4d82bd225df81296165b540d34ad5be43b45e44a/dbt/adapters/databricks/impl.py#L831
    /// TODO: implement if necessary, currently its noop
    fn clean_sql(&self, sql: &str) -> AdapterResult<String> {
        Ok(sql.to_string())
    }

    /// https://github.com/databricks/dbt-databricks/blob/66f513b960c62ee21c4c399264a41a56853f3d82/dbt/adapters/databricks/impl.py#L717
    fn redact_credentials(&self, sql: &str) -> AdapterResult<String> {
        let Some(caps) = CREDENTIAL_IN_COPY_INTO_REGEX.captures(sql) else {
            // WARN: Malformed input by user means credentials may leak.
            // However, this _is_ the fallback strategy implemented in Python.
            return Ok(sql.to_string());
        };

        // Capture the full matched credential(...) string, including the surrounding parentheses.
        // Then extract only the inner key-value content
        let full_parens = caps.get(1).unwrap().as_str();
        let inner = &full_parens[1..full_parens.len() - 1];

        let redacted_pairs = inner
            .split(',')
            .map(|pair| {
                let key = pair.split('=').next().unwrap_or("").trim();
                format!("{key} = '[REDACTED]'")
            })
            .collect::<Vec<_>>()
            .join(", ");

        let redacted_sql = sql.replacen(full_parens, &format!("({redacted_pairs})"), 1);

        Ok(redacted_sql)
    }
    /// https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-spark/src/dbt/adapters/spark/impl.py#L500
    fn standardize_grants_dict(
        &self,
        grants_table: Arc<AgateTable>,
    ) -> AdapterResult<BTreeMap<String, Vec<String>>> {
        let record_batch = grants_table.original_record_batch();

        let grantee_cols = get_column_values::<StringArray>(&record_batch, "Principal")?;
        let privilege_cols = get_column_values::<StringArray>(&record_batch, "ActionType")?;
        let object_type_cols = get_column_values::<StringArray>(&record_batch, "ObjectType")?;

        let mut result = BTreeMap::new();
        for i in 0..record_batch.num_rows() {
            let privilege = privilege_cols.value(i);
            let grantee = grantee_cols.value(i);
            let object_type = object_type_cols.value(i);

            if object_type == "TABLE" && privilege != "OWN" {
                let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                list.push(grantee.to_string());
            }
        }

        Ok(result)
    }
}
