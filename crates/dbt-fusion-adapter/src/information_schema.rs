use crate::relation_object::RelationObject;

use dbt_schemas::{
    dbt_types::RelationType,
    schemas::relations::base::{BaseRelation, BaseRelationProperties, Policy},
};
use minijinja::{Error as MinijinjaError, State, Value};

use std::{any::Any, sync::Arc};

#[derive(Clone, Debug, Default)]
pub struct InformationSchema {
    pub database: Option<String>,
    pub schema: String,
    pub identifier: Option<String>,
    // quote_policy
}

impl InformationSchema {
    pub fn try_from_relation(
        relation_database: Option<String>,
        information_schema_view: &str,
    ) -> Result<Self, MinijinjaError> {
        // Create the InformationSchema object with the database name as none if it is an empty string
        Ok(Self {
            database: if relation_database.is_some() && relation_database.clone().unwrap() == "" {
                None
            } else {
                relation_database
            },
            schema: "INFORMATION_SCHEMA".to_string(),
            identifier: Some(information_schema_view.to_string()),
        })
    }
}

impl BaseRelationProperties for InformationSchema {
    fn include_policy(&self) -> Policy {
        unimplemented!("InformationSchema");
    }

    fn quote_policy(&self) -> Policy {
        unimplemented!("InformationSchema");
    }

    fn quote_character(&self) -> char {
        unimplemented!("InformationSchema");
    }
}

impl BaseRelation for InformationSchema {
    fn as_any(&self) -> &dyn Any {
        unimplemented!()
    }

    fn create_from(&self, _state: &State, _args: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!()
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
        unimplemented!()
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn include_inner(&self, _args: Policy) -> Result<Value, MinijinjaError> {
        unimplemented!("InformationSchema")
    }

    fn render_self(&self) -> Result<Value, MinijinjaError> {
        let result = match (&self.database, &self.identifier) {
            (Some(database), Some(identifier)) => {
                format!("{}.{}.{}", database, &self.schema, identifier)
            }
            (Some(database), None) => format!("{}.{}", database, &self.schema),
            (None, Some(identifier)) => format!("{}.{}", &self.schema, identifier),
            (None, None) => self.schema.to_string(),
        };
        Ok(Value::from(result))
    }

    fn is_hive_metastore(&self) -> Value {
        unimplemented!("InformationSchema")
    }

    fn normalize_component(&self, _component: &str) -> String {
        unimplemented!("InformationSchema")
    }

    fn create_relation(
        &self,
        _database: String,
        _schema: String,
        _identifier: Option<String>,
        _relation_type: Option<RelationType>,
        _quote_policy: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        unimplemented!("InformationSchema")
    }

    fn information_schema_inner(
        &self,
        _database: Option<String>,
        _view_name: &str,
    ) -> Result<Value, MinijinjaError> {
        unimplemented!("InformationSchema")
    }
}
