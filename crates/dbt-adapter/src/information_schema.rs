use crate::AdapterType;

use dbt_common::FsResult;
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::{
    dbt_types::RelationType,
    schemas::relations::base::{BaseRelation, BaseRelationProperties, Policy},
};

use std::{any::Any, sync::Arc};

#[derive(Clone, Debug)]
pub struct InformationSchema {
    pub adapter_type: AdapterType,
    pub database: Option<String>,
    pub schema: String,
    pub identifier: Option<String>,
    // quote_policy
    pub location: Option<String>,
    pub is_delta: Option<bool>,
}

impl InformationSchema {
    pub fn default(adapter_type: AdapterType) -> Self {
        Self {
            adapter_type,
            database: None,
            schema: String::default(),
            identifier: None,
            location: None,
            is_delta: None,
        }
    }

    pub fn try_from_relation(
        adapter_type: AdapterType,
        relation_database: Option<String>,
        information_schema_view: Option<&str>,
    ) -> Result<Self, minijinja::Error> {
        // Create the InformationSchema object with the database name as none if it is an empty string
        Ok(Self {
            adapter_type,
            database: if relation_database.is_some() && relation_database.clone().unwrap() == "" {
                None
            } else {
                relation_database
            },
            schema: "INFORMATION_SCHEMA".to_string(),
            identifier: information_schema_view.map(|s| s.to_string()),
            location: None,
            is_delta: None,
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

    fn get_database(&self) -> FsResult<String> {
        Ok(self.database.clone().unwrap_or_default())
    }

    fn get_schema(&self) -> FsResult<String> {
        Ok(self.schema.clone())
    }

    fn get_identifier(&self) -> FsResult<String> {
        Ok(self.identifier.clone().unwrap_or_default())
    }

    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        Ok(CanonicalFqn::new(
            &Identifier::new(self.get_database()?),
            &Identifier::new(self.get_schema()?),
            &Identifier::new(self.get_identifier()?),
        ))
    }
}

impl BaseRelation for InformationSchema {
    fn as_any(&self) -> &dyn Any {
        unimplemented!("information schema as_any trait downcasting")
    }

    fn to_owned(&self) -> Arc<dyn BaseRelation> {
        Arc::new(self.clone())
    }

    fn create_from(&self) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("information schema relation creation from Jinja values")
    }

    fn is_delta(&self) -> bool {
        self.is_delta.unwrap_or(false)
    }

    fn set_is_delta(&mut self, is_delta: Option<bool>) {
        self.is_delta = is_delta;
    }

    fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    fn schema(&self) -> Option<&str> {
        Some(&self.schema)
    }

    fn identifier(&self) -> Option<&str> {
        self.identifier.as_deref()
    }

    fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn include_inner(&self, _args: Policy) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("InformationSchema")
    }

    fn render_self_as_str(&self) -> String {
        match (&self.database, &self.location, &self.identifier) {
            // With database and location
            (Some(database), Some(location), Some(identifier)) => {
                let region = format!("`region-{location}`");
                format!("{database}.{region}.{}.{}", &self.schema, identifier)
            }
            (Some(database), Some(location), None) => {
                let region = format!("`region-{location}`");
                format!("{database}.{region}.{}", &self.schema)
            }

            // With database, without location
            (Some(database), None, Some(identifier)) => {
                format!("{}.{}.{}", database, &self.schema, identifier)
            }
            (Some(database), None, None) => format!("{}.{}", database, &self.schema),

            // Without database (ignore location if present)
            (None, Some(_), Some(identifier)) => format!("{}.{}", &self.schema, identifier),
            (None, Some(_), None) => self.schema.to_string(),
            (None, None, Some(identifier)) => format!("{}.{}", &self.schema, identifier),
            (None, None, None) => self.schema.to_string(),
        }
    }

    fn is_hive_metastore(&self) -> bool {
        unimplemented!("InformationSchema")
    }

    fn normalize_component(&self, _component: &str) -> String {
        unimplemented!("InformationSchema")
    }

    fn create_relation(
        &self,
        _database: Option<String>,
        _schema: Option<String>,
        _identifier: Option<String>,
        _relation_type: Option<RelationType>,
        _quote_policy: Policy,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("InformationSchema")
    }

    fn information_schema_inner(
        &self,
        _database: Option<String>,
        _view_name: Option<&str>,
    ) -> Result<Arc<dyn BaseRelation>, minijinja::Error> {
        unimplemented!("InformationSchema")
    }
}
