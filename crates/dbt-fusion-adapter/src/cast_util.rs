//! A set of util functions for casting from/to Value
use crate::relation_object::RelationObject;

use dbt_schemas::schemas::relations::base::BaseRelation;
use minijinja::Error as MinijinjaError;
use minijinja::ErrorKind as MinijinjaErrorKind;
use minijinja::Value as MinijinjaValue;

use std::file;
use std::sync::Arc;

pub const THIS_RELATION_KEY: &str = "__this__";

/// Downcast a MinijinjaValue to a dyn BaseRelation object
pub fn downcast_value_to_dyn_base_relation(
    value: &MinijinjaValue,
) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
    if let Some(relation_object) = value.downcast_object::<RelationObject>() {
        Ok(relation_object.inner())
    } else if let Some(relation_object) = value
        .as_object()
        .expect("relation must be an object")
        .get_value(&MinijinjaValue::from(THIS_RELATION_KEY))
    {
        Ok(relation_object
            .downcast_object::<RelationObject>()
            .expect("this must be a BaseRelation object")
            .inner())
    } else {
        Err(MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            format!(
                "Unsupported relation type ({}) in {}:{}",
                value,
                file!(),
                line!()
            ),
        ))
    }
}
