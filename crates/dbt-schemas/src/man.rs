use crate::schemas::profiles::DbtProfiles;
use crate::schemas::project::DbtProject;
use crate::schemas::properties::DbtPropertiesFile;
use crate::schemas::selectors::SelectorFile;
use crate::schemas::telemetry::TelemetryRecord;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::err;
use dbt_common::io_args::EvalArgs;
use dbt_common::io_args::JsonSchemaTypes;
use dbt_common::macros::log_adapter::log;

use strum::IntoEnumIterator;

use schemars::r#gen::SchemaSettings;
use schemars::schema::*;
use schemars::schema::{InstanceType, Schema, SchemaObject};

use serde_json::to_string_pretty;

pub async fn execute_man_command(arg: &EvalArgs) -> FsResult<i32> {
    // create an error if arg.schema.is_empty
    if arg.schema.is_empty() {
        let available_schemas: Vec<String> = JsonSchemaTypes::iter()
            .map(|s| s.to_string().to_lowercase())
            .collect();
        return err!(
            ErrorCode::InvalidArgument,
            "Please provide a --schema <SCHEMA>, where <SCHEMA> is one of {}",
            available_schemas.join(", ")
        );
    }
    for schema_type in &arg.schema {
        match schema_type {
            JsonSchemaTypes::Profile => {
                let settings = SchemaSettings::default();
                let generator = settings.into_generator();
                let mut schema = generator.into_root_schema_for::<DbtProfiles>();
                deny_additional_properties_in_root(&mut schema);
                log::info!("{}", to_string_pretty(&schema)?);
            }
            JsonSchemaTypes::Project => {
                let settings = SchemaSettings::default();
                let generator = settings.into_generator();
                let mut schema = generator.into_root_schema_for::<DbtProject>();
                deny_additional_properties_in_root(&mut schema);
                log::info!("{}", to_string_pretty(&schema)?);
            }
            JsonSchemaTypes::Selector => {
                let settings = SchemaSettings::default();
                let generator = settings.into_generator();
                let mut schema = generator.into_root_schema_for::<SelectorFile>();
                deny_additional_properties_in_root(&mut schema);
                log::info!("{}", to_string_pretty(&schema)?);
            }
            JsonSchemaTypes::Schema => {
                let settings = SchemaSettings::default();
                let generator = settings.into_generator();
                let mut schema = generator.into_root_schema_for::<DbtPropertiesFile>();
                deny_additional_properties_in_root(&mut schema);
                log::info!("{}", to_string_pretty(&schema)?);
            }
            JsonSchemaTypes::Telemetry => {
                let settings = SchemaSettings::draft07();
                let generator = settings.into_generator();
                let schema = generator.into_root_schema_for::<TelemetryRecord>();
                log::info!("{}", to_string_pretty(&schema)?);
            }
        }
    }

    Ok(0)
}

/// Recursively modifies all object schemas in a `RootSchema`
/// to set "additionalProperties": false, unless the current path includes "meta"
pub fn deny_additional_properties_in_root(root: &mut RootSchema) {
    let mut path = Vec::new();
    deny_additional_properties_in_schema_object(&mut root.schema, &mut path);

    for (_name, def_schema) in root.definitions.iter_mut() {
        deny_additional_properties(def_schema, &mut path);
    }
}

// Applies the logic to a SchemaObject (used at the root)
fn deny_additional_properties_in_schema_object(
    schema_obj: &mut SchemaObject,
    path: &mut Vec<String>,
) {
    let mut schema = Schema::Object(schema_obj.clone());
    deny_additional_properties(&mut schema, path);
    if let Schema::Object(new_obj) = schema {
        *schema_obj = new_obj;
    }
}

// Recursively modifies the schema to set "additionalProperties": false
fn deny_additional_properties(schema: &mut Schema, path: &mut Vec<String>) {
    match schema {
        Schema::Object(SchemaObject {
            instance_type: Some(single_or_many),
            object: Some(validation),
            ..
        }) => {
            let types = match single_or_many {
                SingleOrVec::Single(boxed) => vec![*boxed.clone()],
                SingleOrVec::Vec(v) => v.clone(),
            };

            if types.contains(&InstanceType::Object)
                && !path.contains(&"meta".to_string())
                && !path.contains(&"column_types".to_string())
                && !path.contains(&"grants".to_string())
            {
                match validation
                    .additional_properties
                    .as_ref()
                    .map(|s| *s.clone())
                {
                    Some(Schema::Object(_)) => {}
                    _ => {
                        validation.additional_properties = Some(Box::new(Schema::Bool(false)));
                    }
                }
            }

            for (key, subschema) in validation.properties.iter_mut() {
                path.push(key.clone());
                deny_additional_properties(subschema, path);
                path.pop();
            }

            for (_key, subschema) in validation.pattern_properties.iter_mut() {
                deny_additional_properties(subschema, path);
            }
        }

        Schema::Object(SchemaObject {
            subschemas: Some(sub),
            ..
        }) => {
            for subschemas in sub
                .all_of
                .iter_mut()
                .chain(sub.any_of.iter_mut())
                .chain(sub.one_of.iter_mut())
            {
                for sub_schema in subschemas {
                    deny_additional_properties(sub_schema, path);
                }
            }
        }

        _ => {}
    }
}
