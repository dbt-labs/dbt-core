use crate::compiler::instructions::Instruction;
use crate::value::argtypes::KwargsValues;
use crate::value::{Value, ValueKind};
use crate::vm::types::adapter::AdapterType;
use crate::vm::types::api::{ApiColumnType, ApiType};
use crate::vm::types::builtin::Type;
use crate::vm::types::relation::RelationType;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

/// Returns the type of a value
pub fn infer_type_from_const_value(val: &Value) -> Type {
    match val.kind() {
        ValueKind::Number => {
            if val.is_integer() {
                Type::Integer
            } else {
                Type::Float
            }
        }

        ValueKind::Bool => Type::Bool,
        ValueKind::String => Type::String,
        ValueKind::Bytes => Type::Bytes,
        ValueKind::Seq => Type::Seq {
            field1: Box::new(Type::Any),
        },
        ValueKind::Map => {
            if val.downcast_object_ref::<KwargsValues>().is_some() {
                if let Ok(map_iter) = val.try_iter() {
                    let mut ty_map = BTreeMap::new();
                    for k in map_iter {
                        // For maps, the iterator yields keys; get the value for each key
                        if let Some(key_str) = k.as_str() {
                            if let Ok(v) = val.get_item(&k) {
                                let value_ty = infer_type_from_const_value(&v);
                                ty_map.insert(key_str.to_string(), Box::new(value_ty));
                            } else {
                                // fallback to generic Value if value can't be retrieved
                                return Type::Kwargs(BTreeMap::default());
                            }
                        } else {
                            // fallback to generic Value if keys are not strings
                            return Type::Kwargs(BTreeMap::default());
                        }
                    }
                    Type::Kwargs(ty_map)
                } else {
                    Type::Kwargs(BTreeMap::default())
                }
            } else if let Ok(map_iter) = val.try_iter() {
                let mut ty_map = BTreeMap::new();
                for k in map_iter {
                    // For maps, the iterator yields keys; get the value for each key
                    if let Some(key_str) = k.as_str() {
                        if let Ok(v) = val.get_item(&k) {
                            let value_ty = infer_type_from_const_value(&v);
                            ty_map.insert(key_str.to_string(), Box::new(value_ty));
                        } else {
                            // fallback to generic Value if value can't be retrieved
                            return Type::Map(BTreeMap::default());
                        }
                    } else {
                        // fallback to generic Value if keys are not strings
                        return Type::Map(BTreeMap::default());
                    }
                }
                Type::Map(ty_map)
            } else {
                Type::Map(BTreeMap::default())
            }
        }
        ValueKind::Iterable => Type::Iterable,
        ValueKind::Plain => Type::Plain,
        ValueKind::None => Type::None,
        ValueKind::Undefined => Type::Undefined,
        ValueKind::Invalid => Type::Invalid,
    }
}

/// Converts a string representation of a type to a `Type` enum variant.
pub fn parse_type(s: &str) -> Type {
    match s {
        "string" => Type::String,
        "integer" => Type::Integer,
        "float" => Type::Float,
        "bool" => Type::Bool,
        "bytes" => Type::Bytes,
        "seq" => Type::Seq {
            field1: Box::new(Type::Any),
        }, // Default to Value for seq
        "map" => Type::Map(BTreeMap::default()),
        "iterable" => Type::Iterable,
        "plain" => Type::Plain,
        "none" => Type::None,
        "undefined" => Type::Undefined,
        "invalid" => Type::Invalid,
        "relation_object" => Type::Relation(RelationType::default()),
        "adapter" => Type::Adapter(AdapterType::default()),
        "value" => Type::Any,
        "kwargs" => Type::Kwargs(BTreeMap::default()),
        "frame" => Type::Frame,
        "api" => Type::Api(ApiType::default()),
        "apicolumn" => Type::ApiColumn(ApiColumnType::default()),
        "stdcolumn" => Type::StdColumn,
        _ => panic!("Unknown type: {s}"),
    }
}

/// Gets the name of the instructions
pub fn instr_name(instr: &Instruction) -> &'static str {
    match instr {
        Instruction::Add => "Add",
        Instruction::Sub => "Sub",
        Instruction::Mul => "Mul",
        Instruction::Div => "Div",
        Instruction::Eq => "Eq",
        Instruction::Ne => "Ne",
        Instruction::Lt => "Lt",
        Instruction::Lte => "Lte",
        Instruction::Gt => "Gt",
        Instruction::Gte => "Gte",
        Instruction::Rem => "Rem",
        Instruction::Pow => "Pow",
        Instruction::StringConcat => "StringConcat",
        Instruction::In => "In",
        _ => "Other",
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
/// A location in a file.
pub struct CodeLocation {
    /// The line number.
    pub line: u32,
    /// The column number.
    pub col: u32,
    /// The file path.
    pub file: PathBuf,
}

impl fmt::Display for CodeLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 && self.col == 0 {
            write!(f, "{}", self.file.display())
        } else {
            write!(f, "{}:{}:{}", self.file.display(), self.line, self.col)
        }
    }
}
