use crate::types::class::DynClassType;
use crate::types::function::DynFunctionType;
use crate::types::union::UnionType;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Represents the type of a value in the type system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Type {
    String,
    Integer,
    Float,
    Bool,
    Bytes,
    Seq { field1: Box<Type> },
    Map(BTreeMap<String, Box<Type>>),
    Iterable,
    Plain,
    None,
    Undefined,
    Invalid,
    Union(UnionType),
    Any,
    Kwargs(BTreeMap<String, Box<Type>>),
    Frame,
    Class(DynClassType),
    Function(DynFunctionType),
    StdColumn,
}

// only used in abrupt_return
impl crate::value::Object for Type {}

impl Type {
    pub fn get_attribute(&self, name: &str) -> Result<Type, crate::Error> {
        match self {
            Type::Map(map) => {
                if let Some(ty) = map.get(name) {
                    Ok(ty.as_ref().clone())
                } else {
                    Err(crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        "Type does not support method calls",
                    ))
                }
            }
            Type::Class(class) => class.get_attribute(name),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Type does not support attribute access",
            )),
        }
    }

    pub fn call(&self, args: &[Type]) -> Result<Type, crate::Error> {
        match self {
            Type::Function(func) => func.resolve_arguments(args),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Type does not support method calls",
            )),
        }
    }

    /// Judge whether there is a type cast between two types.
    pub fn is_type_cast(&self, other: &Type) -> bool {
        matches!(self, Type::Any) || matches!(other, Type::Any)
    }

    pub fn coerce(&self, other: &Type) -> Type {
        match (self, other) {
            (Type::Any, _) | (_, Type::Any) => Type::Any,
            (Type::None, type_) | (type_, Type::None) => type_.clone(),
            (Type::Union(_), _) | (_, Type::Union(_)) => {
                let mut result = Type::None;
                let self_types = self.flatten().unwrap();
                let other_types = other.flatten().unwrap();
                for a in &self_types {
                    for b in &other_types {
                        result = result.union(&a.coerce(b));
                    }
                }
                result
            }
            (Type::Seq { field1: a }, Type::Seq { field1: b }) => {
                let result = a.coerce(b);
                if result.is_none() {
                    Type::None
                } else {
                    Type::Seq {
                        field1: Box::new(result),
                    }
                }
            }
            _ => {
                if self == other {
                    self.clone()
                } else {
                    Type::None
                }
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn can_compare_with(&self, other: &Type, op: &'static str) -> bool {
        match (self, other) {
            (Type::Any, _) | (_, Type::Any) => true,
            (Type::None, _) | (_, Type::None) => true,
            (Type::Union(_), _) | (_, Type::Union(_)) => false,
            (Type::Seq { field1: a }, Type::Seq { field1: b }) => {
                // if a and b are comparable
                a.can_compare_with(b, op)
            }
            _ => self == other,
        }
    }

    pub fn get_seq_element_type(&self) -> Type {
        match self {
            Type::Seq {
                field1: element_type,
            } => element_type.as_ref().clone(),
            Type::Any => Type::Any,
            Type::Union(union_type) => {
                let mut element_type = Type::None;
                for ty in union_type.types.iter() {
                    element_type = element_type.union(&ty.get_seq_element_type());
                }
                element_type
            }
            _ => Type::None,
        }
    }

    pub fn is_condition(&self) -> bool {
        match self {
            Type::Bool => true,
            Type::Any => true,
            Type::Union(union_type) => union_type.types.iter().any(|ty| ty.is_condition()),
            _ => false,
        }
    }

    pub fn as_map(&self) -> Option<&BTreeMap<String, Box<Type>>> {
        match self {
            Type::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Type::None)
    }

    pub fn is_union(&self) -> bool {
        matches!(self, Type::Union(_))
    }

    pub fn is_any(&self) -> bool {
        matches!(self, Type::Any)
    }

    pub fn flatten(&self) -> Result<BTreeSet<Type>, crate::Error> {
        match self {
            Type::None => Ok(BTreeSet::new()),
            Type::Union(union_type) => Ok(union_type.types.clone()),
            Type::Any => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Cannot flatten any type",
            )),
            _ => Ok(BTreeSet::from_iter([self.clone()])),
        }
    }

    pub fn union(&self, other: &Type) -> Type {
        match (self, other) {
            (Type::None, _) => other.clone(),
            (_, Type::None) => self.clone(),
            (Type::Any, _) | (_, Type::Any) => Type::Any,
            (Type::Union(a), b) => a.union(b),
            (a, Type::Union(b)) => b.union(a),
            (a, b) => {
                if a == b {
                    a.clone()
                } else {
                    Type::Union(UnionType {
                        types: BTreeSet::from_iter([a.clone(), b.clone()]),
                    })
                }
            }
        }
    }
}

/// Implements the Display trait for the Type enum
impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::String => write!(f, "string"),
            Type::Integer => write!(f, "integer"),
            Type::Float => write!(f, "float"),
            Type::Bool => write!(f, "bool"),
            Type::Bytes => write!(f, "bytes"),
            Type::Seq { field1: elem_ty } => write!(f, "list[{elem_ty}]"),
            Type::Map(map) => {
                let entries: Vec<_> = map.iter().map(|(k, v)| format!("{k}: {v}")).collect();
                write!(f, "map{{{}}}", entries.join(", "))
            }

            Type::Iterable => write!(f, "iterable"),
            Type::Plain => write!(f, "plain"),
            Type::None => write!(f, "none"),
            Type::Undefined => write!(f, "undefined"),
            Type::Invalid => write!(f, "invalid"),
            Type::Union(union_type) => {
                let types_str: Vec<String> =
                    union_type.types.iter().map(|t| t.to_string()).collect();
                write!(f, "union[{}]", types_str.join(", "))
            }
            Type::Any => write!(f, "any"),
            Type::Kwargs(_) => write!(f, "kwargs"),
            Type::Frame => write!(f, "frame"),
            Type::Function(_) => write!(f, "function"),
            Type::StdColumn => write!(f, "stdcolumn"),
            Type::Class(class) => write!(f, "class[{}]", class.type_name()),
        }
    }
}
