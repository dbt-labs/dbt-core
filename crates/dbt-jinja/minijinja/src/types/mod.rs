pub mod adapter;
pub mod builtins;
pub mod dict;
/// Types for function signatures
pub mod funcsign_parser;
pub mod function;
pub mod iterable;
pub mod list;
pub mod string;
pub mod struct_;
pub mod timestamp;
pub mod tuple;
pub mod union;
pub mod utils;

#[macro_use]
pub mod type_erase;

use dashmap::DashMap;

use crate::types::builtins::{BuiltinDefinition, Reference};
use crate::types::dict::DictType;
use crate::types::function::{LambdaType, UserDefinedFunctionType};
use crate::types::iterable::IterableType;
use crate::types::list::ListType;
use crate::types::string::{
    StringFormatFunction, StringLowerFunction, StringReplaceFunction, StringSplitFunction,
    StringStripFunction, StringUpperFunction,
};
use crate::types::struct_::StructType;
use crate::types::timestamp::PyDateTimeStrftimeFunction;
use crate::types::tuple::TupleType;
use crate::types::union::UnionType;
use crate::TypecheckingEventListener;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::Hasher;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;

/// Represents the type of a value in the type system.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Type {
    /// String type, the parameter is the literal value of the string
    String(Option<String>),
    /// Integer type, the parameter is the literal value of the integer
    Integer(Option<i64>),
    /// Float type
    Float,
    /// Bool type
    Bool,
    /// Bytes type
    Bytes,
    /// TimeStamp type
    TimeStamp,
    /// Tuple type
    Tuple(TupleType),
    /// List type
    List(ListType),
    /// Struct type
    Struct(StructType),
    /// Iterable type
    Iterable(IterableType),
    /// Dict type
    Dict(DictType),
    /// Plain type
    Plain,
    /// None type
    None,
    /// Undefined type
    Undefined,
    /// Invalid type
    Invalid,
    /// Exception type
    Exception,
    /// Union type
    Union(UnionType),
    /// Any type
    Any {
        /// Soft any types are likely to be a implementation bug will be reported
        /// Hard any types means the type is dynamic, we won't be able to get it in the compile time
        /// Hard any example: load_result()
        hard: bool,
    },
    /// Kwargs type
    Kwargs(BTreeMap<String, Box<Type>>),
    /// Frame type
    Frame,
    /// Object type
    Object(DynObject),
    /// StdColumn type
    StdColumn,
    /// Namespace type
    Namespace(String),
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(None) => write!(f, "String"),
            Self::String(Some(s)) => write!(f, "String({s})"),
            Self::Integer(None) => write!(f, "Integer"),
            Self::Integer(Some(i)) => write!(f, "Integer({i})"),
            Self::Float => write!(f, "Float"),
            Self::Bool => write!(f, "Bool"),
            Self::Bytes => write!(f, "Bytes"),
            Self::TimeStamp => write!(f, "TimeStamp"),
            Self::Tuple(tuple) => write!(f, "Tuple({tuple:?})"),
            Self::List(list) => write!(f, "List({:?})", list.element),
            Self::Struct(struct_) => write!(f, "Struct({struct_:?})"),
            Self::Iterable(iterable) => write!(f, "Iterable({:?})", iterable.element),
            Self::Dict(dict) => write!(f, "Dict({:?}, {:?})", dict.key, dict.value),
            Self::Plain => write!(f, "Plain"),
            Self::None => write!(f, "None"),
            Self::Undefined => write!(f, "Undefined"),
            Self::Invalid => write!(f, "Invalid"),
            Self::Exception => write!(f, "Exception"),
            Self::Union(arg0) => write!(f, "{arg0:?}"),
            Self::Any { hard } => write!(f, "Any({hard})"),
            Self::Kwargs(arg0) => f.debug_tuple("Kwargs").field(arg0).finish(),
            Self::Frame => write!(f, "Frame"),
            Self::Object(arg0) => f.write_fmt(format_args!("{arg0:?}")),
            Self::StdColumn => write!(f, "StdColumn"),
            Self::Namespace(name) => write!(f, "Namespace({name})"),
        }
    }
}

// only used in abrupt_return
impl crate::value::Object for Type {}

impl Type {
    /// Get the attribute of the type
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the attribute to get
    ///
    pub fn get_attribute(
        &self,
        name: &str,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        match self {
            Type::Any { hard: true } => Ok(Type::Any { hard: true }),
            Type::Object(class) => class.get_attribute(name, listener),
            Type::Tuple(tuple) => tuple.get_attribute(name, listener),
            Type::List(list) => list.get_attribute(name, listener),
            Type::Struct(struct_) => struct_.get_attribute(name, listener),
            Type::Iterable(iterable) => iterable.get_attribute(name, listener),
            Type::Dict(dict) => dict.get_attribute(name, listener),
            Type::String(_) => match name {
                "strip" => Ok(Type::Object(DynObject::new(Arc::new(
                    StringStripFunction::default(),
                )))),
                "lower" => Ok(Type::Object(DynObject::new(Arc::new(
                    StringLowerFunction::default(),
                )))),
                "upper" => Ok(Type::Object(DynObject::new(Arc::new(
                    StringUpperFunction::default(),
                )))),
                "replace" => Ok(Type::Object(DynObject::new(Arc::new(
                    StringReplaceFunction::default(),
                )))),
                "split" => Ok(Type::Object(DynObject::new(Arc::new(
                    StringSplitFunction::default(),
                )))),
                "format" => Ok(Type::Object(DynObject::new(Arc::new(
                    StringFormatFunction::default(),
                )))),
                _ => {
                    listener.warn(&format!("{self:?}.{name} is not supported"));
                    Ok(Type::Any { hard: false })
                }
            },
            Type::TimeStamp => match name {
                "strftime" => Ok(Type::Object(DynObject::new(Arc::new(
                    PyDateTimeStrftimeFunction::default(),
                )))),
                _ => {
                    listener.warn(&format!("{self:?}.{name} is not supported"));
                    Ok(Type::Any { hard: false })
                }
            },
            _ => {
                listener.warn(&format!("{self:?}.{name} is not supported"));
                Ok(Type::Any { hard: false })
            }
        }
    }

    /// Call the type
    ///
    /// # Arguments
    ///
    /// * `positional_args` - The positional arguments to call the type with
    /// * `kwargs` - The keyword arguments to call the type with
    pub fn call(
        &self,
        positional_args: &[Type],
        kwargs: &BTreeMap<String, Type>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        match self {
            Type::Object(func) => func.call(positional_args, kwargs, listener),
            _ => {
                listener.warn(&format!("{self:?} does not support method calls"));
                Ok(Type::Any { hard: false })
            }
        }
    }

    /// Subscript the type
    ///
    /// # Arguments
    ///
    /// * `index` - The index to subscript the type with
    ///
    pub fn subscript(
        &self,
        index: &Type,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        match self {
            Type::Struct(struct_) => struct_.subscript(index, listener),
            Type::Dict(dict) => dict.subscript(index, listener),
            Type::List(list) => list.subscript(index, listener),
            Type::Iterable(iterable) => iterable.subscript(index, listener),
            Type::Tuple(tuple) => tuple.subscript(index, listener),
            Type::Object(class) => class.subscript(index, listener),
            Type::Any { hard: true } => Ok(Type::Any { hard: true }),
            _ => {
                listener.warn(&format!("{self:?} does not support subscript"));
                Ok(Type::Any { hard: false })
            }
        }
    }

    /// Checks if this type is a subtype of another type.
    ///
    /// This method is primarily used for function parameter matching to determine
    /// if an argument type can be safely passed to a function expecting a specific
    /// parameter type.
    ///
    /// # Arguments
    ///
    /// * `other` - The target type to check subtype relationship against
    ///
    /// # Returns
    ///
    /// `true` if `self` is a subtype of `other`, `false` otherwise
    ///
    /// # Subtype Rules
    ///
    /// - **Any type**: Acts as a top type - all types are subtypes of `Any`, but `Any`
    ///   is not a subtype of any other type (except `Any` itself)
    /// - **String types**: All string types are compatible with each other
    /// - **Union types**: A type is a subtype of a union if it's a subtype of any member;
    ///   a union is a subtype of another type if all its members are subtypes
    /// - **Container types**: List, Dict, Tuple, Iterable require their element types
    ///   to have compatible subtype relationships
    /// - **Struct to Dict**: A struct can be a subtype of a dict under certain conditions
    /// - **Class types**: Classes with the same type name are considered compatible
    /// - **Exact equality**: For other types, exact equality is required
    ///
    /// # Function Parameter Matching
    ///
    /// When calling a function `f(expected_type)` with `actual_type`:
    /// - ✅ `actual_type.is_subtype_of(expected_type)` → argument accepted
    /// - ❌ `expected_type.is_subtype_of(actual_type)` → argument rejected
    ///
    /// # Examples
    ///
    /// ```rust
    /// // Any type relationships
    /// assert!(String.is_subtype_of(Any));    // ✅ String <: Any
    /// assert!(!Any.is_subtype_of(String));   // ❌ Any <: String
    ///
    /// // Function parameter matching
    /// fn process(param: Any) { ... }
    /// process(string_value);  // ✅ String can be passed to Any parameter
    ///
    /// fn format(param: String) { ... }
    /// format(any_value);      // ❌ Any cannot be passed to String parameter
    /// ```
    pub fn is_subtype_of(&self, other: &Type) -> bool {
        match (self, other) {
            (Type::Any { hard: true }, _) => true,

            // All types are subtypes of Any
            (_, Type::Any { hard: true }) => true,

            // String types are compatible with each other
            (Type::String(_), Type::String(_)) => true,

            // Integer types are compatible with each other
            (Type::Integer(_), Type::Integer(_)) => true,

            // Handle union types - a type is a subtype of a union if it's a subtype of any member
            (type_, Type::Union(UnionType { types })) if !type_.is_union() => {
                types.iter().any(|ty| type_.is_subtype_of(ty))
            }

            // A union is a subtype of another type if all its members are subtypes
            (Type::Union(UnionType { types }), other_type) => {
                types.iter().all(|ty| ty.is_subtype_of(other_type))
            }

            // Struct can be converted to Dict under certain conditions
            (Type::Struct(StructType { fields }), Type::Dict(DictType { key, value })) => {
                matches!(key.as_ref(), Type::String(_))
                    && (fields.is_empty()
                        || fields.values().all(|v| v.is_subtype_of(value.as_ref())))
            }

            // Struct subtype relationship - all fields must be compatible
            (
                Type::Struct(StructType { fields }),
                Type::Struct(StructType {
                    fields: other_fields,
                }),
            ) => {
                fields.len() == other_fields.len()
                    && fields.iter().zip(other_fields.iter()).all(
                        |((a_name, a_type), (b_name, b_type))| {
                            a_name == b_name && a_type.is_subtype_of(b_type)
                        },
                    )
            }

            // List subtype relationship - element types must be compatible
            (Type::List(a), Type::List(b)) => a.element.is_subtype_of(&b.element),

            // List and Iterable are compatible
            (Type::List(a), Type::Iterable(b)) | (Type::Iterable(b), Type::List(a)) => {
                a.element.is_subtype_of(&b.element)
            }

            // Iterable subtype relationship - element types must be compatible
            (Type::Iterable(a), Type::Iterable(b)) => a.element.is_subtype_of(&b.element),

            // Dict subtype relationship - key and value types must be compatible
            (Type::Dict(a), Type::Dict(b)) => {
                (a.key.is_subtype_of(&b.key)) && (a.value.is_subtype_of(&b.value))
            }

            // Tuple subtype relationship - all element types must be compatible
            (Type::Tuple(a), Type::Tuple(b)) => {
                a.fields.len() == b.fields.len()
                    && a.fields
                        .iter()
                        .zip(b.fields.iter())
                        .all(|(a_elem, b_elem)| a_elem.is_subtype_of(b_elem))
            }

            // None type handling
            (Type::None, Type::None) => true,

            (Type::Object(a), b) => {
                if let Some(a) = a.downcast_ref::<BuiltinDefinition>() {
                    if let Some(alias_type) = a.get_alias_type() {
                        return alias_type.is_subtype_of(b);
                    }
                }
                if let Some(b) = b.as_object() {
                    let func_a = if let Some(a) = a.downcast_ref::<LambdaType>() {
                        Some(a.clone())
                    } else {
                        a.downcast_ref::<UserDefinedFunctionType>()
                            .map(|a| a.clone().into())
                    };
                    let func_b = if let Some(b) = b.downcast_ref::<LambdaType>() {
                        Some(b.clone())
                    } else {
                        b.downcast_ref::<UserDefinedFunctionType>()
                            .map(|b| b.clone().into())
                    };

                    if let (Some(a), Some(b)) = (func_a, func_b) {
                        if a.args.len() != b.args.len() {
                            return false;
                        }
                        for (a_arg, b_arg) in a.args.iter().zip(b.args.iter()) {
                            if !a_arg.is_subtype_of(b_arg) {
                                return false;
                            }
                        }
                        a.ret_type.is_subtype_of(&b.ret_type)
                    } else {
                        a.type_debug() == b.type_debug()
                    }
                } else {
                    false
                }
            }
            (a, Type::Object(b)) => {
                if let Some(b) = b.downcast_ref::<Reference>() {
                    if let Ok(type_) = b.get_type() {
                        return a.is_subtype_of(&type_);
                    }
                } else if let Some(b) = b.downcast_ref::<BuiltinDefinition>() {
                    if let Some(alias_type) = b.get_alias_type() {
                        return a.is_subtype_of(&alias_type);
                    }
                }
                false
            }

            // For all other cases, check for exact equality
            _ => self == other,
        }
    }

    /// Check if the type can be compared with another type
    ///
    /// # Arguments
    ///
    /// * `other` - The type to compare with
    /// * `op` - The operator to compare with
    ///
    #[allow(clippy::only_used_in_recursion)]
    pub fn can_compare_with(&self, other: &Type, op: &'static str) -> bool {
        match op {
            // Equality operators (== and !=) - any type can be compared for equality
            "==" | "!=" => {
                match (self, other) {
                    // Any type allows equality comparison (runtime will handle the actual comparison)
                    (Type::Any { hard: true }, _) | (_, Type::Any { hard: true }) => true,
                    // None can compare with anything for equality
                    (Type::None, _) | (_, Type::None) => true,
                    // Union types - check if all members can compare for equality
                    (Type::Union(union_type), other_type) => union_type
                        .types
                        .iter()
                        .all(|member| member.can_compare_with(other_type, op)),
                    (other_type, Type::Union(union_type)) => union_type
                        .types
                        .iter()
                        .all(|member| other_type.can_compare_with(member, op)),
                    // Same types can be ordered
                    (a, b) if a == b => true,

                    // Numeric types can be compared with each other
                    (Type::Integer(_), Type::Float) | (Type::Float, Type::Integer(_)) => true,

                    // Integer can be compared with each other
                    (Type::Integer(_), Type::Integer(_)) => true,

                    // String types can be compared with each other
                    (Type::String(_), Type::String(_)) => true,

                    // Bool can be compared with bool
                    (Type::Bool, Type::Bool) => true,

                    // List can be compared with each other
                    (Type::List(ListType { element: a }), Type::List(ListType { element: b })) => {
                        a.can_compare_with(b, op)
                    }

                    // Default: no ordering possible
                    _ => false,
                }
            }

            // Ordering operators (<, <=, >, >=) - require compatible types
            "<" | "<=" | ">" | ">=" => {
                match (self, other) {
                    // Any type is uncertain for ordering - we can't guarantee it will work
                    (Type::Any { hard: true }, _) | (_, Type::Any { hard: true }) => false,

                    // None cannot be ordered with other types
                    (Type::None, Type::None) => true,
                    (Type::None, _) | (_, Type::None) => false,

                    // Union types - all members must be able to compare for ordering
                    (Type::Union(union_type), other_type) => union_type
                        .types
                        .iter()
                        .all(|member| member.can_compare_with(other_type, op)),
                    (other_type, Type::Union(union_type)) => union_type
                        .types
                        .iter()
                        .all(|member| other_type.can_compare_with(member, op)),

                    // Same types can be ordered
                    (a, b) if a == b => true,

                    // Numeric types can be compared with each other
                    (Type::Integer(_), Type::Float) | (Type::Float, Type::Integer(_)) => true,

                    // Integer can be compared with each other
                    (Type::Integer(_), Type::Integer(_)) => true,

                    // String types can be compared with each other
                    (Type::String(_), Type::String(_)) => true,

                    // Bool can be compared with bool
                    (Type::Bool, Type::Bool) => true,

                    // Default: no ordering possible
                    _ => false,
                }
            }

            // Unknown operator - conservative approach
            _ => {
                match (self, other) {
                    // Any type allows unknown operations (runtime will handle it)
                    (Type::Any { hard: true }, _) | (_, Type::Any { hard: true }) => true,
                    (Type::None, _) | (_, Type::None) => true,
                    // Union types - check all members
                    (Type::Union(union_type), other_type) => union_type
                        .types
                        .iter()
                        .all(|member| member.can_compare_with(other_type, op)),
                    (other_type, Type::Union(union_type)) => union_type
                        .types
                        .iter()
                        .all(|member| other_type.can_compare_with(member, op)),
                    _ => self == other,
                }
            }
        }
    }

    /// Check if the type can do a binary operation with another type
    ///
    /// # Arguments
    ///
    /// * `other` - The type to do the binary operation with
    /// * `op` - The operator to do the binary operation with
    ///
    #[allow(clippy::only_used_in_recursion)]
    pub fn can_binary_op_with(
        &self,
        other: &Type,
        op: &'static str,
        registry: Arc<DashMap<String, Type>>,
    ) -> Option<Type> {
        match (self, other, op) {
            // Any type can do binary operations with anything, result is Any
            (Type::Any { hard: true }, _, _) | (_, Type::Any { hard: true }, _) => {
                Some(Type::Any { hard: true })
            }

            // None type binary operations are generally not supported
            (Type::None, _, _) | (_, Type::None, _) => None,

            // Union types are complex, return None for now
            (Type::Union(_), _, _) | (_, Type::Union(_), _) => None,

            // String operations
            (Type::String(_), Type::String(_), "+") => Some(Type::String(None)),

            // Integer operations
            (Type::Integer(_), Type::Integer(_), "+" | "-" | "*" | "/" | "//" | "%" | "**") => {
                Some(Type::Integer(None))
            }

            // Float operations
            (Type::Float, Type::Float, "+" | "-" | "*" | "/" | "//" | "%" | "**") => {
                Some(Type::Float)
            }

            // Mixed integer/float operations
            (Type::Integer(_), Type::Float, "+" | "-" | "*" | "/" | "//" | "%" | "**")
            | (Type::Float, Type::Integer(_), "+" | "-" | "*" | "/" | "//" | "%" | "**") => {
                Some(Type::Float)
            }

            // String formatting (% operator)
            (Type::String(_), Type::List(_) | Type::Tuple(_), "%") => Some(Type::String(None)),

            (Type::TimeStamp, Type::TimeStamp, "-") => registry
                .get("modules.datetime.timedelta")
                .map(|timedelta| timedelta.value().clone()),

            (Type::TimeStamp, Type::Object(class), "+")
                if class.type_debug() == "modules.datetime.timedelta" =>
            {
                Some(Type::TimeStamp)
            }
            (Type::Object(class), Type::TimeStamp, "+")
                if class.type_debug() == "modules.datetime.timedelta" =>
            {
                Some(Type::TimeStamp)
            }
            (Type::List(ListType { element: a }), Type::List(ListType { element: b }), "+") => {
                if a.is_subtype_of(b) {
                    Some(Type::List(ListType { element: a.clone() }))
                } else if b.is_subtype_of(a) {
                    Some(Type::List(ListType { element: b.clone() }))
                } else {
                    None
                }
            }

            // Default: check if types are equal
            _ => {
                if self == other {
                    Some(self.clone())
                } else {
                    None
                }
            }
        }
    }

    /// Check if the type is a condition
    ///
    /// # Arguments
    ///
    /// * `self` - The type to check if it is a condition
    ///
    pub fn is_condition(&self) -> bool {
        !matches!(self, Type::Any { hard: false })
    }

    /// Check if the type is none
    ///
    /// # Arguments
    ///
    /// * `self` - The type to check if it is none
    ///
    pub fn is_none(&self) -> bool {
        matches!(self, Type::None)
    }

    /// Check if the type is a union
    ///
    /// # Arguments
    ///
    /// * `self` - The type to check if it is a union
    ///
    pub fn is_union(&self) -> bool {
        matches!(self, Type::Union(_))
    }

    /// Check if the type is any
    ///
    /// # Arguments
    ///
    /// * `self` - The type to check if it is any
    ///
    pub fn is_any(&self) -> bool {
        matches!(self, Type::Any { .. })
    }

    /// Check if the type is a namespace
    pub fn is_namespace(&self) -> bool {
        matches!(self, Type::Namespace(_))
    }

    /// Flatten the type
    ///
    /// # Arguments
    ///
    /// * `self` - The type to flatten
    ///
    pub fn flatten(&self) -> Result<BTreeSet<Type>, crate::Error> {
        match self {
            Type::Union(union_type) => Ok(union_type.types.clone()),
            Type::Any { .. } => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Cannot flatten any type",
            )),
            _ => Ok(BTreeSet::from_iter([self.clone()])),
        }
    }

    /// Union the type
    ///
    /// # Arguments
    ///
    /// * `other` - The type to union with
    ///
    pub fn union(&self, other: &Type) -> Type {
        match (self, other) {
            // Any type handling
            (Type::Any { hard: true }, _) | (_, Type::Any { hard: true }) => {
                Type::Any { hard: true }
            }

            (Type::List(ListType { element }), Type::List(_))
                if matches!(element.as_ref(), Type::Any { hard: true }) =>
            {
                Type::List(ListType {
                    element: Box::new(Type::Any { hard: true }),
                })
            }

            (Type::List(_), Type::List(ListType { element }))
                if matches!(element.as_ref(), Type::Any { hard: true }) =>
            {
                Type::List(ListType {
                    element: Box::new(Type::Any { hard: true }),
                })
            }

            // If self is a union, use its union method
            (Type::Union(self_union), other_type) => self_union.union(other_type),

            // If other is a union, directly use its union method
            (self_type, Type::Union(other_union)) => other_union.union(self_type),

            // Neither is union - create temporary union and use union logic
            (a, b) => {
                let temp_union = UnionType::new([a.clone()]);
                temp_union.union(b)
            }
        }
    }

    /// Check if the type is a object
    ///
    /// # Arguments
    ///
    /// * `self` - The type to check if it is a object
    ///
    pub fn as_object(&self) -> Option<&DynObject> {
        match self {
            Type::Object(class) => Some(class),
            _ => None,
        }
    }

    /// Check if the type is optional
    ///
    /// # Arguments
    ///
    /// * `self` - The type to check if it is optional
    ///
    pub fn is_optional(&self) -> bool {
        if let Type::Union(unionty) = self {
            unionty.is_optional()
        } else {
            false
        }
    }

    /// Get the non-optional type
    ///
    /// # Arguments
    ///
    /// * `self` - The type to get the non-optional type from
    ///
    pub fn get_non_optional_type(&self) -> Type {
        if let Type::Union(union) = self {
            union.get_non_optional_type()
        } else {
            self.clone()
        }
    }

    /// Exclude the type
    ///
    /// # Arguments
    ///
    /// * `other` - The type to exclude
    ///
    pub fn exclude(&self, other: &Type) -> Type {
        if let Type::Union(union) = self {
            union.exclude(other)
        } else if other == self {
            Type::None
        } else {
            self.clone()
        }
    }
}

/// Implements the Display trait for the Type enum
impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // same as Debug
        write!(f, "{self:?}")
    }
}

impl FromStr for Type {
    fn from_str(s: &str) -> Result<Self, crate::Error> {
        match s {
            "string" => Ok(Type::String(None)),
            "integer" => Ok(Type::Integer(None)),
            "number" => Ok(Type::Integer(None)),
            "float" => Ok(Type::Float),
            "bool" => Ok(Type::Bool),
            "bytes" => Ok(Type::Bytes),
            "timestamp" => Ok(Type::TimeStamp),
            "none" => Ok(Type::None),
            "defined" => Ok(Type::Any { hard: true }),
            "sequence" => Ok(Type::List(ListType::new(Type::Any { hard: true }))),
            "iterable" => Ok(Type::Iterable(IterableType::new(Type::Any { hard: true }))),
            "callable" => Ok(Type::Any { hard: true }),
            "mapping" => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Invalid type: {s}"),
            )),
        }
    }

    type Err = crate::Error;
}

pub trait Object: Send + Sync + std::fmt::Debug {
    fn get_attribute(
        &self,
        name: &str,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        listener.warn(&format!("{self:?} does not support attribute {name}"));
        Ok(Type::Any { hard: false })
    }
    fn call(
        &self,
        _positional_args: &[Type],
        _kwargs: &BTreeMap<String, Type>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        listener.warn(&format!("{self:?} does not support call"));
        Ok(Type::Any { hard: false })
    }
    fn subscript(
        &self,
        _index: &Type,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        listener.warn(&format!("{self:?} does not support subscript"));
        Ok(Type::Any { hard: false })
    }
}

type_erase! {
    pub trait Object => DynObject {
        fn get_attribute(&self, name: &str, listener: Rc<dyn TypecheckingEventListener>) -> Result<Type, crate::Error>;
        fn call(&self, positional_args: &[Type], kwargs: &BTreeMap<String, Type>, listener: Rc<dyn TypecheckingEventListener>) -> Result<Type, crate::Error>;
        fn subscript(&self, index: &Type, listener: Rc<dyn TypecheckingEventListener>) -> Result<Type, crate::Error>;
    }
}

impl std::fmt::Debug for DynObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_debug())
    }
}

impl PartialEq for DynObject {
    fn eq(&self, other: &Self) -> bool {
        // Compare by pointer equality for type-erased objects
        self.ptr == other.ptr && self.vtable == other.vtable
    }
}

impl Eq for DynObject {}

impl std::hash::Hash for DynObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the pointer and vtable
        self.ptr.hash(state);
        self.vtable.hash(state);
    }
}

impl PartialOrd for DynObject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DynObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by pointer values
        match self.ptr.cmp(&other.ptr) {
            std::cmp::Ordering::Equal => self.vtable.cmp(&other.vtable),
            other => other,
        }
    }
}

unsafe impl Send for DynObject {}
unsafe impl Sync for DynObject {}
