use std::{rc::Rc, sync::Arc};

use dashmap::DashMap;

use crate::{
    types::{
        dbt::DbtType,
        funcsign_parser::parse_type,
        function::{
            Argument, BatchFunctionType, FirstFunctionType, FunctionType, ListFunctionType,
            MapFunctionType, PrintFunctionType, RejectAttrFunctionType, SelectAttrFunctionType,
            TryOrCompilerErrorFunctionType, UserDefinedFunctionType,
        },
        DynObject, Object, Type,
    },
    TypecheckingEventListener,
};

/// Load built-in types from yml
///
/// This function is used to load built-in types from yml.
/// It is used to type check the template.
///
/// # Returns
/// A map of object id to Type.
pub fn load_builtins() -> Result<Arc<DashMap<String, Type>>, crate::Error> {
    let definitions = minijinja_typecheck_builtins::get_definitions();
    let registry = Arc::new(DashMap::new()); // key is object id, value is Type

    for definition in definitions.iter() {
        let definition: Definition = definition.clone().into();
        let id = definition.get_id();
        let dyn_object = DynObject::new(Arc::new(BuiltinDefinition::new(
            &definition,
            registry.clone(),
        )));
        registry.insert(id, Type::Object(dyn_object));
    }
    registry.insert(
        "adapter.dispatch".to_string(),
        Type::Object(DynObject::new(Arc::new(
            crate::types::adapter::AdapterDispatchFunction::instance(),
        ))),
    );
    registry.insert(
        "map".to_string(),
        Type::Object(DynObject::new(Arc::new(MapFunctionType::default()))),
    );
    registry.insert(
        "list".to_string(),
        Type::Object(DynObject::new(Arc::new(ListFunctionType::default()))),
    );
    // TODO: dbt is a namespace
    registry.insert(
        "dbt".to_string(),
        Type::Object(DynObject::new(Arc::new(DbtType::default()))),
    );
    registry.insert(
        "try_or_compiler_error".to_string(),
        Type::Object(DynObject::new(Arc::new(
            TryOrCompilerErrorFunctionType::default(),
        ))),
    );
    registry.insert(
        "selectattr".to_string(),
        Type::Object(DynObject::new(Arc::new(SelectAttrFunctionType::default()))),
    );
    registry.insert(
        "rejectattr".to_string(),
        Type::Object(DynObject::new(Arc::new(RejectAttrFunctionType::default()))),
    );
    registry.insert(
        "print".to_string(),
        Type::Object(DynObject::new(Arc::new(PrintFunctionType::default()))),
    );
    registry.insert(
        "first".to_string(),
        Type::Object(DynObject::new(Arc::new(FirstFunctionType::default()))),
    );
    registry.insert(
        "batch".to_string(),
        Type::Object(DynObject::new(Arc::new(BatchFunctionType::default()))),
    );

    Ok(registry)
}

#[derive(Clone)]
pub(crate) enum Definition {
    Object(minijinja_typecheck_builtins::Object),
    Alias(minijinja_typecheck_builtins::Alias),
}

impl From<minijinja_typecheck_builtins::Definition> for Definition {
    fn from(definition: minijinja_typecheck_builtins::Definition) -> Self {
        if let Some(object) = definition.object {
            Definition::Object(object)
        } else if let Some(alias) = definition.alias {
            Definition::Alias(alias)
        } else {
            panic!("Definition has no object or alias");
        }
    }
}

impl Definition {
    pub(crate) fn get_id(&self) -> String {
        match self {
            Definition::Object(object) => object.id.clone(),
            Definition::Alias(alias) => alias.id.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct BuiltinDefinition {
    pub(crate) definition: Definition,
    registry: Arc<DashMap<String, Type>>,
}

impl std::fmt::Debug for BuiltinDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.definition.get_id())
    }
}

impl BuiltinDefinition {
    pub(crate) fn new(definition: &Definition, registry: Arc<DashMap<String, Type>>) -> Self {
        Self {
            definition: definition.clone(),
            registry,
        }
    }

    pub(crate) fn get_alias_type(&self) -> Option<Type> {
        match &self.definition {
            Definition::Alias(alias) => {
                Some(parse_type(alias.type_.as_str(), self.registry.clone()).unwrap())
            }
            Definition::Object(_) => None,
        }
    }
}

impl Object for BuiltinDefinition {
    fn get_attribute(
        &self,
        name: &str,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<super::Type, crate::Error> {
        match &self.definition {
            Definition::Alias(alias) => {
                let object =
                    parse_type(alias.type_.as_str(), self.registry.clone()).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("parse type {} failed: {}", alias.type_, e),
                        )
                    })?;
                object.get_attribute(name, listener)
            }
            Definition::Object(object) => {
                if let Some(attributes) = &object.attributes {
                    for attribute in attributes {
                        if attribute.name == name {
                            return parse_type(attribute.type_.as_str(), self.registry.clone())
                                .map_err(|e| {
                                    crate::Error::new(
                                        crate::error::ErrorKind::InvalidOperation,
                                        std::format!(
                                            "parse type {} failed: {}",
                                            attribute.type_,
                                            e
                                        ),
                                    )
                                });
                        }
                    }
                }
                if let Some(parent) = &object.inherit_from {
                    let parent_object = parse_type(parent, self.registry.clone()).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("parse type {parent} failed: {e}"),
                        )
                    })?;
                    return parent_object.get_attribute(name, listener).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("Failed to get {self:?}.{name} from {parent}: {e}"),
                        )
                    });
                }
                listener.warn(&format!("{self:?}.{name} does not exist"));
                Ok(Type::Any { hard: false })
            }
        }
    }

    fn call(
        &self,
        positional_args: &[super::Type],
        kwargs: &std::collections::BTreeMap<String, super::Type>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<super::Type, crate::Error> {
        match &self.definition {
            Definition::Alias(alias) => {
                let object =
                    parse_type(alias.type_.as_str(), self.registry.clone()).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("parse type {} failed: {}", alias.type_, e),
                        )
                    })?;
                object.call(positional_args, kwargs, listener)
            }
            Definition::Object(object) => {
                if let Some(call) = &object.call {
                    let args = call
                        .arguments
                        .iter()
                        .map(|arg| {
                            Ok(Argument {
                                name: arg.name.clone(),
                                type_: parse_type(arg.type_.as_str(), self.registry.clone())
                                    .map_err(|e| {
                                        crate::Error::new(
                                            crate::error::ErrorKind::InvalidOperation,
                                            std::format!("parse type {} failed: {}", arg.type_, e),
                                        )
                                    })?,
                                is_optional: arg.is_optional,
                            })
                        })
                        .collect::<Result<Vec<Argument>, crate::Error>>()?;
                    let ret_type = parse_type(call.return_type.as_str(), self.registry.clone())
                        .map_err(|e| {
                            crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                std::format!("parse type {} failed: {}", call.return_type, e),
                            )
                        })?;
                    let udf = UserDefinedFunctionType::new("udf", args, ret_type);
                    return udf.resolve_arguments(positional_args, kwargs, listener);
                }
                if let Some(parent) = &object.inherit_from {
                    let parent_object = parse_type(parent, self.registry.clone()).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("parse type {parent} failed: {e}"),
                        )
                    })?;
                    return parent_object
                        .call(positional_args, kwargs, listener)
                        .map_err(|e| {
                            crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                std::format!("Failed to call {self:?} from {parent}: {e}"),
                            )
                        });
                }
                listener.warn(&format!("{self:?} does not support call"));
                Ok(Type::Any { hard: false })
            }
        }
    }

    fn subscript(
        &self,
        index: &super::Type,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<super::Type, crate::Error> {
        match &self.definition {
            Definition::Alias(alias) => {
                let object =
                    parse_type(alias.type_.as_str(), self.registry.clone()).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("parse type {} failed: {}", alias.type_, e),
                        )
                    })?;
                object.subscript(index, listener)
            }
            Definition::Object(object) => {
                if let Some(parent) = &object.inherit_from {
                    let parent_object = parse_type(parent, self.registry.clone()).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("parse type {parent} failed: {e}"),
                        )
                    })?;
                    return parent_object.subscript(index, listener).map_err(|e| {
                        crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            std::format!("Failed to subscript {self:?} from {parent}: {e}"),
                        )
                    });
                }
                listener.warn(&format!("{self:?} does not support subscript"));
                Ok(Type::Any { hard: false })
            }
        }
    }
}

pub(crate) struct Reference {
    id: String,
    registry: Arc<DashMap<String, Type>>,
}

impl std::fmt::Debug for Reference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl Reference {
    pub(crate) fn new(id: String, registry: Arc<DashMap<String, Type>>) -> Self {
        Self { id, registry }
    }

    pub(crate) fn get_type(&self) -> Result<Type, crate::Error> {
        let type_ = self.registry.get(&self.id);
        if let Some(type_) = type_ {
            Ok(type_.value().clone())
        } else {
            Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Unknown type: {}", self.id),
            ))
        }
    }
}

impl Object for Reference {
    fn get_attribute(
        &self,
        name: &str,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<super::Type, crate::Error> {
        let type_ = self.registry.get(&self.id);
        if let Some(type_) = type_ {
            type_.value().get_attribute(name, listener)
        } else {
            Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Unknown type: {}", self.id),
            ))
        }
    }

    fn call(
        &self,
        positional_args: &[super::Type],
        kwargs: &std::collections::BTreeMap<String, super::Type>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<super::Type, crate::Error> {
        let type_ = self.registry.get(&self.id);
        if let Some(type_) = type_ {
            type_.value().call(positional_args, kwargs, listener)
        } else {
            Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Unknown type: {}", self.id),
            ))
        }
    }

    fn subscript(
        &self,
        index: &super::Type,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<super::Type, crate::Error> {
        let type_ = self.registry.get(&self.id);
        if let Some(type_) = type_ {
            type_.value().subscript(index, listener)
        } else {
            Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Unknown type: {}", self.id),
            ))
        }
    }
}
