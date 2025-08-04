use crate::types::iterable::IterableType;
use crate::types::list::ListType;
use crate::types::utils::CodeLocation;
use crate::types::{Object, Type};
use crate::TypecheckingEventListener;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ArgSpec {
    pub name: String,
    pub is_optional: bool,
}

impl From<Argument> for ArgSpec {
    fn from(arg: Argument) -> Self {
        Self {
            name: arg.name,
            is_optional: arg.is_optional,
        }
    }
}
impl ArgSpec {
    pub fn new(name: &str, is_optional: bool) -> Self {
        Self {
            name: name.to_string(),
            is_optional,
        }
    }
}

pub trait FunctionType: Object + Send + Sync + std::fmt::Debug {
    fn resolve_arguments(
        &self,
        positional_args: &[Type],
        kwargs: &BTreeMap<String, Type>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        let mut args = vec![];
        let mut kwargs = kwargs.clone();

        for (i, spec) in self.arg_specs().iter().enumerate() {
            if i < positional_args.len() {
                let name = spec.name.clone();
                if kwargs.contains_key(&name) {
                    listener.warn(&format!("Duplicate argument: {name}"));
                    return Ok(Type::Any { hard: false });
                }
                args.push(positional_args[i].clone());
            } else if let Some(value) = kwargs.get(&spec.name) {
                args.push(value.clone());
                kwargs.remove(&spec.name);
            } else if spec.is_optional {
                args.push(Type::None);
            } else {
                listener.warn(&format!("Missing required argument: {}", spec.name));
                return Ok(Type::Any { hard: false });
            }
        }
        // caller is a special argument, it is not in the arg_specs
        kwargs.remove("caller");
        if !kwargs.is_empty() {
            listener.warn(&format!("Unknown arguments: {:?}", kwargs.keys()));
            return Ok(Type::Any { hard: false });
        }
        self._resolve_arguments(&args, listener)
    }

    fn arg_specs(&self) -> Vec<ArgSpec>;

    fn _resolve_arguments(
        &self,
        actual_arguments: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error>;
}

impl<T: FunctionType> Object for T {
    fn get_attribute(
        &self,
        name: &str,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        listener.warn(&format!("Attribute {name} not found"));
        Ok(Type::Any { hard: false })
    }

    fn call(
        &self,
        positional_args: &[Type],
        kwargs: &BTreeMap<String, Type>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        self.resolve_arguments(positional_args, kwargs, listener)
    }

    fn subscript(
        &self,
        _index: &Type,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        listener.warn("Subscript not supported for function type");
        Ok(Type::Any { hard: false })
    }
}

#[derive(Clone)]
pub struct Argument {
    pub name: String,
    pub type_: Type,
    pub is_optional: bool,
}

#[derive(Clone)]
pub struct LambdaType {
    pub args: Vec<Type>,
    pub ret_type: Type,
}

impl LambdaType {
    pub fn new(args: Vec<Type>, ret_type: Type) -> Self {
        Self { args, ret_type }
    }
}

impl std::fmt::Debug for LambdaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LambdaType({:?}, {:?})", self.args, self.ret_type)
    }
}

impl FunctionType for LambdaType {
    fn _resolve_arguments(
        &self,
        actual_arguments: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        if self.args.len() != actual_arguments.len() {
            listener.warn(&format!(
                "Expected {} arguments, got {}",
                self.args.len(),
                actual_arguments.len()
            ));
        }
        for (arg, actual_arg) in self.args.iter().zip(actual_arguments.iter()) {
            if !actual_arg.is_subtype_of(arg) {
                listener.warn(&format!("Expected {arg:?}, got {actual_arg:?}"));
            }
        }
        Ok(self.ret_type.clone())
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        self.args
            .iter()
            .enumerate()
            .map(|(i, _)| ArgSpec::new(&format!("arg{i}"), false))
            .collect()
    }
}

impl From<UserDefinedFunctionType> for LambdaType {
    fn from(value: UserDefinedFunctionType) -> Self {
        Self {
            args: value.args.iter().map(|arg| arg.type_.clone()).collect(),
            ret_type: value.ret_type.clone(),
        }
    }
}

#[derive(Clone)]
pub struct UserDefinedFunctionType {
    pub name: String,
    pub args: Vec<Argument>,
    pub ret_type: Type,
}

impl fmt::Debug for UserDefinedFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl UserDefinedFunctionType {
    pub fn new(name: &str, args: Vec<Argument>, ret_type: Type) -> Self {
        Self {
            name: name.to_string(),
            args,
            ret_type,
        }
    }
}

impl FunctionType for UserDefinedFunctionType {
    fn _resolve_arguments(
        &self,
        actual_arguments: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        // match the actual arguments with the expected arguments, if matches return Ok else Err
        if self.args.len() != actual_arguments.len() {
            listener.warn(&format!(
                "Argument number mismatch: expected {}, got {}",
                self.args.len(),
                actual_arguments.len()
            ));
        } else {
            for (i, (expected, actual)) in self.args.iter().zip(actual_arguments).enumerate() {
                if !actual.is_subtype_of(&expected.type_) {
                    listener.warn(&format!(
                        "Argument type mismatch: expected {:?}, got {actual:?}, at index {i}",
                        expected.type_,
                    ));
                }
            }
        }
        Ok(self.ret_type.clone())
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        self.args.iter().map(|arg| arg.clone().into()).collect()
    }
}

pub struct UndefinedFunctionType {
    pub name: String,
    pub location: CodeLocation,
}

impl fmt::Debug for UndefinedFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name.to_string())
    }
}

impl UndefinedFunctionType {
    pub fn new(name: &str, location: CodeLocation) -> Self {
        Self {
            name: name.to_string(),
            location,
        }
    }
}

impl FunctionType for UndefinedFunctionType {
    fn _resolve_arguments(
        &self,
        _actual_arguments: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        listener.warn(&format!(
            "Function {} @ {} is not defined",
            self.name, self.location
        ));
        Ok(Type::Any { hard: false })
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct MapFunctionType {}

impl FunctionType for MapFunctionType {
    fn _resolve_arguments(
        &self,
        actual_arguments: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        if let Type::String(Some(key)) = &actual_arguments[1] {
            let element = match &actual_arguments[0] {
                Type::List(ListType { element }) | Type::Iterable(IterableType { element }) => {
                    element.get_attribute(key.as_str(), listener)
                }
                Type::Any { hard: true } => Ok(Type::Any { hard: true }),
                _ => {
                    listener.warn(&format!(
                        "map requires a list or iterable argument as the first argument, got {:?}",
                        actual_arguments[0]
                    ));
                    return Ok(Type::Any { hard: false });
                }
            }?;
            Ok(Type::Iterable(IterableType::new(element)))
        } else if matches!(actual_arguments[1], Type::Any { hard: true }) {
            Ok(Type::Any { hard: true })
        } else {
            listener.warn(&format!(
                "map requires a literal string argument as the second argument, got {:?}",
                actual_arguments[1]
            ));
            Ok(Type::Any { hard: false })
        }
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::new("iterable", false),
            ArgSpec::new("attribute", false),
        ]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct ListFunctionType;

impl FunctionType for ListFunctionType {
    fn _resolve_arguments(
        &self,
        actual_arguments: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 1 {
            listener.warn("list requires exactly 1 argument");
            return Ok(Type::Any { hard: false });
        }
        let element = match &actual_arguments[0] {
            Type::List(ListType { element }) | Type::Iterable(IterableType { element }) => element,
            Type::Any { hard: true } => {
                return Ok(Type::List(ListType::new(Type::Any { hard: true })))
            }
            _ => {
                listener.warn(&format!(
                    "list requires a list or iterable argument, got {:?}",
                    actual_arguments[0]
                ));
                return Ok(Type::Any { hard: false });
            }
        };
        Ok(Type::List(ListType::new(*element.clone())))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("iterable", false)]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct TryOrCompilerErrorFunctionType;

impl fmt::Debug for TryOrCompilerErrorFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "try_or_compiler_error")
    }
}

impl FunctionType for TryOrCompilerErrorFunctionType {
    fn _resolve_arguments(
        &self,
        args: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        if args.len() <= 2 {
            listener.warn("Expected at least 2 arguments for try_or_compiler_error function");
            return Ok(Type::Any { hard: false });
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            listener.warn("Expected a string argument for try_or_compiler_error function");
            return Ok(Type::Any { hard: false });
        }
        if let Type::Object(_func) = &args[1] {
            // It is not possible to resolve the arguments of the function,
            // because the function args are not known.
            // let rest_args = args[2..].to_vec();
            // func.resolve_arguments(&rest_args)
        } else {
            listener.warn(&format!(
                "Expected a function argument for try_or_compiler_error function, got {:?}",
                args[1]
            ));
            return Ok(Type::Any { hard: false });
        }
        Ok(Type::Any { hard: true })
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::new("message_if_exception", false),
            ArgSpec::new("func", false),
            ArgSpec::new("args", false), // TODO: arg number depends on the function
        ]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct SelectAttrFunctionType;

impl fmt::Debug for SelectAttrFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("selectattr")
    }
}

impl FunctionType for SelectAttrFunctionType {
    fn _resolve_arguments(
        &self,
        args: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        if !args[0].is_subtype_of(&Type::List(ListType::new(Type::Any { hard: true }))) {
            listener.warn(&format!(
                "Expected a list argument for selectattr function, got {:?}",
                args[0]
            ));
            return Ok(Type::Any { hard: false });
        }
        if !args[1].is_subtype_of(&Type::String(None)) {
            listener.warn(&format!(
                "Expected a string argument for selectattr function, got {:?}",
                args[1]
            ));
            return Ok(Type::Any { hard: false });
        }
        if !args[2].is_subtype_of(&Type::String(None)) {
            listener.warn(&format!(
                "Expected a string argument for selectattr function, got {:?}",
                args[2]
            ));
            return Ok(Type::Any { hard: false });
        }
        // TODO check the args[3] based on the op

        Ok(args[0].clone())
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::new("list", false),
            ArgSpec::new("name", false),
            ArgSpec::new("op", false),
            ArgSpec::new("inside_transaction", true),
        ]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct RejectAttrFunctionType;

impl fmt::Debug for RejectAttrFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("rejectattr")
    }
}

impl FunctionType for RejectAttrFunctionType {
    fn _resolve_arguments(
        &self,
        args: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        if !args[0].is_subtype_of(&Type::List(ListType::new(Type::Any { hard: true }))) {
            listener.warn(&format!(
                "Expected a list argument for rejectattr function, got {:?}",
                args[0]
            ));
            return Ok(Type::Any { hard: false });
        }
        if !args[1].is_subtype_of(&Type::String(None)) {
            listener.warn(&format!(
                "Expected a string argument for rejectattr function, got {:?}",
                args[1]
            ));
            return Ok(Type::Any { hard: false });
        }
        if !args[2].is_subtype_of(&Type::String(None)) {
            listener.warn(&format!(
                "Expected a string argument for rejectattr function, got {:?}",
                args[2]
            ));
            return Ok(Type::Any { hard: false });
        }
        // TODO check the args[3] based on the op

        Ok(args[0].clone())
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::new("list", false),
            ArgSpec::new("name", false),
            ArgSpec::new("op", false),
            ArgSpec::new("inside_transaction", true),
        ]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct PrintFunctionType;

impl fmt::Debug for PrintFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("print")
    }
}

impl FunctionType for PrintFunctionType {
    fn _resolve_arguments(
        &self,
        args: &[Type],
        _listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        println!("print: {args:?}");
        Ok(Type::None)
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("value", false)]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct FirstFunctionType;

impl fmt::Debug for FirstFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("first")
    }
}

impl FunctionType for FirstFunctionType {
    fn _resolve_arguments(
        &self,
        args: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        match &args[0] {
            Type::List(ListType { element, .. }) => Ok(element.as_ref().clone()),
            _ => {
                listener.warn("Expected a list argument for first function");
                Ok(Type::Any { hard: false })
            }
        }
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("iterable", false)]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct BatchFunctionType;

impl fmt::Debug for BatchFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("batch")
    }
}

impl FunctionType for BatchFunctionType {
    fn _resolve_arguments(
        &self,
        args: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        if !args[1].is_subtype_of(&Type::Integer(None)) {
            listener.warn("Expected an integer argument for batch function");
        }

        match &args[0] {
            Type::List(ListType { element }) => Ok(Type::List(ListType::new(Type::List(
                ListType::new(*element.clone()),
            )))),
            Type::Iterable(IterableType { element }) => Ok(Type::List(ListType::new(Type::List(
                ListType::new(*element.clone()),
            )))),
            Type::Any { hard: true } => Ok(Type::Any { hard: true }),
            _ => {
                listener.warn("Expected a list or iterable argument for batch function");
                Ok(Type::Any { hard: false })
            }
        }
    }
    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::new("value", false),
            ArgSpec::new("count", false),
            ArgSpec::new("fill_with", true),
        ]
    }
}
