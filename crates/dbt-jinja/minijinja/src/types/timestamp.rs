use crate::types::{
    builtin::Type,
    function::{ArgSpec, FunctionType},
};

#[derive(Default, Clone, Eq, PartialEq)]
pub struct PyDateTimeStrftimeFunction {}

impl std::fmt::Debug for PyDateTimeStrftimeFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("timestamp.strftime")
    }
}

impl FunctionType for PyDateTimeStrftimeFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!("Expected 1 argument, got {}", args.len()),
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!("Expected string, got {}", args[0]),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("format", false)]
    }
}
