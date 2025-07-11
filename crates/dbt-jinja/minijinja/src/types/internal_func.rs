use crate::types::builtin::Type;
use crate::types::function::FunctionType;

#[derive(Clone, Debug, Default)]
pub struct InternalCaller {}

impl FunctionType for InternalCaller {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if !actual_arguments.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "The 'caller' function does not accept any arguments.".to_string(),
            ));
        }
        Ok(Type::String)
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}
