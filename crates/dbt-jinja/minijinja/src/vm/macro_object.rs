use std::fmt;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use crate::arg_utils::ArgParser;
use crate::error::{Error, ErrorKind};
use crate::listener::RenderingEventListener;
use crate::machinery::Span;
use crate::output::Output;
use crate::output_tracker;
use crate::utils::AutoEscape;
use crate::value::mutable_map::MutableMap;
use crate::value::{Enumerator, Kwargs, Object, Value, ValueMap};
use crate::vm::state::State;
use crate::vm::Vm;

pub(crate) struct Macro {
    pub name: Value,
    pub arg_spec: Vec<Value>,
    // because values need to be 'static, we can't hold a reference to the
    // instructions that declared the macro.  Instead of that we place the
    // reference to the macro instruction (and the jump offset) in the
    // state under `state.macros`.
    pub macro_ref_id: usize,
    pub state_id: isize,
    pub closure: Value,
    pub caller_reference: bool,
    pub path: PathBuf,
    pub span: Span,
}

impl fmt::Debug for Macro {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<macro {}>", self.name)
    }
}

impl Object for Macro {
    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&["name", "arguments", "caller"])
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        Some(match some!(key.as_str()) {
            "name" => self.name.clone(),
            "arguments" => Value::from_iter(self.arg_spec.iter().cloned()),
            "caller" => Value::from(self.caller_reference),
            _ => return None,
        })
    }

    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        // we can only call macros that point to loaded template state.
        if state.id != self.state_id {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "cannot call this macro. template state went away.",
            ));
        }
        let base_context = state.get_base_context_with_path_and_span(
            &Value::from(self.path.to_string_lossy()),
            &Value::from_serialize(self.span),
        );
        let base_ctx = base_context
            .as_object()
            .unwrap()
            .downcast_ref::<MutableMap>()
            .unwrap();
        let base_ctx = base_ctx.clone();

        let parsed_args = parse_macro_arguments_with_spec(args, None, &self.arg_spec)?;
        let arg_values = parsed_args.arg_values;
        let extra_args = parsed_args.extra_args;
        let extra_kwargs = parsed_args.extra_kwargs;

        let caller = if self.caller_reference {
            Some(
                extra_kwargs
                    .get(&Value::from("caller"))
                    .unwrap_or(&Value::UNDEFINED)
                    .clone(),
            )
        } else {
            // Check if caller was provided but not expected
            if extra_kwargs.contains_key(&Value::from("caller")) {
                return Err(Error::new(
                    ErrorKind::TooManyArguments,
                    "macro does not accept caller argument",
                ));
            }
            None
        };

        let (instructions, offset) = &state.macros[self.macro_ref_id];
        let vm = Vm::new(state.env());
        let mut rv = String::new();
        let mut output_tracker = output_tracker::OutputTracker::new(&mut rv);
        let current_location = output_tracker.location.clone();
        let mut out = Output::with_write(&mut output_tracker);

        // This requires some explanation here.  Because we get the state as &State and
        // not &mut State we are required to create a new state here.  This is unfortunate
        // but makes the calling interface more convenient for the rest of the system.
        // Because macros cannot return anything other than strings (most importantly they)
        // can't return other macros this is however not an issue, as modifications in the
        // macro cannot leak out.
        ok!(vm.eval_macro(
            instructions,
            *offset,
            self.closure.clone(),
            Value::from_object(base_ctx),
            caller,
            extra_args,
            extra_kwargs,
            &mut out,
            current_location,
            state,
            arg_values,
            listeners
        ));

        Ok(if !matches!(state.auto_escape(), AutoEscape::None) {
            Value::from_safe_string(rv)
        } else {
            Value::from(rv)
        })
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<macro {}>", self.name)
    }
}

/// Struct for parsed macro template arguments given a macro arg spec
#[derive(Debug)]
pub struct ParsedArgs {
    /// The required args spread out
    pub arg_values: Vec<Value>,
    /// The extra args
    pub extra_args: Vec<Value>,
    /// The extra kwargs
    pub extra_kwargs: ValueMap,
}

/// Parse args given a macro arg spec and return the parsed args and any extra args or kwargs
pub fn parse_macro_arguments_with_spec(
    args: &[Value],
    kwargs: Option<Kwargs>,
    arg_spec: &[Value],
) -> Result<ParsedArgs, Error> {
    let mut parser = ArgParser::new(args, kwargs);
    let mut arg_values = Vec::with_capacity(arg_spec.len());
    let mut used_params = std::collections::HashSet::new();

    for name in arg_spec {
        let name = name.as_str().unwrap_or_default();

        // Check if this parameter was already used as a positional argument
        if parser.has_kwarg(name) && parser.positional_len() > 0 {
            return Err(Error::new(
                ErrorKind::TooManyArguments,
                format!("duplicate argument `{name}`"),
            ));
        }

        if let Ok(value) = parser.get::<Value>(name) {
            if !used_params.insert(name) {
                return Err(Error::new(
                    ErrorKind::TooManyArguments,
                    format!("duplicate argument `{name}`"),
                ));
            }
            arg_values.push(value);
        } else {
            arg_values.push(Value::UNDEFINED);
        }
    }

    Ok(ParsedArgs {
        arg_values,
        extra_args: parser.get_args_as_vec_of_values(),
        extra_kwargs: parser.get_kwargs_as_value_map(),
    })
}
