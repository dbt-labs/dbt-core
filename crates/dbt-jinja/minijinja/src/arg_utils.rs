use std::collections::BTreeMap;

use crate::{
    value::{value_map_with_capacity, Kwargs, ValueMap},
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, Value,
};

/// Report invalid argument error (returns a MinijinjaError)
#[macro_export]
macro_rules! invalid_argument_inner {
    ($msg:expr) => {
        MinijinjaError::new(MinijinjaErrorKind::InvalidArgument, $msg)
    };

    ($($arg:tt)*) => {
        MinijinjaError::new(MinijinjaErrorKind::InvalidArgument, format!($($arg)*))
    };
}

/// Report invalid argument error (returns an Err wrapped MinijinjaError)
#[macro_export]
macro_rules! invalid_argument {
    ($msg:expr) => {
        Err(invalid_argument_inner!($msg))
    };

    ($($arg:tt)*) => {
        Err(invalid_argument_inner!($($arg)*))
    };
}

/// Report missing argument error
#[macro_export]
macro_rules! missing_argument {
    ($msg:expr) => {
        Err(MinijinjaError::new(MinijinjaErrorKind::MissingArgument, $msg))
    };

    ($($arg:tt)*) => {
        Err(MinijinjaError::new(MinijinjaErrorKind::MissingArgument, format!($($arg)*)))
    };
}

/// Report too many arguments error
#[macro_export]
macro_rules! too_many_arguments {
    ($msg:expr) => {
        Err(MinijinjaError::new(MinijinjaErrorKind::TooManyArguments, $msg))
    };

    ($($arg:tt)*) => {
        Err(MinijinjaError::new(MinijinjaErrorKind::TooManyArguments, format!($($arg)*)))
    };
}

/// Util function to check that the number of arguments known to the
/// parser is within the given bounds.
pub fn check_num_args(
    func_name: impl Into<String>,
    parser: &ArgParser,
    min: usize,
    max: usize,
) -> Result<(), MinijinjaError> {
    let num_args = parser.positional_len() + parser.kwargs_len();

    if num_args < min {
        missing_argument!(format!(
            "{} requires {}..{} argument(s)",
            func_name.into(),
            min,
            max
        ))
    } else if num_args > max {
        too_many_arguments!(format!(
            "{} requires {}..{} argument(s)",
            func_name.into(),
            min,
            max
        ))
    } else {
        Ok(())
    }
}

/// Struct for parsing arguments for Jinja macros
#[derive(Debug)]
pub struct ArgParser {
    positional: Vec<Value>,
    kwargs: BTreeMap<String, Value>,
}

/// Implementation of the ArgParser struct
impl ArgParser {
    /// Create a new ArgParser instance that handles both mixed args and separate kwargs
    pub fn new(args: &[Value], kwargs: Option<Kwargs>) -> Self {
        let mut parser = ArgParser {
            positional: Vec::new(),
            kwargs: BTreeMap::new(),
        };

        // First process any kwargs provided separately
        if let Some(kw) = kwargs {
            for key in kw.args() {
                parser
                    .kwargs
                    .insert(key.to_string(), kw.get::<Value>(key).unwrap().clone());
            }
        }

        // Then process args, which might contain more kwargs
        for arg in args.iter() {
            if arg.is_kwargs() {
                if let Some(map) = arg.as_object() {
                    if let Some(iter) = map.try_iter() {
                        for key in iter {
                            if let Some(value) = map.get_value(&key) {
                                // Later kwargs override earlier ones
                                parser.kwargs.insert(
                                    key.as_str().unwrap_or_default().to_string(),
                                    value.clone(),
                                );
                            }
                        }
                    }
                }
            } else {
                parser.positional.push(arg.clone());
            }
        }

        parser
    }

    /// Get and consume a value by name or next positional argument
    pub fn get<T>(&mut self, name: &str) -> Result<T, MinijinjaError>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        // First check kwargs
        if let Some(value) = self.kwargs.remove(name) {
            return T::try_from(value).map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Failed to convert argument '{name}': {e}"),
                )
            });
        }

        // Then take the next positional argument
        // TODO: this try_from conversion is troublesome; we need to verify value.is_none() and return an Option accordingly
        // For example if None is used in the template,
        // this method returns "none" string literal if it's invoked as get<String>(...)
        if let Some(value) = self.positional.first().cloned() {
            self.positional.remove(0);
            return T::try_from(value).map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Failed to convert next positional argument: {e}"),
                )
            });
        }

        Err(MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            format!("Required argument '{name}' not provided"),
        ))
    }

    /// Get and consume the next positional argument
    pub fn next_positional<T>(&mut self) -> Result<T, MinijinjaError>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        if let Some(value) = self.positional.first().cloned() {
            self.positional.remove(0);
            return T::try_from(value).map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Failed to convert next positional argument: {e}"),
                )
            });
        }
        Err(MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            "No positional arguments left".to_string(),
        ))
    }

    /// Get and consume an optional value by name or next positional argument
    pub fn get_optional<T>(&mut self, name: &str) -> Option<T>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        self.get(name).ok()
    }

    /// Get and consume a value by either of two names or next positional argument
    pub fn get_either<T>(&mut self, name1: &str, name2: &str) -> Result<T, MinijinjaError>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        // First check kwargs for name1
        if let Some(value) = self.kwargs.remove(name1) {
            return T::try_from(value).map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Failed to convert argument '{name1}': {e}"),
                )
            });
        }

        // Then check kwargs for name2
        if let Some(value) = self.kwargs.remove(name2) {
            return T::try_from(value).map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Failed to convert argument '{name2}': {e}"),
                )
            });
        }

        // Finally check positional args
        if let Some(value) = self.positional.first().cloned() {
            self.positional.remove(0);
            return T::try_from(value).map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Failed to convert next positional argument: {e}"),
                )
            });
        }

        Err(MinijinjaError::new(
            MinijinjaErrorKind::InvalidOperation,
            format!("Required argument '{name1}' or '{name2}' not provided"),
        ))
    }

    /// Get and consume an optional value by either of two names or next positional argument
    pub fn get_optional_either<T>(&mut self, name1: &str, name2: &str) -> Option<T>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        self.get_either(name1, name2).ok()
    }

    /// Check if a named argument exists
    pub fn has_kwarg(&self, name: &str) -> bool {
        self.kwargs.contains_key(name)
    }

    /// Get the number of positional arguments
    pub fn positional_len(&self) -> usize {
        self.positional.len()
    }

    /// Get all kwargs
    pub fn drain_kwargs(&mut self) -> BTreeMap<String, Value> {
        // Drain the kwargs and return them from BTreeMap
        let mut drained = BTreeMap::new();
        std::mem::swap(&mut self.kwargs, &mut drained);
        drained
    }

    /// iterate over all of the kwargs
    pub fn kwargs_iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.kwargs.iter()
    }

    /// number of kwargs
    pub fn kwargs_len(&self) -> usize {
        self.kwargs.len()
    }

    /// Return the remaining positional arguments
    pub fn get_args_as_vec_of_values(&self) -> Vec<Value> {
        self.positional.clone()
    }

    /// Return the remaining kwargs as a ValueMap
    pub fn get_kwargs_as_value_map(&self) -> ValueMap {
        let mut value_map = value_map_with_capacity(self.kwargs.len());
        for (key, value) in self.kwargs.clone() {
            value_map.insert(Value::from(key), value);
        }
        value_map
    }

    /// Get and consume an optional value by name from kwargs
    pub fn consume_optional_only_from_kwargs<T>(&mut self, name: &str) -> Option<T>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        // Then check kwargs for name
        if let Some(value) = self.kwargs.remove(name) {
            return if !value.is_none() {
                T::try_from(value).ok()
            } else {
                None
            };
        }
        None
    }

    /// Get and consume an optional value by either of two names from kwargs
    pub fn consume_optional_either_from_kwargs<T>(&mut self, name1: &str, name2: &str) -> Option<T>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        self.consume_optional_only_from_kwargs(name1)
            .or_else(|| self.consume_optional_only_from_kwargs(name2))
    }

    /// Validate the number of arguments for a method call
    pub fn check_num_args(
        &self,
        func_name: impl Into<String>,
        min: usize,
        max: usize,
    ) -> Result<(), MinijinjaError> {
        let num_args = self.positional_len() + self.kwargs_len();
        let err_msg = format!("{} requires {}..{} argument(s)", func_name.into(), min, max);

        // TODO: migrate Jinja Error related macros from dbt-adapter/src/macros.rs here
        if num_args < min {
            Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidArgument,
                err_msg,
            ))
        } else if num_args > max {
            Err(MinijinjaError::new(
                MinijinjaErrorKind::TooManyArguments,
                err_msg,
            ))
        } else {
            Ok(())
        }
    }

    /// Assert that all arguments have been consumed
    pub fn assert_all_used(&self) -> Result<(), MinijinjaError> {
        if self.positional_len() > 0 || !self.kwargs.is_empty() {
            Err(MinijinjaError::new(
                MinijinjaErrorKind::TooManyArguments,
                "Too many positional arguments".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

/// A zero-copy, streaming parser of arguments from a `&[Value]` slice.
///
/// NOTE(felipecrv): this is experimental and not part of the original minijinja
/// codebase. It may change in the future.
pub struct ArgsIter<'a> {
    fn_name: &'a str,
    min_pos_args: usize,
    args: &'a [Value],
    index: usize,
    kwargs: Option<&'a Value>,
}

impl<'a> ArgsIter<'a> {
    /// Create a new `ArgsIter` from a slice of `Value`s.
    ///
    /// PRECONDITION: if one of the args `.is_kwargs()`, it must be the last argument in the slice.
    pub fn new(fn_name: &'a str, min_pos_args: usize, args: &'a [Value]) -> ArgsIter<'a> {
        ArgsIter {
            fn_name,
            min_pos_args,
            args,
            index: 0,
            kwargs: None,
        }
    }

    /// Number of positional arguments provided to the iterator.
    pub fn num_pos_args(&self) -> usize {
        let has_kwargs = self.kwargs.is_some() || self.args.last().map_or(false, |v| v.is_kwargs());
        self.args.len() - if has_kwargs { 1 } else { 0 }
    }

    /// Ensure all positional arguments have been consumed and return any trailing kwargs.
    ///
    /// Call this instead of `finish()` if you expect trailing kwargs.
    pub fn trailing_kwargs(&mut self) -> Result<Kwargs, MinijinjaError> {
        if let Some(value) = self.kwargs {
            // If kwargs is set, all positional arguments have been consumed.
            // .unwrap() is safe here because we know `value.is_kwargs()` is true.
            return Ok(Kwargs::extract(value).unwrap());
        }
        if let Some(arg) = self._peek() {
            if let Some(kwargs) = Kwargs::extract(arg) {
                self._next(); // consume the peeked() kwargs argument
                self.kwargs.replace(arg);
                Ok(kwargs)
            } else {
                let max_pos_args = self.index;
                Err(self._unexpected_positional_arg(max_pos_args))
            }
        } else {
            Ok(Kwargs::default())
        }
    }

    /// Ensure all positional arguments have been consumed when no kwargs are expected.
    pub fn finish(&self) -> Result<(), MinijinjaError> {
        if let Some(arg) = self._peek() {
            let err = if arg.is_kwargs() {
                self._unexpected_kwargs(arg)
            } else {
                // The user called finish() now, so the current iterator position
                // is the expected maximum number of positional arguments.
                let max_pos_args = self.index;
                self._unexpected_positional_arg(max_pos_args)
            };
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Handle unexpected kwargs.
    ///
    /// PRECONDITION: arg.is_kwargs()
    #[inline(never)]
    fn _unexpected_kwargs(&self, arg: &Value) -> MinijinjaError {
        // Like Python, we produce a message that lets the user
        // easily identify the unexpected keyword argument.
        let kwargs = arg.as_object().unwrap(); // arg.is_kwargs()
        let mut iter = kwargs.try_iter().unwrap(); // Kwargs is iterable
        let key = iter.next().unwrap(); // Kwargs must have at least one key
        let msg = format!(
            "{}() got an unexpected keyword argument '{}'",
            self.fn_name,
            key.as_str().unwrap()
        );
        MinijinjaError::new(MinijinjaErrorKind::TooManyArguments, msg)
    }

    #[inline(never)]
    fn _unexpected_positional_arg(&self, max_pos_args: usize) -> MinijinjaError {
        // handle the unexpected positional argument case
        debug_assert!(
            max_pos_args >= self.min_pos_args,
            "trailing_kwargs() or finish() called before min_pos_args={} arguments \
were consumed from the iterator. You are misusing the ArgsIter API.",
            self.min_pos_args
        );

        let num_pos_args = self.num_pos_args();
        let msg = if self.min_pos_args == max_pos_args {
            format!(
                "{}() takes {} positional argument but {} were given",
                self.fn_name, self.min_pos_args, num_pos_args
            )
        } else {
            format!(
                "{}() takes from {} to {} positional arguments but {} were given",
                self.fn_name, self.min_pos_args, max_pos_args, num_pos_args
            )
        };
        MinijinjaError::new(MinijinjaErrorKind::TooManyArguments, msg)
    }

    fn _peek(&self) -> Option<&'a Value> {
        if self.index < self.args.len() {
            Some(&self.args[self.index])
        } else {
            None
        }
    }

    fn _next(&mut self) -> Option<&'a Value> {
        if self.index < self.args.len() {
            let arg = &self.args[self.index];
            self.index += 1;
            Some(arg)
        } else {
            None
        }
    }

    #[inline(never)]
    fn _did_consume_all_positional_args(&self) -> Result<(), MinijinjaError> {
        let num_pos_args = self.num_pos_args();
        if num_pos_args < self.min_pos_args {
            let msg = format!(
                "{}() requires at least {} positional arguments but only {} were given",
                self.fn_name,
                self.min_pos_args,
                num_pos_args
            );
            let err = MinijinjaError::new(MinijinjaErrorKind::MissingArgument, msg);
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl<'a> Iterator for ArgsIter<'a> {
    type Item = Result<&'a Value, MinijinjaError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self._next() {
            Some(arg) => {
                if arg.is_kwargs() {
                    self.kwargs.replace(arg);
                    // fallthrough
                } else {
                    return Some(Ok(arg));
                }
            }
            None => (), // fallthrough
        };
        match self._did_consume_all_positional_args() {
            Ok(()) => None,             // finish iteration
            Err(err) => Some(Err(err)), // generate errors infinitely
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_empty_args() {
        let parser = ArgParser::new(&[], None);
        assert!(parser.positional.is_empty());
        assert!(parser.kwargs.is_empty());
    }

    #[test]
    fn test_positional_only() {
        let args = vec![Value::from("first"), Value::from("second")];
        let mut parser = ArgParser::new(&args, None);

        assert_eq!(parser.get::<String>("arg0").unwrap(), "first");
        assert_eq!(parser.get::<String>("arg1").unwrap(), "second");
        assert!(parser.kwargs.is_empty());
    }

    #[test]
    fn test_kwargs_only() {
        let args = vec![Value::from(Kwargs::from_iter([
            ("name", Value::from("test")),
            ("namespace", Value::from("test_ns")),
        ]))];
        let mut parser = ArgParser::new(&args, None);

        assert_eq!(parser.get::<String>("name").unwrap(), "test");
        assert_eq!(parser.get::<String>("namespace").unwrap(), "test_ns");
        assert!(parser.positional.is_empty());
    }

    #[test]
    fn test_kwargs_only_reversed() {
        let args = vec![Value::from(Kwargs::from_iter([
            ("namespace", Value::from("test_ns")),
            ("name", Value::from("test")),
        ]))];
        let mut parser = ArgParser::new(&args, None);

        assert_eq!(parser.get::<String>("name").unwrap(), "test");
        assert_eq!(parser.get::<String>("namespace").unwrap(), "test_ns");
        assert!(parser.positional.is_empty());
    }

    #[test]
    fn test_kwargs_btreemap() {
        let mut map = BTreeMap::new();
        map.insert("name".to_string(), Value::from("test"));
        map.insert("namespace".to_string(), Value::from("test_ns"));

        let args = vec![Value::from(Kwargs::from_iter(map))];
        let mut parser = ArgParser::new(&args, None);

        assert_eq!(parser.get::<String>("name").unwrap(), "test");
        assert_eq!(parser.get::<String>("namespace").unwrap(), "test_ns");
    }

    #[test]
    fn test_mixed_args() {
        let mut kwargs = BTreeMap::new();
        kwargs.insert("namespace".to_string(), Value::from("test_ns"));

        let args = vec![
            Value::from("positional"),
            Value::from(Kwargs::from_iter(kwargs)),
        ];
        let mut parser = ArgParser::new(&args, None);

        assert_eq!(parser.get::<String>("arg0").unwrap(), "positional");
        assert_eq!(parser.get::<String>("namespace").unwrap(), "test_ns");
    }

    #[test]
    fn test_optional_args() {
        let mut parser = ArgParser::new(&[], None);
        assert!(parser.get_optional::<String>("missing").is_none());

        let args = vec![Value::from("test")];
        let mut parser = ArgParser::new(&args, None);
        assert_eq!(parser.get_optional::<String>("arg0").unwrap(), "test");
        assert!(parser.get_optional::<String>("missing").is_none());
    }

    #[test]
    fn test_type_conversion() {
        let args = vec![Value::from(42), Value::from(true), Value::from("string")];
        let mut parser = ArgParser::new(&args, None);

        assert_eq!(parser.get::<i64>("num").unwrap(), 42);
        assert!(parser.get::<bool>("bool").unwrap());
        assert_eq!(parser.get::<String>("str").unwrap(), "string");
    }

    #[test]
    fn test_has_kwarg() {
        let mut kwargs = BTreeMap::new();
        kwargs.insert("test".to_string(), Value::from(true));

        let args = vec![Value::from(Kwargs::from_iter(kwargs))];
        let parser = ArgParser::new(&args, None);

        assert!(parser.has_kwarg("test"));
        assert!(!parser.has_kwarg("missing"));
    }

    #[test]
    fn test_positional_len() {
        let args = vec![Value::from(1), Value::from(2), Value::from(3)];
        let parser = ArgParser::new(&args, None);

        assert_eq!(parser.positional_len(), 3);
    }

    #[test]
    fn test_error_cases() {
        let mut parser = ArgParser::new(&[], None);

        // Missing required argument
        let err = parser.get::<String>("required").unwrap_err();
        assert!(err
            .to_string()
            .contains("Required argument 'required' not provided"));

        // Wrong type conversion
        let args = vec![Value::from("not a number")];
        let mut parser = ArgParser::new(&args, None);
        assert!(parser.get::<i64>("num").is_err());
    }

    #[test]
    fn test_kwargs_precedence() {
        let mut kwargs = BTreeMap::new();
        kwargs.insert("value".to_string(), Value::from("kwarg_value"));

        let args = vec![
            Value::from("positional_value"),
            Value::from(Kwargs::from_iter(kwargs)),
        ];
        let mut parser = ArgParser::new(&args, None);

        // Kwargs should take precedence over positional args
        assert_eq!(parser.get::<String>("value").unwrap(), "kwarg_value");
    }

    #[test]
    fn test_multiple_kwargs() {
        let mut kwargs1 = BTreeMap::new();
        kwargs1.insert("first".to_string(), Value::from("value1"));

        let mut kwargs2 = BTreeMap::new();
        kwargs2.insert("second".to_string(), Value::from("value2"));

        let args = vec![
            Value::from(Kwargs::from_iter(kwargs1)),
            Value::from(Kwargs::from_iter(kwargs2)),
        ];
        let mut parser = ArgParser::new(&args, None);

        assert_eq!(parser.get::<String>("first").unwrap(), "value1");
        assert_eq!(parser.get::<String>("second").unwrap(), "value2");
    }

    #[test]
    fn test_get_either() {
        let mut kwargs = BTreeMap::new();
        kwargs.insert("alt_name".to_string(), Value::from("test_value"));

        let args = vec![Value::from(Kwargs::from_iter(kwargs))];
        let mut parser = ArgParser::new(&args, None);

        // Should find value under alternative name
        assert_eq!(
            parser.get_either::<String>("name", "alt_name").unwrap(),
            "test_value"
        );
    }

    #[test]
    fn test_get_optional_either() {
        let mut kwargs = BTreeMap::new();
        kwargs.insert("v".to_string(), Value::from("1.0.0"));

        let args = vec![Value::from(Kwargs::from_iter(kwargs))];
        let mut parser = ArgParser::new(&args, None);

        // Should find value under alternative name
        assert_eq!(
            parser
                .get_optional_either::<String>("version", "v")
                .unwrap(),
            "1.0.0"
        );

        // Should return None when neither name exists
        assert!(parser
            .get_optional_either::<String>("missing1", "missing2")
            .is_none());
    }
}
