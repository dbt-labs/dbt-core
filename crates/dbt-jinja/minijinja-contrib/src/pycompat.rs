use std::collections::BTreeSet;

use minijinja::tuple;
use minijinja::value::mutable_vec::MutableVec;
use minijinja::value::{from_args, ValueKind};
use minijinja::{Error, ErrorKind, State, Value};
use regex::Regex;

/// An unknown method callback implementing python methods on primitives.
///
/// This implements a lot of Python methods on basic types so that the
/// compatibility with Jinja2 templates improves.
///
/// ```
/// use minijinja::Environment;
/// use minijinja_contrib::pycompat::unknown_method_callback;
///
/// let mut env = Environment::new();
/// env.set_unknown_method_callback(unknown_method_callback);
/// ```
///
/// Today the following methods are implemented:
///
/// * `dict.get`
/// * `dict.items`
/// * `dict.keys`
/// * `dict.values`
/// * `list.count`
/// * `list.union`
/// * `str.capitalize`
/// * `str.count`
/// * `str.endswith`
/// * `str.find`
/// * `str.isalnum`
/// * `str.isalpha`
/// * `str.isascii`
/// * `str.isdigit`
/// * `str.islower`
/// * `str.isnumeric`
/// * `str.isupper`
/// * `str.join`
/// * `str.lower`
/// * `str.lstrip`
/// * `str.replace`
/// * `str.rfind`
/// * `str.rstrip`
/// * `str.split`
/// * `str.splitlines`
/// * `str.startswith`
/// * `str.strip`
/// * `str.title`
/// * `str.upper`
#[cfg_attr(docsrs, doc(cfg(feature = "pycompat")))]
pub fn unknown_method_callback(
    _state: &State,
    value: &Value,
    method: &str,
    args: &[Value],
) -> Result<Value, Error> {
    match value.kind() {
        ValueKind::String => string_methods(value, method, args),
        ValueKind::Map => map_methods(value, method, args),
        ValueKind::Seq => seq_methods(value, method, args),
        ValueKind::Number => number_methods(value, method, args),
        _ => Err(Error::from(ErrorKind::UnknownMethod(
            format!("{}", value.kind()),
            method.to_string(),
        ))),
    }
}

#[allow(clippy::cognitive_complexity)]
fn string_methods(value: &Value, method: &str, args: &[Value]) -> Result<Value, Error> {
    let s = match value.as_str() {
        Some(s) => s,
        None => {
            return Err(Error::from(ErrorKind::UnknownMethod(
                "None".to_string(),
                method.to_string(),
            )))
        }
    };

    match method {
        "upper" => {
            let () = from_args(args)?;
            Ok(Value::from(s.to_uppercase()))
        }
        "lower" => {
            let () = from_args(args)?;
            Ok(Value::from(s.to_lowercase()))
        }
        "islower" => {
            let () = from_args(args)?;
            Ok(Value::from(s.chars().all(|x| x.is_lowercase())))
        }
        "isupper" => {
            let () = from_args(args)?;
            Ok(Value::from(s.chars().all(|x| x.is_uppercase())))
        }
        "isspace" => {
            let () = from_args(args)?;
            Ok(Value::from(s.chars().all(|x| x.is_whitespace())))
        }
        "isdigit" | "isnumeric" => {
            // this is not a perfect mapping to what Python does, but
            // close enough for most uses in templates.
            let () = from_args(args)?;
            Ok(Value::from(s.chars().all(|x| x.is_numeric())))
        }
        "isalnum" => {
            let () = from_args(args)?;
            Ok(Value::from(s.chars().all(|x| x.is_alphanumeric())))
        }
        "isalpha" => {
            let () = from_args(args)?;
            Ok(Value::from(s.chars().all(|x| x.is_alphabetic())))
        }
        "isascii" => {
            let () = from_args(args)?;
            Ok(Value::from(s.is_ascii()))
        }
        "strip" => {
            let (chars,): (Option<&str>,) = from_args(args)?;
            Ok(Value::from(if let Some(chars) = chars {
                s.trim_matches(&chars.chars().collect::<Vec<_>>()[..])
            } else {
                s.trim()
            }))
        }
        "lstrip" => {
            let (chars,): (Option<&str>,) = from_args(args)?;
            Ok(Value::from(if let Some(chars) = chars {
                s.trim_start_matches(&chars.chars().collect::<Vec<_>>()[..])
            } else {
                s.trim_start()
            }))
        }
        "rstrip" => {
            let (chars,): (Option<&str>,) = from_args(args)?;
            Ok(Value::from(if let Some(chars) = chars {
                s.trim_end_matches(&chars.chars().collect::<Vec<_>>()[..])
            } else {
                s.trim_end()
            }))
        }
        "replace" => {
            let (old, new, count): (&str, &str, Option<i32>) = from_args(args)?;
            let count = count.unwrap_or(-1);
            Ok(Value::from(if count < 0 {
                s.replace(old, new)
            } else {
                s.replacen(old, new, count as usize)
            }))
        }
        "title" => {
            let () = from_args(args)?;
            // one shall not call into these filters.  However we consider ourselves
            // privileged.
            Ok(Value::from(minijinja::filters::title(s.into())))
        }
        "split" => {
            let (sep, maxsplits) = from_args(args)?;
            // one shall not call into these filters.  However we consider ourselves
            // privileged.
            Ok(Value::from_object(MutableVec::from(
                minijinja::filters::split(s.into(), sep, maxsplits)
                    .try_iter()?
                    .collect::<Vec<Value>>(),
            )))
        }
        "splitlines" => {
            let (keepends,): (Option<bool>,) = from_args(args)?;
            if !keepends.unwrap_or(false) {
                Ok(Value::from_object(MutableVec::from(
                    s.lines().map(Value::from).collect::<Vec<Value>>(),
                )))
            } else {
                let mut rv = Vec::new();
                let mut rest = s;
                while let Some(offset) = rest.find('\n') {
                    rv.push(Value::from(&rest[..offset + 1]));
                    rest = &rest[offset + 1..];
                }
                if !rest.is_empty() {
                    rv.push(Value::from(rest));
                }
                Ok(Value::from_object(MutableVec::from(rv)))
            }
        }
        "capitalize" => {
            let () = from_args(args)?;
            // one shall not call into these filters.  However we consider ourselves
            // privileged.
            Ok(Value::from(minijinja::filters::capitalize(s.into())))
        }
        "count" => {
            let (what,): (&str,) = from_args(args)?;
            let mut c = 0;
            let mut rest = s;
            while let Some(offset) = rest.find(what) {
                c += 1;
                rest = &rest[offset + what.len()..];
            }
            Ok(Value::from(c))
        }
        "find" => {
            let (what,): (&str,) = from_args(args)?;
            Ok(Value::from(match s.find(what) {
                Some(x) => x as i64,
                None => -1,
            }))
        }
        "rfind" => {
            let (what,): (&str,) = from_args(args)?;
            Ok(Value::from(match s.rfind(what) {
                Some(x) => x as i64,
                None => -1,
            }))
        }
        "startswith" => {
            let (prefix,): (&Value,) = from_args(args)?;
            if let Some(prefix) = prefix.as_str() {
                Ok(Value::from(s.starts_with(prefix)))
            } else if matches!(prefix.kind(), ValueKind::Iterable | ValueKind::Seq) {
                for prefix in prefix.try_iter()? {
                    if s.starts_with(prefix.as_str().ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidOperation,
                            format!(
                                "tuple for startswith must contain only strings, not {}",
                                prefix.kind()
                            ),
                        )
                    })?) {
                        return Ok(Value::from(true));
                    }
                }
                Ok(Value::from(false))
            } else {
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!(
                        "startswith argument must be string or a tuple of strings, not {}",
                        prefix.kind()
                    ),
                ))
            }
        }
        "endswith" => {
            let (suffix,): (&Value,) = from_args(args)?;
            if let Some(suffix) = suffix.as_str() {
                Ok(Value::from(s.ends_with(suffix)))
            } else if matches!(suffix.kind(), ValueKind::Iterable | ValueKind::Seq) {
                for suffix in suffix.try_iter()? {
                    if s.ends_with(suffix.as_str().ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidOperation,
                            format!(
                                "tuple for endswith must contain only strings, not {}",
                                suffix.kind()
                            ),
                        )
                    })?) {
                        return Ok(Value::from(true));
                    }
                }
                Ok(Value::from(false))
            } else {
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!(
                        "endswith argument must be string or a tuple of strings, not {}",
                        suffix.kind()
                    ),
                ))
            }
        }
        "join" => {
            use std::fmt::Write;
            let (values,): (&Value,) = from_args(args)?;
            let mut rv = String::new();
            for (idx, value) in values.try_iter()?.enumerate() {
                if idx > 0 {
                    rv.push_str(s);
                }
                write!(rv, "{}", value).ok();
            }
            Ok(Value::from(rv))
        }
        "format" => {
            let args = args.to_vec();
            let mut result = s.to_string();

            // Handle numbered placeholders {0}, {1}, etc
            if Regex::new(r"\{\d+\}").unwrap().is_match(&result) {
                if result.contains("{}")
                    || Regex::new(r"\{[a-zA-Z_]\w*\}").unwrap().is_match(&result)
                {
                    return Err(Error::new(
                        ErrorKind::InvalidOperation,
                        "Cannot mix numbered placeholders with other placeholder types".to_string(),
                    ));
                }
                for (idx, value) in args.iter().enumerate() {
                    result = result.replace(&format!("{{{}}}", idx), &value.to_string());
                }
            }
            // Handle simple {} placeholders
            else if result.contains("{}") {
                if Regex::new(r"\{[a-zA-Z_]\w*\}").unwrap().is_match(&result) {
                    return Err(Error::new(
                        ErrorKind::InvalidOperation,
                        "Cannot mix empty placeholders with named placeholders".to_string(),
                    ));
                }
                for arg in args.iter() {
                    result = result.replacen("{}", &arg.to_string(), 1);
                }
            }
            // Handle named placeholders {name}
            else if Regex::new(r"\{[a-zA-Z_]\w*\}").unwrap().is_match(&result) {
                if args.len() != 1 || args[0].kind() != ValueKind::Map {
                    return Err(Error::new(
                        ErrorKind::InvalidOperation,
                        "Named placeholders require a dictionary argument".to_string(),
                    ));
                }
                if let Some(obj) = args[0].as_object() {
                    if let Some(iter) = obj.try_iter_pairs() {
                        for (key, value) in iter {
                            result = result.replace(&format!("{{{}}}", key), &value.to_string());
                        }
                    }
                }
            }
            Ok(Value::from(result))
        }
        "zfill" => {
            let (width,): (usize,) = from_args(args)?;
            Ok(Value::from(format!("{:0>width$}", s, width = width)))
        }
        "removesuffix" => {
            let (suffix,): (&str,) = from_args(args)?;
            Ok(Value::from(s.trim_end_matches(suffix)))
        }
        _ => Err(Error::from(ErrorKind::UnknownMethod(
            "String".to_string(),
            method.to_string(),
        ))),
    }
}

fn map_methods(value: &Value, method: &str, args: &[Value]) -> Result<Value, Error> {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => {
            return Err(Error::from(ErrorKind::UnknownMethod(
                "None".to_string(),
                method.to_string(),
            )))
        }
    };

    match method {
        "keys" => {
            let () = from_args(args)?;
            Ok(Value::make_object_iterable(obj.clone(), |obj| {
                match obj.try_iter() {
                    Some(iter) => iter,
                    None => Box::new(None.into_iter()),
                }
            }))
        }
        "values" => {
            let () = from_args(args)?;
            Ok(Value::make_object_iterable(obj.clone(), |obj| {
                match obj.try_iter_pairs() {
                    Some(iter) => Box::new(iter.map(|(_, v)| v)),
                    None => Box::new(None.into_iter()),
                }
            }))
        }
        "items" => {
            let () = from_args(args)?;
            Ok(Value::make_object_iterable(obj.clone(), |obj| {
                match obj.try_iter_pairs() {
                    Some(iter) => Box::new(iter.map(|(k, v)| Value::from(tuple![k, v]))),
                    None => Box::new(None.into_iter()),
                }
            }))
        }
        "get" => {
            let (key, default): (&Value, Option<&Value>) = from_args(args)?;
            Ok(match obj.get_value(key) {
                Some(value) if !value.is_none() => value,
                _ => default.cloned().unwrap_or_else(|| Value::from(())),
            })
        }
        _ => Err(Error::from(ErrorKind::UnknownMethod(
            "Map".to_string(),
            method.to_string(),
        ))),
    }
}

fn seq_methods(value: &Value, method: &str, args: &[Value]) -> Result<Value, Error> {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => {
            return Err(Error::from(ErrorKind::UnknownMethod(
                "None".to_string(),
                method.to_string(),
            )))
        }
    };

    match method {
        "count" => {
            let (what,): (&Value,) = from_args(args)?;
            Ok(Value::from(if let Some(iter) = obj.try_iter() {
                iter.filter(|x| x == what).count()
            } else {
                0
            }))
        }
        "__sub__" => {
            let (other,): (&Value,) = from_args(args)?;
            if other.kind() == ValueKind::Seq {
                let other_set = other.try_iter().unwrap().collect::<BTreeSet<_>>();
                let mut result = Vec::new();
                for item in obj.try_iter().unwrap() {
                    if !other_set.contains(&item) {
                        result.push(item.clone());
                    }
                }
                return Ok(Value::from_object(MutableVec::from(result)));
            }
            Err(Error::new(
                ErrorKind::InvalidOperation,
                "Cannot subtract non-sequence".to_string(),
            ))
        }
        "union" => {
            // Handle multiple arguments like Python's set.union(*others)
            let mut result_set = BTreeSet::new();

            // Add all items from the original sequence
            if let Some(iter) = obj.try_iter() {
                for item in iter {
                    result_set.insert(item);
                }
            }

            // Add all items from each argument sequence
            for arg in args {
                match arg.try_iter() {
                    Ok(iter) => {
                        for item in iter {
                            result_set.insert(item);
                        }
                    }
                    Err(_) => {
                        return Err(Error::new(
                            ErrorKind::InvalidOperation,
                            "union() argument must be iterable",
                        ));
                    }
                }
            }

            // Convert result back to a sequence
            let result: Vec<Value> = result_set.into_iter().collect();
            Ok(Value::from_object(MutableVec::from(result)))
        }
        _ => Err(Error::from(ErrorKind::UnknownMethod(
            "Sequence".to_string(),
            method.to_string(),
        ))),
    }
}

fn number_methods(value: &Value, method: &str, args: &[Value]) -> Result<Value, Error> {
    let i = value.as_i64().unwrap_or_default();
    match method {
        "strftime" => {
            // i is the timestamp in seconds
            let (format,): (&str,) = from_args(args)?;
            if let Some(dt) = chrono::DateTime::from_timestamp(i, 0) {
                let formatted = dt.format(format);
                Ok(Value::from(formatted.to_string()))
            } else {
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "Invalid timestamp".to_string(),
                ))
            }
        }
        _ => Err(Error::from(ErrorKind::UnknownMethod(
            "Number".to_string(),
            method.to_string(),
        ))),
    }
}
