//! A mini re-like module for MiniJinja, intended to mirror Python's `re` module behavior.
//!
//! This module provides functions such as `compile`, `match`, `search`, `fullmatch`,
//! `findall`, `split`, `sub`, etc., using Rust's `regex` crate under the hood. While
//! this is only a partial implementation of Python's `re` spec, it demonstrates the
//! pattern-oriented usage consistent with MiniJinja's function/value approach.

use fancy_regex::{Captures, Expander, Regex}; // like python regex, fancy_regex supports lookadheds/lookbehinds
use minijinja::{value::Object, Error, ErrorKind, Value};
use std::{collections::BTreeMap, fmt, iter, sync::Arc};

/// Create a namespace with `re`-like functions for pattern matching.
pub fn create_re_namespace() -> BTreeMap<String, Value> {
    let mut re_module = BTreeMap::new();

    // Python-like top-level functions:
    re_module.insert("compile".to_string(), Value::from_function(re_compile));
    re_module.insert("match".to_string(), Value::from_function(re_match));
    re_module.insert("search".to_string(), Value::from_function(re_search));
    re_module.insert("fullmatch".to_string(), Value::from_function(re_fullmatch));
    re_module.insert("findall".to_string(), Value::from_function(re_findall));
    re_module.insert("split".to_string(), Value::from_function(re_split));
    re_module.insert("sub".to_string(), Value::from_function(re_sub));

    re_module
}

/// Compile the given pattern into a RegexObject, optionally using flags (not fully implemented).
///
/// Python signature: re.compile(pattern, flags=0)
fn re_compile(args: &[Value]) -> Result<Value, Error> {
    let pattern = args
        .first()
        .ok_or_else(|| Error::new(ErrorKind::MissingArgument, "Pattern argument required"))?
        .to_string();

    // If desired, we could parse optional flags from args.get(1), but we omit advanced flags here.
    let compiled = Regex::new(&pattern).map_err(|e| {
        Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to compile regex: {e}"),
        )
    })?;

    let pattern = Pattern::new(&pattern, compiled);
    Ok(Value::from_object(pattern))
}

#[derive(Debug, Clone)]
pub struct Pattern {
    raw: String,
    _compiled: Regex,
}

impl Pattern {
    pub fn new(raw: &str, compiled: Regex) -> Self {
        Self {
            raw: raw.to_string(),
            _compiled: compiled, // TODO: use this in re methods
        }
    }
}

impl Object for Pattern {
    fn call_method(
        self: &std::sync::Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        let args = iter::once(Value::from(self.raw.clone()))
            .chain(args.iter().cloned())
            .collect::<Vec<_>>();
        if method == "match" {
            re_match(&args)
        } else if method == "search" {
            re_search(&args)
        } else if method == "fullmatch" {
            re_fullmatch(&args)
        } else if method == "findall" {
            re_findall(&args)
        } else if method == "split" {
            re_split(&args)
        } else if method == "sub" {
            re_sub(&args)
        } else {
            Err(Error::new(
                ErrorKind::UnknownMethod,
                format!("Pattern object has no method named '{method}'"),
            ))
        }
    }
}

/// Python `re.match(pattern, string, flags=0)`.
/// Checks for a match only at the beginning of the string.
fn re_match(args: &[Value]) -> Result<Value, Error> {
    if args.len() < 2 {
        return Err(Error::new(
            ErrorKind::MissingArgument,
            "match() requires pattern and string arguments",
        ));
    }

    let (regex, text) = get_or_compile_regex_and_text(&args[..2])?;

    // Create a new pattern that must match from the start
    let mut pattern = String::from(r"\A");
    pattern.push_str(regex.as_str());

    let start_anchored = Regex::new(&pattern).map_err(|e| {
        Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to compile regex: {e}"),
        )
    })?;

    if let Ok(Some(captures)) = start_anchored.captures(text) {
        let groups: Vec<Value> = captures
            .iter()
            .map(|m| Value::from(m.map(|m| m.as_str()).unwrap_or_default()))
            .collect();
        let span: Option<(usize, usize)> = captures
            .iter()
            .next()
            .and_then(|m| m.map(|m| (m.start(), m.end())));
        let capture = Capture::new(groups, span);
        Ok(Value::from_object(capture))
    } else {
        Ok(Value::NONE)
    }
}

/// Python `re.search(pattern, string, flags=0)`.
/// Searches through the entire string for the first match.
fn re_search(args: &[Value]) -> Result<Value, Error> {
    if args.len() < 2 {
        return Err(Error::new(
            ErrorKind::MissingArgument,
            "search() requires pattern and string arguments",
        ));
    }

    let (regex, text) = get_or_compile_regex_and_text(&args[..2])?;

    if let Ok(Some(captures)) = regex.captures(text) {
        let groups: Vec<Value> = captures
            .iter()
            .map(|m| Value::from(m.map(|m| m.as_str()).unwrap_or_default()))
            .collect();
        let span = captures
            .iter()
            .next()
            .and_then(|m| m.map(|m| (m.start(), m.end())));
        let capture = Capture::new(groups, span);
        Ok(Value::from_object(capture))
    } else {
        Ok(Value::NONE)
    }
}

/// Python `re.fullmatch(pattern, string, flags=0)`.
/// Matches the entire string against the pattern (like `^pattern$`).
fn re_fullmatch(args: &[Value]) -> Result<Value, Error> {
    let (regex, text) = get_or_compile_regex_and_text(args)?;
    match regex.find(text) {
        Ok(Some(m)) if m.start() == 0 && m.end() == text.len() => {
            Ok(match_obj_to_list(&regex, text, m.start(), m.end()))
        }
        _ => Ok(Value::from(None::<Value>)),
    }
}

/// Python `re.findall(pattern, string, flags=0)`.
/// Returns all non-overlapping matches of pattern in string, as a list of strings or
/// list of tuples if groups exist.
fn re_findall(args: &[Value]) -> Result<Value, Error> {
    if args.len() < 2 {
        return Err(Error::new(
            ErrorKind::MissingArgument,
            "findall() requires pattern and string arguments",
        ));
    }

    let (regex, text) = get_or_compile_regex_and_text(&args[..2])?;
    let matches = regex
        .captures_iter(text)
        .map(|captures| {
            let captures =
                captures.map_err(|err| Error::new(ErrorKind::RegexError, err.to_string()))?;
            Ok(match captures.len() {
                1 => {
                    let full = captures.get(0).unwrap().as_str();
                    Value::from(full)
                }
                2 => {
                    let capture = captures.get(1).unwrap().as_str();
                    Value::from(capture)
                }
                _ => {
                    let groups: Vec<Value> = captures
                        .iter()
                        .skip(1)
                        .map(|m| Value::from(m.map(|m| m.as_str()).unwrap_or_default()))
                        .collect();
                    let span = captures
                        .iter()
                        .nth(1)
                        .and_then(|m| m.map(|m| (m.start(), m.end())));
                    let capture = Capture::new(groups, span);
                    Value::from_object(capture)
                }
            })
        })
        .collect::<Result<Vec<Value>, Error>>()?;

    Ok(Value::from(matches))
}

/// Python `re.split(pattern, string, maxsplit=0, flags=0)`.
/// Split string by occurrences of pattern. If capturing groups are used,
/// those are included in the result.
fn re_split(args: &[Value]) -> Result<Value, Error> {
    if args.len() < 2 {
        return Err(Error::new(
            ErrorKind::MissingArgument,
            "split() requires pattern and string arguments",
        ));
    }

    let (regex, text) = get_or_compile_regex_and_text(&args[..2])?;

    let maxsplit = args.get(2).and_then(|v| v.as_i64()).unwrap_or(0) as usize;

    let mut result = Vec::new();
    let mut last = 0;

    for (n, captures) in regex.captures_iter(text).enumerate() {
        if maxsplit != 0 && n >= maxsplit {
            break;
        }
        let captures =
            captures.map_err(|err| Error::new(ErrorKind::RegexError, err.to_string()))?;

        let full = captures.get(0).unwrap();
        result.push(Value::from(&text[last..full.start()]));

        for m in captures.iter().skip(1) {
            if let Some(m) = m {
                result.push(Value::from(m.as_str()));
            } else {
                result.push(Value::from(""));
            }
        }

        last = full.end();
    }

    if last <= text.len() {
        result.push(Value::from(&text[last..]));
    }

    Ok(Value::from(result))
}

/// Python `re.sub(pattern, repl, string, count=0, flags=0)`.
/// Return the string obtained by replacing the leftmost non-overlapping occurrences
/// of pattern in string by repl. If repl is a function, it is called for every match.
fn re_sub(args: &[Value]) -> Result<Value, Error> {
    if args.len() < 3 {
        return Err(Error::new(
            ErrorKind::MissingArgument,
            "Usage: sub(pattern, repl, string, [count=0])",
        ));
    }

    let (regex, _text) = get_or_compile_regex_and_text(&args[..2])?;
    let repl_text = args[1].to_string();
    let text_arg = &args[2].to_string();

    let count = args.get(3).and_then(|v| v.as_i64()).unwrap_or(0);

    let expander = Expander::python();
    let replacer = |caps: &Captures| expander.expansion(&repl_text, caps);

    if count == 0 {
        Ok(Value::from(
            regex.replace_all(text_arg, replacer).to_string(),
        ))
    } else {
        Ok(Value::from(
            regex
                .replacen(text_arg, count as usize, replacer)
                .to_string(),
        ))
    }
}

/// Extract either a compiled regex from arg[0] *or* compile arg[0], plus read `string` from arg[1].
fn get_or_compile_regex_and_text(args: &[Value]) -> Result<(Box<Regex>, &str), Error> {
    if args.len() < 2 {
        return Err(Error::new(
            ErrorKind::MissingArgument,
            "Need at least pattern and string arguments",
        ));
    }

    // First arg: either compiled or raw pattern
    let pattern = args[0].to_string();
    let compiled = Box::new(Regex::new(&pattern).map_err(|e| {
        Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to compile regex: {e}"),
        )
    })?);

    // Second arg: the text to match against
    let text = args[1].to_string();
    Ok((compiled, Box::leak(text.into_boxed_str())))
}

/// Utility: turn a single match range into a quick list describing the match start/end/group0.
fn match_obj_to_list(re: &Regex, text: &str, start: usize, end: usize) -> Value {
    if let Ok(Some(caps)) = re.captures(&text[start..end]) {
        // We'll store (group0, group1, ...) as a list of strings or None
        let mut cap_vals = Vec::with_capacity(caps.len());
        for i in 0..caps.len() {
            cap_vals.push(Value::from(caps.get(i).map(|m| m.as_str()).unwrap_or("")));
        }
        Value::from(cap_vals)
    } else {
        // If for some reason capturing fails, just store the entire match
        Value::from(&text[start..end])
    }
}
#[derive(Debug, Clone)]
pub struct Capture {
    groups: Vec<Value>,
    span: Option<(usize, usize)>,
}

impl Capture {
    pub fn new(groups: Vec<Value>, span: Option<(usize, usize)>) -> Self {
        Self { groups, span }
    }
}

impl Object for Capture {
    fn call_method(
        self: &std::sync::Arc<Self>,
        _state: &minijinja::State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<Value, Error> {
        if method == "group" {
            let idx = if args.is_empty() {
                0
            } else {
                args[0].as_i64().unwrap_or(0) as usize
            };

            if idx < self.groups.len() {
                Ok(self.groups[idx].clone())
            } else {
                Ok(Value::from(""))
            }
        } else {
            Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Method '{method}' not found"),
            ))
        }
    }

    fn is_true(self: &Arc<Self>) -> bool {
        !self.groups.is_empty()
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result
    where
        Self: Sized + 'static,
    {
        write!(f, "<re.Match object; ")?;
        if let Some(g) = self.groups.first() {
            if let Some((start, end)) = self.span {
                write!(f, "span = ({start}, {end}), ")?;
            }
            // TODO: escape quotes in g
            write!(f, "match = '{g}'")?;
        }
        write!(f, ">")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_re_sub() {
        let result = re_sub(&[
            Value::from("(A)".to_string()),
            Value::from("_\\1_".to_string()),
            Value::from("ABAB $1".to_string()),
        ])
        .unwrap();
        assert_eq!(result.to_string(), "_A_B_A_B $1");

        let result = re_sub(&[
            Value::from("(A)".to_string()),
            Value::from("_\\1_".to_string()),
            Value::from("ABAB $1".to_string()),
            Value::from(1),
        ])
        .unwrap();
        assert_eq!(result.to_string(), "_A_BAB $1");
    }

    #[test]
    fn test_re_match() {
        let result = re_match(&[
            Value::from(".*".to_string()),
            Value::from("xyz".to_string()),
        ])
        .unwrap();
        assert!(result.is_true());
        assert_eq!(
            result.to_string(),
            "<re.Match object; span = (0, 3), match = 'xyz'>"
        );

        let result = re_match(&[
            Value::from("\\d{10}".to_string()),
            Value::from("1234567890".to_string()),
        ])
        .unwrap();
        assert!(result.is_true());
        assert_eq!(
            result.to_string(),
            "<re.Match object; span = (0, 10), match = '1234567890'>"
        );

        let result = re_match(&[
            Value::from("\\d{10}".to_string()),
            Value::from("xyz".to_string()),
        ])
        .unwrap();
        assert!(!result.is_true());
        assert_eq!(result.to_string(), "none");
    }

    #[test]
    fn test_re_search() {
        let result = re_search(&[
            Value::from("world".to_string()),
            Value::from("hello, world".to_string()),
        ])
        .unwrap();
        assert!(result.is_true());
        assert_eq!(
            result.to_string(),
            "<re.Match object; span = (7, 12), match = 'world'>"
        );

        let result = re_search(&[
            Value::from("hello".to_string()),
            Value::from("world".to_string()),
        ])
        .unwrap();
        assert!(!result.is_true());
        assert_eq!(result.to_string(), "none");
    }
}
