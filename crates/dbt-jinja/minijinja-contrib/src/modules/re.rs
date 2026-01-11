//! A mini re-like module for MiniJinja, intended to mirror Python's `re` module behavior.
//!
//! This module provides functions such as `compile`, `match`, `search`, `fullmatch`,
//! `findall`, `split`, `sub`, etc., using Rust's `regex` crate under the hood. While
//! this is only a partial implementation of Python's `re` spec, it demonstrates the
//! pattern-oriented usage consistent with MiniJinja's function/value approach.

use fancy_regex::{Captures, Expander, Regex}; // like python regex, fancy_regex supports lookadheds/lookbehinds
use indexmap::IndexMap;
use minijinja::{
    arg_utils::ArgsIter,
    value::{Object, ValueMap},
    Error, ErrorKind, Value,
};
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
    re_module.insert("escape".to_string(), Value::from_function(re_escape));

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
    compiled: Regex,
}

impl Pattern {
    pub fn new(raw: &str, compiled: Regex) -> Self {
        Self {
            raw: raw.to_string(),
            compiled,
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
        let groups: Vec<(Value, Option<Span>)> = captures
            .iter()
            .map(|m| {
                m.map(|m| (Value::from(m.as_str()), Some((m.start(), m.end()))))
                    .unwrap_or((Value::NONE, None))
            })
            .collect();
        let names = start_anchored.capture_names();
        let named_groups: IndexMap<String, usize> = IndexMap::from_iter(
            names
                .enumerate()
                .filter_map(|(idx, name)| name.map(|name| (name.to_string(), idx))),
        );
        let capture = Capture::new(groups, named_groups);
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
        let groups: Vec<(Value, Option<Span>)> = captures
            .iter()
            .map(|m| {
                m.map(|m| (Value::from(m.as_str()), Some((m.start(), m.end()))))
                    .unwrap_or((Value::NONE, None))
            })
            .collect();
        let names = regex.capture_names();
        let named_groups: IndexMap<String, usize> = IndexMap::from_iter(
            names
                .enumerate()
                .filter_map(|(idx, name)| name.map(|name| (name.to_string(), idx))),
        );
        let capture = Capture::new(groups, named_groups);
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
    let matches =
        regex
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
                        let groups: Vec<(Value, Option<Span>)> = captures
                            .iter()
                            .skip(1)
                            .map(|m| {
                                m.map(|m| (Value::from(m.as_str()), Some((m.start(), m.end()))))
                                    .unwrap_or((Value::NONE, None))
                            })
                            .collect();
                        let names = regex.capture_names();
                        let named_groups: IndexMap<String, usize> =
                            IndexMap::from_iter(names.enumerate().skip(1).filter_map(
                                |(idx, name)| name.map(|name| (name.to_string(), idx)),
                            ));
                        let capture = Capture::new(groups, named_groups);
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

/// Python `re.escape(pattern)`.
/// Escapes special characters in a string so it can be used as a literal pattern in a regex.
/// According to Python 3.7+ behavior, escapes these characters: \ . ^ $ * + ? { } [ ] ( ) |
fn re_escape(args: &[Value]) -> Result<Value, Error> {
    if args.is_empty() {
        return Err(Error::new(
            ErrorKind::MissingArgument,
            "escape() requires a pattern string argument",
        ));
    }

    let pattern = args[0].to_string();
    let mut escaped = String::with_capacity(pattern.len() * 2);

    for ch in pattern.chars() {
        match ch {
            '\\' | '.' | '^' | '$' | '*' | '+' | '?' | '{' | '}' | '[' | ']' | '(' | ')' | '|' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => {
                escaped.push(ch);
            }
        }
    }

    Ok(Value::from(escaped))
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
    let compiled = if let Some(object) = args[0].as_object() {
        if let Some(pattern) = object.downcast_ref::<Pattern>() {
            Box::new(pattern.compiled.clone())
        } else {
            let pattern = args[0].to_string();
            Box::new(Regex::new(&pattern).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Failed to compile regex: {e}"),
                )
            })?)
        }
    } else {
        let pattern = args[0].to_string();
        Box::new(Regex::new(&pattern).map_err(|e| {
            Error::new(
                ErrorKind::InvalidOperation,
                format!("Failed to compile regex: {e}"),
            )
        })?)
    };

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

type Span = (usize, usize);

#[derive(Debug, Clone)]
pub struct Capture {
    /// List of groups and spans
    groups: Vec<(Value, Option<Span>)>,
    /// Map of group names to their indices in the group vector
    named_groups: IndexMap<String, usize>,
}

impl Capture {
    pub fn new(groups: Vec<(Value, Option<Span>)>, named_groups: IndexMap<String, usize>) -> Self {
        Self {
            groups,
            named_groups,
        }
    }

    /// Helper: parse the [group] argument, which could be an index or the name of a group
    fn get_group_idx_from_value(self: &std::sync::Arc<Self>, arg: &Value) -> Result<usize, Error> {
        if let Some(idx) = arg.as_usize() {
            Ok(idx)
        } else if let Some(group) = arg.as_str() {
            self.named_groups
                .get(group)
                .copied()
                .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "no such group"))
        } else {
            Err(Error::new(
                ErrorKind::InvalidArgument,
                "group argument must be an int or string",
            ))
        }
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
        match method {
            // Match.expand(template)
            "expand" => {
                // https://docs.python.org/3/library/re.html#re.Match.expand
                todo!("'expand' is not yet implemented")
            }
            // Match.group([group1, ...])
            "group" => {
                if args.len() > 1 {
                    let mut groups: Vec<Value> = Vec::with_capacity(args.len());

                    for arg in args {
                        let idx = self.get_group_idx_from_value(arg)?;
                        groups.push(self.groups[idx].0.clone());
                    }

                    Ok(Value::from_tuple(groups))
                } else {
                    let idx = if args.is_empty() {
                        0
                    } else {
                        self.get_group_idx_from_value(&args[0])?
                    };

                    if idx < self.groups.len() {
                        Ok(self.groups[idx].0.clone())
                    } else {
                        Err(Error::new(ErrorKind::InvalidArgument, "no such group"))
                    }
                }
            }
            // Match.groups(default=None)
            "groups" => {
                let iter = ArgsIter::new(method, &[], args);
                let default = iter
                    .next_kwarg::<Option<Value>>("default")?
                    .unwrap_or(Value::NONE);
                let groups = Vec::from_iter(self.groups.iter().skip(1).map(|(group, _)| {
                    if group.is_none() {
                        default.clone()
                    } else {
                        group.clone()
                    }
                }));
                Ok(Value::from_tuple(groups))
            }
            // Match.groupdict(default=None)
            "groupdict" => {
                let iter = ArgsIter::new(method, &[], args);
                let default = iter
                    .next_kwarg::<Option<Value>>("default")?
                    .unwrap_or(Value::NONE);
                let named_groups =
                    ValueMap::from_iter(self.named_groups.iter().map(|(name, idx)| {
                        let group = &self.groups[*idx].0;
                        if group.is_none() {
                            (Value::from(name), default.clone())
                        } else {
                            (Value::from(name), group.clone())
                        }
                    }));
                Ok(Value::from(named_groups))
            }
            // Match.start([group])
            "start" => {
                let iter = ArgsIter::new(method, &["group"], args);
                let idx = if let Ok(arg) = iter.next_arg() {
                    self.get_group_idx_from_value(arg)?
                } else {
                    0
                };
                iter.finish()?;

                if idx < self.groups.len() {
                    if let Some((start, _)) = self.groups[idx].1 {
                        Ok(Value::from(start))
                    } else {
                        Ok(Value::from(-1))
                    }
                } else {
                    Err(Error::new(ErrorKind::InvalidArgument, "no such group"))
                }
            }
            // Match.end([group])
            "end" => {
                let iter = ArgsIter::new(method, &["group"], args);
                let idx = if let Ok(arg) = iter.next_arg() {
                    self.get_group_idx_from_value(arg)?
                } else {
                    0
                };
                iter.finish()?;

                if idx < self.groups.len() {
                    if let Some((_, end)) = self.groups[idx].1 {
                        Ok(Value::from(end))
                    } else {
                        Ok(Value::from(-1))
                    }
                } else {
                    Err(Error::new(ErrorKind::InvalidArgument, "no such group"))
                }
            }
            // Match.span([group])
            "span" => {
                let iter = ArgsIter::new(method, &["group"], args);
                let idx = if let Ok(arg) = iter.next_arg() {
                    self.get_group_idx_from_value(arg)?
                } else {
                    0
                };
                iter.finish()?;

                if idx < self.groups.len() {
                    if let Some((start, end)) = self.groups[idx].1 {
                        Ok(Value::from_tuple(vec![
                            Value::from(start),
                            Value::from(end),
                        ]))
                    } else {
                        Ok(Value::from_tuple(vec![Value::from(-1), Value::from(-1)]))
                    }
                } else {
                    Err(Error::new(ErrorKind::InvalidArgument, "no such group"))
                }
            }
            _ => Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Method '{method}' not found!"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            // Match.pos
            Some("pos") => {
                // https://docs.python.org/3/library/re.html#re.Match.pos
                todo!("'pos' is not yet implemented")
            }
            // Match.endpos
            Some("endpos") => {
                // https://docs.python.org/3/library/re.html#re.Match.endpos
                todo!("'endpos' is not yet implemented")
            }
            // Match.lastindex
            Some("lastindex") => {
                // https://docs.python.org/3/library/re.html#re.Match.lastindex
                todo!("'lastindex' is not yet implemented")
            }
            // Match.lastgroup
            Some("lastgroup") => {
                // https://docs.python.org/3/library/re.html#re.Match.lastgroup
                todo!("'lastgroup' is not yet implemented")
            }
            // Match.re
            Some("re") => {
                // https://docs.python.org/3/library/re.html#re.Match.re
                todo!("'re' is not yet implemented")
            }
            // Match.string
            Some("string") => {
                // https://docs.python.org/3/library/re.html#re.Match.string
                todo!("'string' is not yet implemented")
            }
            _ => None,
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
        if let Some((g, span)) = self.groups.first() {
            if let Some((start, end)) = span {
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
        assert_eq!(result.to_string(), "None");
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
        assert_eq!(result.to_string(), "None");
    }

    #[test]
    fn test_re_glob_search() {
        let result = re_search(&[
            Value::from(".*".to_string()),
            Value::from("xyz".to_string()),
        ])
        .unwrap();
        assert!(result.is_true());

        let compiled_pattern = re_compile(&[Value::from(".*".to_string())]).unwrap();
        let result = re_search(&[compiled_pattern, Value::from("xyz".to_string())]).unwrap();
        assert!(result.is_true());
    }

    #[test]
    fn test_re_escape() {
        // Test basic special character escaping
        let result = re_escape(&[Value::from("hello.world")]).unwrap();
        assert_eq!(result.to_string(), r"hello\.world");

        // Test multiple special characters
        let result = re_escape(&[Value::from("$100+")]).unwrap();
        assert_eq!(result.to_string(), r"\$100\+");

        // Test all metacharacters
        let result = re_escape(&[Value::from(r"\^$.*+?{}[]|()")]).unwrap();
        assert_eq!(result.to_string(), r"\\\^\$\.\*\+\?\{\}\[\]\|\(\)");

        // Test string with no special characters
        let result = re_escape(&[Value::from("hello")]).unwrap();
        assert_eq!(result.to_string(), "hello");

        // Test empty string
        let result = re_escape(&[Value::from("")]).unwrap();
        assert_eq!(result.to_string(), "");

        // Test typical suffix pattern (the use case from the issue)
        let result = re_escape(&[Value::from("_usd$")]).unwrap();
        assert_eq!(result.to_string(), r"_usd\$");
    }

    #[test]
    fn test_re_escape_missing_argument() {
        let result = re_escape(&[]);
        assert!(result.is_err());
    }
}
