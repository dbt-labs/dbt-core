use std::path::PathBuf;

use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::stdfs;

use super::preprocessor_location;

/// Represents a concrete location in some source file.
#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, JsonSchema)]
pub struct CodeLocation {
    pub line: usize,
    pub col: usize,
    pub index: usize,
    pub file: PathBuf,
    // An optional pointer to a corresponding location in some intermediate
    // preprocessed code, for example after macro expansion. Mainly intended for
    // debugging purposes.
    expanded: Option<Box<CodeLocation>>,
}

impl From<CodeLocation> for dbt_frontend_common::error::CodeLocation {
    fn from(location: CodeLocation) -> Self {
        dbt_frontend_common::error::CodeLocation {
            line: location.line,
            col: location.col,
            index: location.index,
        }
    }
}

impl PartialOrd for CodeLocation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CodeLocation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file
            .cmp(&other.file)
            .then(self.index.cmp(&other.index))
    }
}

impl CodeLocation {
    /// Constructs a new [CodeLocation] with the specified line, column and file
    /// path.
    pub fn new(line: usize, column: usize, index: usize, file: impl Into<PathBuf>) -> Self {
        CodeLocation {
            line,
            col: column,
            index,
            file: file.into(),
            expanded: None,
        }
    }

    /// Using the specified span information, maps this location to a
    /// pre-expanded location in the corresponding source file.
    pub fn with_macro_spans(
        self,
        spans: &[preprocessor_location::MacroSpan],
        expanded_file: Option<impl Into<PathBuf>>,
    ) -> Self {
        let expanded = expanded_file
            .map(|path| Box::new(CodeLocation::new(self.line, self.col, self.index, path)));
        CodeLocation {
            expanded,
            ..self.get_source_location(spans).with_file(self.file)
        }
    }

    /// Whether this code location has line and column number info.
    pub fn has_position(&self) -> bool {
        // 0:0 means unknown location
        self.line != 0 || self.col != 0
    }

    pub fn get_source_location(
        &self,
        macro_spans: &[preprocessor_location::MacroSpan],
    ) -> dbt_frontend_common::error::CodeLocation {
        let location = self.to_owned().into();

        let mut prev_macro_end = dbt_frontend_common::error::CodeLocation::new(1, 1, 0);
        let mut prev_expanded_end = dbt_frontend_common::error::CodeLocation::new(1, 1, 0);
        for macro_span in macro_spans {
            if macro_span.expanded_span.contains(&location) {
                return macro_span.macro_span.start.to_owned();
            } else if location < macro_span.expanded_span.start {
                return prev_macro_end + (location - prev_expanded_end);
            }
            prev_macro_end.clone_from(&macro_span.macro_span.stop);
            prev_expanded_end.clone_from(&macro_span.expanded_span.stop);
        }
        prev_macro_end + (location.to_owned() - prev_expanded_end)
    }

    pub fn with_file(self, file: impl Into<PathBuf>) -> Self {
        CodeLocation {
            file: file.into(),
            ..self
        }
    }

    pub fn with_offset(self, offset: dbt_frontend_common::error::CodeLocation) -> Self {
        let line = self.line + offset.line - 1;
        let col = if self.line == 1 {
            self.col + offset.col - 1
        } else {
            self.col
        };
        let index = self.index + offset.index;
        CodeLocation {
            line,
            col,
            index,
            ..self
        }
    }
}

impl From<PathBuf> for CodeLocation {
    fn from(file: PathBuf) -> Self {
        CodeLocation {
            file,
            ..Default::default()
        }
    }
}

impl From<dbt_serde_yaml::Span> for CodeLocation {
    fn from(span: dbt_serde_yaml::Span) -> Self {
        CodeLocation::new(
            span.start.line,
            span.start.column,
            span.start.index,
            span.filename
                .as_deref()
                .map_or_else(|| PathBuf::from("<unknown>"), PathBuf::from),
        )
    }
}

pub struct MiniJinjaErrorWrapper(pub minijinja::Error);

impl From<MiniJinjaErrorWrapper> for CodeLocation {
    fn from(err: MiniJinjaErrorWrapper) -> Self {
        if let Some(span) = err.0.span() {
            CodeLocation {
                file: err.0.name().unwrap_or_default().into(),
                line: span.start_line as usize,
                col: span.start_col as usize,
                index: span.start_offset as usize,
                expanded: None,
            }
        } else {
            CodeLocation {
                file: err.0.name().unwrap_or_default().into(),
                line: err.0.line().unwrap_or_default(),
                ..Default::default()
            }
        }
    }
}

impl std::fmt::Display for CodeLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let relative_path = if self.file.is_relative() {
            self.file.to_owned()
        } else if let Ok(cwd) = std::env::current_dir() {
            let cwd = stdfs::canonicalize(cwd.as_path()).unwrap_or(cwd);
            pathdiff::diff_paths(&self.file, &cwd).unwrap_or_else(|| self.file.to_owned())
        } else {
            self.file.to_owned()
        };

        if !self.has_position() {
            write!(f, "{}", relative_path.display())?;
        } else if self.col == 0 {
            write!(f, "{}:{}", relative_path.display(), self.line)?;
        } else {
            write!(f, "{}:{}:{}", relative_path.display(), self.line, self.col)?;
        }
        if let Some(expanded) = &self.expanded {
            write!(f, " ({expanded})")?;
        }
        Ok(())
    }
}

/// A location without an associate file path.
///
/// Can be converted to a concrete [CodeLocation] by calling
/// [AbstractLocation::with_file].
pub trait AbstractLocation {
    fn with_file(&self, file: impl Into<PathBuf>) -> CodeLocation;
}

impl AbstractLocation for dbt_frontend_common::error::CodeLocation {
    fn with_file(&self, file: impl Into<PathBuf>) -> CodeLocation {
        CodeLocation::new(self.line, self.col, self.index, file)
    }
}

impl AbstractLocation for (usize, usize, usize) {
    fn with_file(&self, file: impl Into<PathBuf>) -> CodeLocation {
        CodeLocation::new(self.0, self.1, self.2, file)
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd, Ord)]
pub struct Span {
    pub start: CodeLocation,
    pub stop: CodeLocation,
}

impl Span {
    pub fn with_macro_spans(
        self,
        spans: &[preprocessor_location::MacroSpan],
        expanded_file: Option<impl Into<PathBuf>>,
    ) -> Self {
        let expanded_file: Option<PathBuf> = expanded_file.map(|path| path.into());
        Span {
            start: self.start.with_macro_spans(spans, expanded_file.to_owned()),
            stop: self.stop.with_macro_spans(spans, expanded_file),
        }
    }

    pub fn with_offset(self, offset: dbt_frontend_common::error::CodeLocation) -> Self {
        Span {
            start: self.start.with_offset(offset),
            stop: self.stop.with_offset(offset),
        }
    }
}

pub trait AbstractSpan {
    fn with_file(&self, file: impl Into<PathBuf>) -> Span;
}

impl AbstractSpan for dbt_frontend_common::span::Span {
    fn with_file(&self, file: impl Into<PathBuf>) -> Span {
        let file = file.into();
        Span {
            start: self.start.with_file(file.to_owned()),
            stop: self.stop.with_file(file),
        }
    }
}
