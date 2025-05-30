use std::ptr::addr_of_mut;
use std::{fmt, io};

use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind};
use crate::machinery::Span;
use crate::utils::AutoEscape;
use crate::value::Value;

/// How should output be captured?
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[cfg_attr(feature = "unstable_machinery_serde", derive(serde::Serialize))]
pub enum CaptureMode {
    Capture,
    Discard,
}

/// An abstraction over [`fmt::Write`](std::fmt::Write) for the rendering.
///
/// This is a utility type used in the engine which can be written into like one
/// can write into an [`std::fmt::Write`] value.  It's primarily used internally
/// in the engine but it's also passed to the custom formatter function.
pub struct Output<'a> {
    w: &'a mut (dyn fmt::Write + 'a),
    capture_stack: Vec<Option<String>>,
}

impl<'a> Output<'a> {
    /// Creates an output writing to a string.
    pub(crate) fn with_string(buf: &'a mut String) -> Self {
        Self {
            w: buf,
            capture_stack: Vec::new(),
        }
    }

    /// Creates an output writing to a writer.
    pub fn with_write(w: &'a mut (dyn fmt::Write + 'a)) -> Self {
        Self {
            w,
            capture_stack: Vec::new(),
        }
    }

    /// Creates a null output that writes nowhere.
    pub(crate) fn null() -> Self {
        // The null writer also has a single entry on the discarding capture
        // stack.  In fact, `w` is more or less useless here as we always
        // shadow it.  This is done so that `is_discarding` returns true.
        Self {
            w: NullWriter::get_mut(),
            capture_stack: vec![None],
        }
    }

    /// Begins capturing into a string or discard.
    pub(crate) fn begin_capture(&mut self, mode: CaptureMode) {
        self.capture_stack.push(match mode {
            CaptureMode::Capture => Some(String::new()),
            CaptureMode::Discard => None,
        });
    }

    /// Ends capturing and returns the captured string as value.
    pub(crate) fn end_capture(&mut self, auto_escape: AutoEscape) -> Value {
        if let Some(captured) = self.capture_stack.pop().unwrap() {
            if !matches!(auto_escape, AutoEscape::None) {
                Value::from_safe_string(captured)
            } else {
                Value::from(captured)
            }
        } else {
            Value::UNDEFINED
        }
    }

    #[inline(always)]
    fn target(&mut self) -> &mut dyn fmt::Write {
        match self.capture_stack.last_mut() {
            Some(Some(stream)) => stream as _,
            Some(None) => NullWriter::get_mut(),
            None => self.w,
        }
    }

    /// Returns `true` if the output is discarding.
    #[inline(always)]
    pub(crate) fn is_discarding(&self) -> bool {
        matches!(self.capture_stack.last(), Some(None))
    }

    /// Writes some data to the underlying buffer contained within this output.
    #[inline]
    pub fn write_str(&mut self, s: &str) -> fmt::Result {
        self.target().write_str(s)
    }

    /// Writes some formatted information into this instance.
    #[inline]
    pub fn write_fmt(&mut self, a: fmt::Arguments<'_>) -> fmt::Result {
        self.target().write_fmt(a)
    }
}

impl fmt::Write for Output<'_> {
    #[inline]
    fn write_str(&mut self, s: &str) -> fmt::Result {
        fmt::Write::write_str(self.target(), s)
    }

    #[inline]
    fn write_char(&mut self, c: char) -> fmt::Result {
        fmt::Write::write_char(self.target(), c)
    }

    #[inline]
    fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> fmt::Result {
        fmt::Write::write_fmt(self.target(), args)
    }
}

pub struct NullWriter;

impl NullWriter {
    /// Returns a reference to the null writer.
    pub fn get_mut() -> &'static mut NullWriter {
        static mut NULL_WRITER: NullWriter = NullWriter;
        // SAFETY: this is safe as the null writer is a ZST
        unsafe { &mut *addr_of_mut!(NULL_WRITER) }
    }
}

impl fmt::Write for NullWriter {
    #[inline]
    fn write_str(&mut self, _s: &str) -> fmt::Result {
        Ok(())
    }

    #[inline]
    fn write_char(&mut self, _c: char) -> fmt::Result {
        Ok(())
    }
}

pub struct WriteWrapper<W> {
    pub w: W,
    pub err: Option<io::Error>,
}

impl<W> WriteWrapper<W> {
    /// Replaces the given error with the held error if available.
    pub fn take_err(&mut self, original: Error) -> Error {
        self.err
            .take()
            .map(|io_err| {
                Error::new(ErrorKind::WriteFailure, "I/O error during rendering")
                    .with_source(io_err)
            })
            .unwrap_or(original)
    }
}

impl<W: io::Write> fmt::Write for WriteWrapper<W> {
    #[inline]
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.w.write_all(s.as_bytes()).map_err(|e| {
            self.err = Some(e);
            fmt::Error
        })
    }

    #[inline]
    fn write_char(&mut self, c: char) -> fmt::Result {
        self.w
            .write_all(c.encode_utf8(&mut [0; 4]).as_bytes())
            .map_err(|e| {
                self.err = Some(e);
                fmt::Error
            })
    }
}

/// This a location mapping information between the source file and macro expanded file
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MacroSpans {
    /// The list of spans that are mapped to the source file
    pub items: Vec<(Span, Span)>,
}

impl MacroSpans {
    /// push a new span to the list
    pub fn push(&mut self, source: Span, expanded: Span) {
        self.items.push((source, expanded));
    }

    /// extend the list with another list
    pub fn extend(&mut self, other: MacroSpans) {
        self.items.extend(other.items);
    }
}
