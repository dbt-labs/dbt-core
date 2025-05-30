use serde::{Deserialize, Serialize};

use crate::error::CodeLocation;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Serialize, Deserialize, Hash, Ord,
)]
pub struct Span {
    /// start location of the span(inclusive)
    pub start: CodeLocation,
    /// stop location of the span(exclusive)
    pub stop: CodeLocation,
}
impl Span {
    pub fn new(start: CodeLocation, stop: CodeLocation) -> Self {
        Span { start, stop }
    }

    pub fn contains(&self, location: &CodeLocation) -> bool {
        &self.start <= location && location < &self.stop
    }

    pub fn slice(&self, input: &str) -> String {
        // TODO it was
        // input[self.start.index..self.stop.index + 1].to_string()
        // because macro span is exclusive, we removed +1
        // be careful lint fix is inclusive, lint fix will break
        input[self.start.index..self.stop.index].to_string()
    }

    pub fn with_offset(&self, start_location: &CodeLocation) -> Self {
        Span {
            start: self.start.with_offset(start_location),
            stop: self.stop.with_offset(start_location),
        }
    }

    pub fn diff(&self, start_location: &CodeLocation) -> Self {
        Span {
            start: self.start.diff(start_location),
            stop: self.stop.diff(start_location),
        }
    }
}
