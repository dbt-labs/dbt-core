//! Export flags for telemetry events.
//!
//! These flags control which mediums an event is exported to.
//! If no flags are set, the event is considered internal-only.

use bitflags::bitflags;

bitflags! {
    /// Flags that determine where a telemetry event should be exported.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct TelemetryExportFlags: u32 {
        /// Export event to Parquet files.
        const EXPORT_PARQUET = 1;
        /// Export event to an OTLP backend.
        const EXPORT_OTLP = 1 << 1;
        /// Export event to JSONL files.
        const EXPORT_JSONL = 1 << 2;

        /// Alias that has all flags set – event goes to all mediums.
        const EXPORT_ALL = Self::EXPORT_PARQUET.bits() | Self::EXPORT_OTLP.bits() | Self::EXPORT_JSONL.bits();

        /// Alias that has both JSONL and OTLP flags, but no other flags.
        const EXPORT_JSONL_AND_OTLP = Self::EXPORT_JSONL.bits() | Self::EXPORT_OTLP.bits();
    }
}

impl TelemetryExportFlags {
    /// True if no flags are set – the event is internal-only.
    pub fn is_internal(self) -> bool {
        self.is_empty()
    }
}

impl Default for TelemetryExportFlags {
    fn default() -> Self {
        Self::empty()
    }
}
