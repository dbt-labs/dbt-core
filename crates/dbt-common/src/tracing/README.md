# Tracing Infrastructure

This module provides a comprehensive tracing infrastructure for Fusion, serving multiple purposes:
1. **Unified span & events data layer** - The single source of truth for all operations and events in the system
2. **Structured telemetry** - Capturing application performance data and metrics for downstream systems (e.g. cloud clients, orchestration, metadata etc.)
3. **Interactive user experience** - [TBD] Formats data for CLI and user logs
2. **Developer debugging** - Providing rich debugging information compiled away in release builds

## Architecture Overview

The tracing infrastructure follows a layered architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Log Facade                                   │
│                  (log crate API)                                │
│                 Legacy log! macros                              │
└─────────────────────────┬───────────────────────────────────────┘
                          │ (Bridge forwards to tracing)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Tracing Facade                               │
│                (tracing crate API)                              │
│          tracing::instrument, tracing::info!, etc.              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   Data Layer                                    │
│              (TelemetryDataLayer)                               │
│  - Converts spans/events to structured telemetry records        │
│  - Stores data in span extensions for writing layers            │
│  - Handles trace/span ID generation and correlation             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                 Writing Layers                                  │
│  ┌─────────────────────┐ ┌─────────────────────────────────┐    │
│  │ TelemetryWriterLayer│ │    OTLP Exporter Layer          │    │
│  │   (File output)     │ │ (OpenTelemetry Protocol)        │    │
│  │   - JSONL format    │ │   - Debug builds only           │    │
│  │   - Production use  │ │   - Feature gated               │    │
│  └─────────────────────┘ └─────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │          CLI & User Logs Layer                          │    │
│  │              [NOT IMPLEMENTED]                          │    │
│  │   - Pretty formatting for terminal output               │    │
│  │   - Progress bars and interactive elements              │    │
│  │   - User-facing log messages                            │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### Data Layer (`TelemetryDataLayer`)
- **Purpose**: Converts tracing spans and events into structured telemetry records
- **Key Features**:
  - Generates globally unique span IDs across the entire process
  - Correlates all data with a trace ID (derived from invocation UUID)
  - Extracts structured attributes from span/event fields
  - Handles code location recording (stripped in release builds)
  - Stores telemetry data in span extensions for writer layers

### Writing Layers
- **File Writer** (`TelemetryWriterLayer`): Outputs telemetry to JSONL files for production use
- **OTLP Exporter** (`OTLPExporterLayer`): Exports to OpenTelemetry Protocol endpoints (debug builds only)

### Telemetry Records
All telemetry data follows structured schemas defined in `dbt-telemetry/src/schemas/record.rs`:
- **SpanStartInfo**: Emitted when spans begin
- **SpanEndInfo**: Emitted when spans complete
- **LogRecordInfo**: Emitted for log events within spans

## Usage Examples

CAVEAT: as of time of writing we are in transitioning from legacy `log` crate to `tracing` crate. Most of the logging is still done via `log!` based macros.

### Basic Span Instrumentation

```rust
use tracing::{instrument, info_span};

// On rare ocasion you may want to create spans manually instead of
// using `#[instrument]` attribute.
let session_span = tracing::info_span!(
    "Invocation",
    { TRACING_ATTR_FIELD } = SpanAttributes::Invocation {
        invocation_id: arg.io.invocation_id.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        host_os: std::env::consts::OS.to_string(),
        host_arch: std::env::consts::ARCH.to_string(),
        target: arg.target.clone(),
        metrics: None,
    }
    .to_tracing_value(),
);

// But be very careful with async boundaties. To see a complex Usage
// of manually created spans that are used across async boundaries
// see `run_tasks_with_listener` in `crates/dbt-tasks/src/task_runner.rs`
// and the associated nested spans in specific tasks like:
// `run_task` in `crates/dbt-tasks/src/runnable/mod.rs`
```

### About async

If your function spawns a task or awaits on an async operation, you must either:
- Instrument the async function itself with `#[instrument]`
- Or use `.in_current_span()` or ` to ensure the span context is preserved.

```rust
use tracing::Instrument;

#[tracing::instrument(level = "trace")]
async fn parent_function() {
    // This will automatically run in the same span as the function
    let result = child_function().in_current_span().await;
}

async fn non_instrumented() {
    let manual_span = tracing::info_span!("ManualSpan");

    // Here span is NOT entered and code runs in the parent span
    ...

    // But the async function can will enter and run in the manual span
    some_async_func().instrument(manual_span).await;
}
```

### Structured Event Logging

Notice that we always use a special `TRACING_ATTR_FIELD` to pass all data as structured attributes to the tracing system.

The goal is to never pass any unstructured data to the tracing system. Instead, we should record all data on corresponding `LogAttributes` variants and it should be sufficient to produce
any desired output format, including fancy colored tty logs & progress bars.

```rust
tracing::warn!(
    { TRACING_ATTR_FIELD } = LogAttributes::Log {
        code: Some(err.code as u16 as u32),
        dbt_core_code: None,
        original_severity_number,
        original_severity_text,
        location: RecordCodeLocation::none(), // Will be auto injected
    }.to_tracing_value(),
    "{}",
    err.pretty().as_str()
);
```

## Developer Debugging Features

### Developer Debugging with Argument Capture

Trace level spans are captured when `--log-level trace` is set via CLI argument.
This works in both debug and production builds, allowing for detailed debugging
in any environment when needed.

Functions instrumented at TRACE level automatically become `SpanAttributes::DevInternal`, capturing:
- Function name
- Code location (file, line, module)
- Function arguments (when --log-level trace is set)
- Custom debug fields


When using `#[instrument(level = "trace")]` and `--log-level trace` is set, function arguments are automatically captured:

```rust

// Notice skip_all is used to skip all arguments
#[instrument(skip_all, level = "trace")]
fn my_function(arg1: &str, arg2: i32) -> Result<String, Error> {
    // All function arguments are ignored, but a span is created
    do_work(arg1, arg2)
}

#[instrument(skip(big_fat_arg), level = "trace")]
fn my_other_function(big_fat_arg: &MegaStruct, arg2: i32) -> Result<String, Error> {
    // Function arguments are captured when --log-level trace is set
    do_work(arg1, arg2)
}
```

## Log Level Filtering

The tracing infrastructure respects the `--log-level` CLI argument in both debug and production builds and `RUST_LOG` environment variable ONLY in debug builds:

```bash
# Show all tracing output including developer traces
dbt --log-level trace run

# Show only spans and events from specific modules
RUST_LOG=dbt_tasks=debug,dbt_adapter=info dbt run

# Show only errors and warnings
dbt --log-level warn run
```

**Note**: The `RUST_LOG` environment variable is only respected in debug builds. In production builds, use the `--log-level` CLI argument instead.

## Legacy Log Bridge

The infrastructure includes a bridge that forwards existing `log` crate messages to the tracing system:

### Bridge Implementation
- **Location**: `fs/sa/crates/dbt-common/src/logging/logger.rs`
- **Purpose**: Captures legacy log messages and forwards them to tracing
- **Features**:
  - Converts log levels to tracing levels
  - Strips ANSI codes for structured output
  - Marks bridged messages to prevent double-processing
  - Maintains backward compatibility during migration

### Usage in Legacy Code
```rust
use log::{info, error};

// These will be automatically forwarded to tracing
info!("Legacy log message");
error!("Legacy error message");
```

## Configuration

As of time of writing tracing will only be enabled if `otm_file_name` CLI argument is set. It will use the log path to write telemetry data to a JSONL file.

## Best Practices

1. **Use structured attributes** for spans that need to be analyzed downstream
2. **Prefer `#[instrument]`** over manual span creation for functions
3. **Use TRACE level** for developer debugging spans with argument capture
4. **Always use `.in_current_span()`** for all futures that are not async functions, and all async operations that should inherit span context. Prefer instrumenting the async function itself.
5. **Avoid sensitive data** in span names or attributes - use `skip_all` liberally

## WIP

* Metrics infrastructure
* OpenTelemetry configuration
* Bridging to progress bars and CLI output
