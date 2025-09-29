# Tracing Infrastructure

This module provides a comprehensive tracing infrastructure for Fusion, serving multiple purposes:
1. **Unified span & log data layer** - The single source of truth for all operations and events in the system
2. **Structured telemetry** - Capturing application performance data and metrics for downstream systems (e.g. cloud clients, orchestration, metadata etc.)
3. **Interactive user experience** - [TBD] Formats data for CLI and user logs
2. **Developer debugging** - Providing rich debugging information compiled away in release builds

## Architecture Overview

The tracing infrastructure follows a layered architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Tracing Facade                               │
│                (tracing crate API)                              │
│  - tracing::instrument,                                         │
   - crate::tracing::emit::create_info_span!, etc.                │
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
│  │ JSONL Writer Layer  │ │    OTLP Exporter Layer          │    │
│  │  (File/Stdout)      │ │  (OpenTelemetry Protocol)       │    │
│  │  - JSONL format     │ │  - Exports spans and logs       │    │
│  └─────────────────────┘ └─────────────────────────────────┘    │
│  ┌─────────────────────┐                                        │
│  │ Parquet Writer      │                                        │
│  │  (Arrow/Parquet)    │                                        │
│  │  - Batch + flush    │                                        │
│  └─────────────────────┘                                        │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │          CLI & User Logs Layer                          │    │
│  │              (TTY-friendly UX)                          │    │
│  │   - Pretty formatting for terminal output               │    │
│  │   - Progress bars and interactive elements              │    │
│  │   - User-facing log messages                            │    │
│  │   - Disabled when log-format is OTEL                    │    │
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
  - Auto-injects code location (file, line, module, target)
  - Stores telemetry data in span extensions for writer layers

#### Auto Context Injection

The data layer enriches every span and log with execution context and location:
- Context: currently phase and node `unique_id` (from surrounding evaluation spans). The context type is extensible and may include additional fields over time.
- Code location: injected at the callsite for both spans and logs. In release builds location may be stripped unless explicitly enabled.

### Writing Layers
- JSONL Writer (`TelemetryJsonlWriterLayer`): Writes JSONL to file when configured; emits JSONL to stdout when `--log-format otel` is used.
- Parquet Writer (`TelemetryParquetWriterLayer`): Writes Arrow/Parquet to `{target_path}/metadata/...` when configured.
- OTLP Exporter (`OTLPExporterLayer`): Exports spans and logs to an OTLP backend (endpoint via OTEL env vars).
- Progress Bar Layer: Provides interactive progress; enabled for default log format, disabled in OTEL mode.

### Telemetry Records
All telemetry data follows structured schemas defined in `dbt-telemetry/src/schemas/record.rs`:
- **SpanStartInfo**: Emitted when spans begin
- **SpanEndInfo**: Emitted when spans complete
- **LogRecordInfo**: Emitted for log events within spans

## Usage Examples

CAVEAT: as of time of writing we are in transitioning from legacy `log` crate to `tracing` crate. Most of the logging is still done via `log!` based macros.

### Basic Span Instrumentation

```rust
let _sp = create_info_span!(
    ArtifactWritten {
        artifact_type: artifact_type as i32,
        relative_path: rel_path,
    }
    .into()
)
.entered();
```

TODO: document recording span status

### About async & thread spawning

If your function spawns a thread, a task or awaits on an async operation, you must either:
- Instrument the async function itself with `#[instrument]`
- Or use `.in_current_span()` to ensure the span context is preserved.

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
use dbt_common::tracing::emit::create_info_span;

create_info_span!(
    _e = ?store_event_attributes(LogAttributes::Log {
        code: Some(err.code as u16 as u32),
        dbt_core_code: None,
        original_severity_number,
        original_severity_text,
        location: RecordCodeLocation::none(), // Will be auto injected
    }),
    "{}",
    err.pretty().as_str()
);
```

## Developer Debugging Features

### Developer Debugging with Argument Capture

Trace level spans are captured when `--log-level trace` is set via CLI argument.
This works in both debug and production builds, allowing for detailed debugging
in any environment when needed.

Functions instrumented at TRACE level without a telemetry data struct automatically become `CallTrace`, capturing:
- Function name
- Code location (file, line, module)
- All structured fields passed to instrumentation, including function arguments if you are using `#[instrument(level = "trace")]`


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

## Exporters and Configuration

Tracing output is controlled by configuration and event-level export flags:

- JSONL file: enabled via `--otel-file-name` cli arg, written under the resolved log path.
- JSONL stdout: enabled automatically when `--log-format otel` is selected.
- Parquet file: enabled via `--otel-parquet-file-name`, written to `{target_path}/metadata/...`.
- OTLP export: enabled via the CLI/export flag and standard OTEL env vars for endpoints.

Each event carries `TelemetryOutputFlags` flags that determine destinations. Writers honor these flags:
- `EXPORT_JSONL` → JSONL writers only
- `EXPORT_PARQUET` → Parquet writer only
- `EXPORT_OTLP` → OTLP exporter only
- `EXPORT_ALL` and `EXPORT_JSONL_AND_OTLP` for convenience
- `OUTPUT_CONSOLE` -> output on the console
- `OUTPUT_LOG_FILE` -> output to unstructured log file (i.e. dbt.log)

## Best Practices

1. **Use structured attributes** for spans that need to be analyzed downstream
2. **Prefer `#[instrument]`** over manual span creation for functions
3. **Use TRACE level** for developer debugging spans with argument capture
4. **Always use `.in_current_span()`** for all futures that are not async functions, and all async operations that should inherit span context. Prefer instrumenting the async function itself.

## WIP

* Metrics infrastructure
* Bridging to progress bars and CLI output
