# Tracing Infrastructure

This module provides a comprehensive tracing infrastructure for Fusion, serving multiple purposes:
1. **Unified span & log data layer** - The single source of truth for all operations and events in the system
2. **Structured telemetry** - Capturing application performance data and metrics for downstream systems (e.g. cloud clients, orchestration, metadata etc.)
3. **Interactive user experience** - Formats data for CLI TUI with progress bars and rich output
4. **Developer debugging** - Providing rich debugging information at trace level

## Performance Investigation with Telemetry

The `cargo xtask telemetry` command spins up a local Jaeger UI that can be used to investigate performance issues.

Start Jaeger for telemetry visualization:
```bash
# Start Jaeger
cargo xtask telemetry

# Run Fusion with telemetry enabled
OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318" cargo run -p dbt-cli -- --export-to-otlp <your-dbt-commands>

# Access Jaeger UI at http://localhost:16686

# [Optional] Stop Jaeger
cargo xtask telemetry --stop
```

## Architecture Overview

Our tracing infrastructure is built on top of `tracing` Registry and layers, but uses bespoke middleware and consumer abstractions to expand on and provide a safer, more convenient API than the native `tracing` library:

```
┌────────────────────────────────────────────────────────────────┐
│                      Application Code                          │
│     tracing::instrument, create_info_span!, etc.               │
└────────────────────────┬───────────────────────────────────────┘
                         │
┌────────────────────────▼───────────────────────────────────────┐
│              TelemetryDataLayer (tracing native Layer)         │
│  - Generates globally unique span/event IDs                    │
│  - Correlates data with trace ID (from invocation UUID)        │
│  - Auto-injects code location (file, line, module)             │
│  - Converts spans/events to structured telemetry records       │
│  - Drives middleware and consumer layers, handles span         │
│    filtering and exposes data provider for thread-safe         │
│    span extensions and metrics storage                         │
└────────────────────────┬───────────────────────────────────────┘
                         │
┌────────────────────────▼───────────────────────────────────────┐
│                  Middleware Pipeline                           │
│  - Can modify or drop spans and log records                    │
│  - Mutable access to metrics via DataProviderMut               │
│  - Example: TelemetryMetricAggregator                          │
└────────────────────────┬───────────────────────────────────────┘
                         │
┌────────────────────────▼───────────────────────────────────────┐
│                    Consumer Layers                             │
│  - Read-only consumers of processed telemetry data             │
│  - Can filter spans and logs independently                     │
│  - Access to metrics via DataProvider                          │
│                                                                │
│  Examples:                                                     │
│  - JSONLWriter: JSONL output to file/stdout                    │
│  - ParquetWriter: Arrow/Parquet to metadata/                   │
│  - OTLPLayer: OpenTelemetry Protocol export                    │
│  - TUILayer: All console output.                               │
│  - FileLogLayer: Unstructured dbt.log output                   │
└────────────────────────────────────────────────────────────────┘
```

### Why Custom Abstractions?

The native `tracing` library has several limitations:
1. No thread-safe storage for event data (only for spans)
2. Cannot pass arbitrary structured data through span/log facades
3. Per-layer filtering lacks access to span/log data
4. Runtime layer reloading doesn't work with filtered layers ([issue](https://github.com/tokio-rs/tracing/issues/1629))
5. Span extension, that provide per-span storage susceptible to self-deadlocks

Our custom `TelemetryMiddleware` and `TelemetryConsumer` traits provide:
- Stricter, safer APIs with clear read/write semantics
- Access to structured telemetry data for filtering
- Thread-safe metric tracking
- Runtime reloadability for testing

## Core Components

### TelemetryDataLayer
The bridge between native `tracing` and our custom infrastructure. It:
- Converts tracing spans/events to structured telemetry records
- Generates globally unique IDs across the entire process
- Auto-injects code location and execution context
- Stores telemetry data in span extensions
- Dispatches to middleware and consumers

#### Auto Context Injection

The data layer enriches every span and log with execution context and location:
- Context: currently phase and node `unique_id` (from surrounding evaluation spans). The context type is extensible and may include additional fields over time.
- Code location: injected at the callsite for both spans and logs. In release builds location is stripped.
- 
### TelemetryMiddleware
Middleware that can transform telemetry data before it reaches consumers. Receives mutable access via `DataProviderMut` to:
- Modify or drop spans and log records
- Increment metrics stored in root span
- Initialize custom extensions in root span

Each callback returns `Option<T>` - return `None` to drop the event from all consumers.

**Example**: [`TelemetryMetricAggregator`](/fs/sa/crates/dbt-common/src/tracing/middlewares/metric_aggregator.rs) - Aggregates node outcomes and updates invocation-level metrics.

See trait definition in [`layer.rs`](/fs/sa/crates/dbt-common/src/tracing/layer.rs).

### TelemetryConsumer
Read-only consumers that process telemetry data. Consumers can:
- Filter spans and logs independently via `is_span_enabled()` and `is_log_enabled()`
- Access metrics via `DataProvider`
- Access custom extensions in root span

**Examples**:
- [`TelemetryJsonlWriterLayer`](/fs/sa/crates/dbt-common/src/tracing/layers/jsonl_writer.rs) - Writes JSONL to file/stdout
- [`OTLPLayer`](/fs/sa/crates/dbt-common/src/tracing/layers/otlp.rs) - Exports to OpenTelemetry Protocol endpoints
- [`ParquetWriter`](/fs/sa/crates/dbt-common/src/tracing/layers/parquet_writer.rs) - Writes Arrow/Parquet format
- [`PrettyWriter`](/fs/sa/crates/dbt-common/src/tracing/layers/pretty_writer.rs) - Formatted CLI output
- [`TUILayer`](/fs/sa/crates/dbt-common/src/tracing/layers/tui_layer.rs) - Interactive progress bars

See trait definition in [`layer.rs`](/fs/sa/crates/dbt-common/src/tracing/layer.rs).

### Data Providers

**DataProvider** (read-only): Provides safe, controlled read-only access to metrics and custom extensions stored in the root span.

**DataProviderMut** (mutable): Extends `DataProvider` with mutable operations - incrementing metrics and initializing extensions.

See full implementation in [`data_provider.rs`](/fs/sa/crates/dbt-common/src/tracing/data_provider.rs).

## Usage Examples

CAVEAT: as of time of writing we are in transitioning from legacy `log` crate to `tracing` crate. Most of the logging is still done via `log!` based macros.

### Creating a Middleware

```rust
use dbt_telemetry::{SpanEndInfo, LogRecordInfo};
use crate::tracing::{TelemetryMiddleware, DataProviderMut, MetricKey};

/// Example middleware that counts spans
pub struct SpanCounter;

impl TelemetryMiddleware for SpanCounter {
    fn on_span_end(
        &self,
        span: SpanEndInfo,
        data_provider: &mut DataProviderMut<'_>,
    ) -> Option<SpanEndInfo> {
        // Increment a custom metric
        data_provider.increment_metric(
            MetricKey::Custom("span_count"),
            1
        );

        // Pass span through to consumers
        Some(span)
    }
}
```

### Creating a Consumer

```rust
use dbt_telemetry::{SpanEndInfo, TelemetryOutputFlags};
use crate::tracing::{TelemetryConsumer, DataProvider};

/// Example consumer that logs span durations
pub struct DurationLogger;

impl TelemetryConsumer for DurationLogger {
    fn is_span_enabled(&self, span: &SpanStartInfo, _meta: &Metadata) -> bool {
        // Only process spans marked for console output
        span.attributes
            .output_flags()
            .contains(TelemetryOutputFlags::OUTPUT_CONSOLE)
    }

    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &DataProvider<'_>) {
        if let Some(duration) = span.end_time_unix_nanos
            .checked_sub(span.start_time_unix_nanos)
        {
            println!("Span {} took {}ms", span.name, duration / 1_000_000);
        }
    }
}
```

### Using Filters

Consumers can be filtered using closures:

```rust
use tracing::Level;

// Filter spans by level
let consumer = MyConsumer::new()
    .with_span_filter(|span, meta| meta.level() <= &Level::INFO);

// Filter logs by severity
let consumer = MyConsumer::new()
    .with_log_filter(|log, _| log.severity_number >= SeverityNumber::Warn);
```

### Instrumenting Code

#### Creating Spans

```rust
use dbt_common::create_info_span;

// Create a structured span with attributes
let _sp = create_info_span(
    ArtifactWritten {
        artifact_type: artifact_type as i32,
        relative_path: rel_path,
    }
).entered();
```

#### Creating Log Events

```rust
use dbt_common::tracing::emit::emit_trace_event;
use dbt_telemetry::LogMessage;
use tracing::Level as TracingLevel;

// Emit a log event with structured attributes
emit_trace_event(
    level: TracingLevel::WARN,
    LogMessage {
        code: Some(err.code as u16 as u32),
        dbt_core_event_code: None,
        original_severity_number: original_severity_number as i32,
        original_severity_text: original_severity_text.to_string(),
        // phase, unique_id, file, line are auto-injected
        phase: None,
        unique_id: None,
        file: None,
        line: None,
    }.into(),
    "{}",
    err.pretty().as_str()
);

// Or without a message (defaults to INFO level)
emit_trace_event(
    MyLogAttributes { /* ... */ }
);
```

### Async & Thread Spawning

**Important**: Spawning threads or tasks creates a boundary where span context must be explicitly propagated. If your function spawns a thread, a task, or awaits on an async operation, you must either:
- Instrument the async function itself with `#[instrument]`
- Or use `.in_current_span()` to ensure the span context is preserved

#### Async Functions

```rust
use tracing::Instrument as _;

// Async function itself can be instrumented and then all calls to it
// will be in the correct span context
#[tracing::instrument(
    skip_all,
    fields(
        // `The name of _e` is irrelevant, but just a convention to avoid collisions.
        // `= ?` is not a typo, it should be used (see doc comment for details)
        _e = ?store_event_attributes(PhaseExecuted::start_general(ExecutionPhase::LoadProject)),
    )
)]
async fn parent_function() {
    // Automatically runs in the function's span
    let result = child_function().await;
}

async fn manual_span_example() {
    let manual_span = tracing::info_span!("ManualSpan");

    // Here span is NOT entered and code runs in the parent span
    // ...

    // But the async function can will enter and run in the manual span
    some_async_func().instrument(manual_span).await;
}
```

#### Thread and Task Spawning

When spawning threads or tokio tasks, capture the current span and propagate it:

```rust
use std::thread;
use tokio::task;

fn spawn_thread_example() {
    let current_span = tracing::Span::current();

    thread::spawn(move || {
        let _guard = current_span.enter();
        // Thread code runs in the captured span context
        do_work();
    });
}

async fn spawn_task_example() {
    let current_span = tracing::Span::current();

    task::spawn(async move {
        let _guard = current_span.enter();
        // Task code runs in the captured span context
        do_async_work().await;
    });
}
```

## Developer Debugging

Use `--log-level trace` to capture detailed function traces:

```rust
// Capture all arguments (skipping large ones)
#[instrument(skip(big_fat_arg), level = "trace")]
fn my_function(big_fat_arg: &MegaStruct, arg2: i32) -> Result<String, Error> {
    // Function arguments are captured when --log-level trace is set
    do_work(big_fat_arg, arg2)
}
```

Trace-level spans without telemetry attributes become `CallTrace` records, capturing:
- Function name
- In debug builds only:
  - Code location (file, line, module)
  - All structured fields including function arguments

## Log Level Filtering

Control tracing output via CLI or environment:

```bash
# Show all tracing including developer traces
dbt --log-level trace run

# Show only errors and warnings
dbt --log-level warn run

# Module-specific filtering (debug builds only)
RUST_LOG=dbt_tasks=debug,dbt_adapter=info dbt run
```

**Note**: `RUST_LOG` only works in debug builds. Use `--log-level` in release builds.

## Exporters and Configuration

Telemetry output is controlled by user via cli args, enabling different outputs (consumers) and setting log level.
Within application, each strucutred event type (span or log) defines `TelemetryOutputFlags` that determine which exporters (consumers) will receive it.

Cli options for enabling exporters:
- **TUI with progress bars**: the default (same as specifying `--log-format default`)
- **Non-interactive console output**: `--log-format text`
- **Unstructured dbt.log file**: enabled by default, `--log-path` allows customing log dir relative to project dir. Log is written to `{log_path}/dbt.log`
- **JSONL to file**: `--otel-file-name` - written to `{log_path}/`
- **JSONL on stdout**: `--log-format otel` - writes JSONL to console
- **Parquet file**: `--otel-parquet-file-name` - written to `{target_path}/metadata/`
- **OTLP export**: `--export-to-otlp` will send via OTLP protocol to endpoint set by the canonical OTEL env var: `OTEL_EXPORTER_OTLP_ENDPOINT`

Each telemetry event type has flags that determine destinations:
- `EXPORT_JSONL` → JSONL writers
- `EXPORT_PARQUET` → Parquet writer
- `EXPORT_OTLP` → OTLP exporter
- `EXPORT_ALL` → All exporters
- `OUTPUT_CONSOLE` → Console/TUI
- `OUTPUT_LOG_FILE` → Unstructured dbt.log

## Best Practices

1. **Use structured attributes** for spans that need downstream analysis
2. **Prefer `#[instrument]`** over manual span creation when instrumenting async functions
3. **Use TRACE level** for developer debugging with argument capture. Do NOT use DEBUG level for this purpose. DEBUG is still considered a user-facing log level per dbt-core conventions.
4. **Always use `.in_current_span()`** for futures not in async functions
5. **Implement filtering in `is_*_enabled`** to avoid processing overhead
6. **Use middleware for metrics** to keep consumers read-only and simple
