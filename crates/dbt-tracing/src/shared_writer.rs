use std::io::{self, Write};

/// Decide whether output should be treated as a color-capable terminal.
///
/// Honors the widely-adopted environment conventions before falling back to
/// the writer's actual TTY status:
///
/// * `NO_COLOR` (any non-empty value) forces a non-terminal result.
///   Widely honored convention used by many CLI tools.
/// * `FORCE_COLOR` (any value other than `"0"`) forces a terminal result —
///   see <https://force-color.org>. This lets wrappers that pipe stdout
///   (e.g. a parent process capturing output) still opt into ANSI output.
/// * Otherwise, defers to [`SharedWriter::is_terminal`].
pub fn resolve_is_terminal<W: SharedWriter + ?Sized>(writer: &W) -> bool {
    if std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()) {
        return false;
    }
    if let Some(v) = std::env::var_os("FORCE_COLOR") {
        return v != "0";
    }
    writer.is_terminal()
}

/// A trait for threadsafe writers used by tracing layers.
///
/// Writers implementing this trait are expected to handle errors internally.
/// For background/async writers, errors should be stored and reported during shutdown.
/// For synchronous writers (stdout/stderr), unrecoverable errors should panic.
///
/// This infallible design simplifies consumer code and reflects the reality that
/// telemetry write errors are typically non-recoverable at the call site.
pub trait SharedWriter: Send + Sync {
    /// Write data to the underlying writer.
    ///
    /// Implementations must handle errors internally. Background writers should
    /// store errors for later reporting during shutdown. Synchronous writers
    /// should panic on unrecoverable errors.
    fn write(&self, data: &str);

    /// Write data followed by a newline to the underlying writer.
    ///
    /// Implementations must handle errors internally. Background writers should
    /// store errors for later reporting during shutdown. Synchronous writers
    /// should panic on unrecoverable errors.
    fn writeln(&self, data: &str);

    fn is_terminal(&self) -> bool {
        false
    }
}

impl SharedWriter for io::Stdout {
    fn write(&self, data: &str) {
        // Lock stdout for the duration of the write operation
        let mut handle = self.lock();

        // Write the data, panic on error as this is unrecoverable
        handle
            .write_all(data.as_bytes())
            .expect("failed to write to stdout");

        // Immediately flush to ensure data is written
        handle.flush().expect("failed to flush stdout");
    }

    fn writeln(&self, data: &str) {
        // Lock stdout for the duration of the write operation
        let mut handle = self.lock();

        // Write the data, panic on error as this is unrecoverable
        handle
            .write_all(data.as_bytes())
            .expect("failed to write to stdout");
        handle.write_all(b"\n").expect("failed to write to stdout");

        // Immediately flush to ensure data is written
        handle.flush().expect("failed to flush stdout");
    }

    fn is_terminal(&self) -> bool {
        io::IsTerminal::is_terminal(self)
    }
}

impl SharedWriter for io::Stderr {
    fn write(&self, data: &str) {
        // Lock stderr for the duration of the write operation
        let mut handle = self.lock();

        // Write the data, panic on error as this is unrecoverable
        handle
            .write_all(data.as_bytes())
            .expect("failed to write to stderr");

        // Immediately flush to ensure data is written
        handle.flush().expect("failed to flush stderr");
    }

    fn writeln(&self, data: &str) {
        // Lock stderr for the duration of the write operation
        let mut handle = self.lock();

        // Write the data, panic on error as this is unrecoverable
        handle
            .write_all(data.as_bytes())
            .expect("failed to write to stderr");
        handle.write_all(b"\n").expect("failed to write to stderr");

        // Immediately flush to ensure data is written
        handle.flush().expect("failed to flush stderr");
    }

    fn is_terminal(&self) -> bool {
        io::IsTerminal::is_terminal(self)
    }
}
