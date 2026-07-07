use std::io::{self, Write};

/// Whether the writer should be treated as a terminal for **routing**
/// purposes (e.g. selecting console vs log-file telemetry filters).
///
/// * `FORCE_COLOR` set to any value other than `"0"` promotes a non-TTY
///   writer to terminal-like — see <https://force-color.org>. This lets
///   wrappers that pipe stdout (e.g. a parent process capturing output)
///   still opt into console-style output.
/// * `FORCE_COLOR=0` (or unset) is *not* a demotion: it just declines to
///   force, and terminal-ness falls back to the writer's actual TTY
///   status. Treating `FORCE_COLOR=0` as "force off" would drop
///   console-only telemetry on a real interactive terminal.
/// * `NO_COLOR` does **not** affect this — it only disables ANSI styling
///   (see [`resolve_use_color`]), not terminal-ness. Demoting a real TTY
///   here would drop console-only telemetry records, which is beyond
///   `NO_COLOR`'s intended scope.
pub fn resolve_is_terminal<W: SharedWriter + ?Sized>(writer: &W) -> bool {
    if std::env::var_os("FORCE_COLOR").is_some_and(|v| v != "0") {
        return true;
    }
    writer.is_terminal()
}

/// Whether ANSI styling should be emitted for output written to this
/// writer.
///
/// * `NO_COLOR` disables styling regardless of the writer's terminal
///   status. Presence-only: any value (including an empty string, as
///   produced by `export NO_COLOR` with no assignment) counts. Widely
///   honored convention used by many CLI tools.
/// * `FORCE_COLOR=0` also disables styling (matches the convention
///   established by npm's `supports-color` / `chalk`), without affecting
///   routing decisions in [`resolve_is_terminal`].
/// * Otherwise, returns [`resolve_is_terminal`].
pub fn resolve_use_color<W: SharedWriter + ?Sized>(writer: &W) -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if std::env::var_os("FORCE_COLOR").is_some_and(|v| v == "0") {
        return false;
    }
    resolve_is_terminal(writer)
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
