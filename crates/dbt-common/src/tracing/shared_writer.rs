use std::io::{self, Write};

/// A trait for threadsafe writers used by tracing layers.
pub trait SharedWriter: Send + Sync {
    /// Write data to the underlying writer.
    fn write(&self, data: &str) -> io::Result<()>;

    /// Write data followed by a newline to the underlying writer.
    fn writeln(&self, data: &str) -> io::Result<()>;

    fn is_terminal(&self) -> bool {
        false
    }
}

impl SharedWriter for io::Stdout {
    fn write(&self, data: &str) -> io::Result<()> {
        // Lock stdout for the duration of the write operation
        let mut handle = self.lock();

        // Write the data
        handle.write_all(data.as_bytes())?;

        // Immediately flush to ensure data is written
        handle.flush()?;

        Ok(())
    }

    fn writeln(&self, data: &str) -> io::Result<()> {
        // Lock stdout for the duration of the write operation
        let mut handle = self.lock();

        // Write the data
        handle.write_all(data.as_bytes())?;
        handle.write_all(b"\n")?;

        // Immediately flush to ensure data is written
        handle.flush()?;

        Ok(())
    }

    fn is_terminal(&self) -> bool {
        io::IsTerminal::is_terminal(self)
    }
}
