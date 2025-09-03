use std::io::{self, Write};

/// A trait for threadsafe writers used by tracing layers.
pub trait SharedWriter: Send + Sync {
    fn write(&self, data: &str) -> io::Result<()>;
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
}
