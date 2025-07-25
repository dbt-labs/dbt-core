use std::path::Path;
use std::time::SystemTime;
use tokio;

use crate::error::LiftableResult;
use crate::{FsResult, ectx};

/// Wrapper around [`tokio::fs::create_dir_all`] that returns a useful error in case of failure.
pub async fn create_dir_all(path: impl AsRef<Path>) -> FsResult<()> {
    let path = path.as_ref();
    tokio::fs::create_dir_all(path)
        .await
        .lift(ectx!("Failed to create directory: {}", path.display()))
}

/// Wrapper around [`tokio::fs::remove_dir_all`] that returns a useful error in case of failure.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> FsResult<()> {
    let path = path.as_ref();
    tokio::fs::remove_dir_all(path)
        .await
        .lift(ectx!("Failed to delete directory: {}", path.display()))
}

/// Wrapper around [`tokio::fs::read_to_string`] that returns a useful error in case of failure.
pub async fn read_to_string<P: AsRef<Path>>(path: P) -> FsResult<String> {
    let path = path.as_ref();
    tokio::fs::read_to_string(path)
        .await
        .lift(ectx!("Failed to read file: {}", path.display()))
}

/// Wrapper around [`tokio::fs::read`] that returns a useful error in case of failure.
pub async fn read(path: impl AsRef<Path>) -> FsResult<Vec<u8>> {
    let path = path.as_ref();
    tokio::fs::read(path)
        .await
        .lift(ectx!("Failed to read file: {}", path.display()))
}

/// Wrapper around [`tokio::fs::write`] that returns a useful error in case of failure.
pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> FsResult<()> {
    let path = path.as_ref();
    tokio::fs::write(path, contents)
        .await
        .lift(ectx!("Failed to write file: {}", path.display()))
}

/// Wrapper around [`tokio::fs::copy`] that returns a useful error in case of failure.
pub async fn copy(from: impl AsRef<Path>, to: impl AsRef<Path>) -> FsResult<u64> {
    let from = from.as_ref();
    let to = to.as_ref();
    tokio::fs::copy(from, to).await.lift(ectx!(
        "Failed to copy file {} to {}",
        from.display(),
        to.display()
    ))
}

/// Wrapper around [`tokio::fs::metadata`] + [`Metadata::modified`] that returns a useful error in case of failure.
pub async fn last_modified<P: AsRef<Path>>(path: P) -> FsResult<SystemTime> {
    let path = path.as_ref();
    tokio::fs::metadata(path)
        .await
        .and_then(|metadata| metadata.modified())
        .lift(ectx!(
            "Failed to get last modified time of: {}",
            path.display()
        ))
}

/// Wrapper around [`tokio::fs::rename`] that returns a useful error in case of failure.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> FsResult<()> {
    let from = from.as_ref();
    let to = to.as_ref();
    tokio::fs::rename(from, to).await.lift(ectx!(
        "Failed to rename file {} to {}",
        from.display(),
        to.display()
    ))
}

/// Wrapper around [`tokio::fs::read_dir`] that returns a useful error in case of failure.
pub async fn read_dir(path: impl AsRef<Path>) -> FsResult<tokio::fs::ReadDir> {
    let path = path.as_ref();
    tokio::fs::read_dir(path)
        .await
        .lift(ectx!("Failed to read directory: {}", path.display()))
}

pub struct File {}
impl File {
    /// Wrapper around [`tokio::fs::File::create`] that returns a useful error in case of failure.
    pub async fn create<P: AsRef<Path>>(path: P) -> FsResult<tokio::fs::File> {
        let path = path.as_ref();
        tokio::fs::File::create(path)
            .await
            .lift(ectx!("Failed to create file: {}", path.display()))
    }
}
