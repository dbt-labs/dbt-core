use core::fmt;
use std::ffi::OsString;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, io};

use adbc_core::error::{Error, Status};
use percent_encoding::AsciiSet;
use sha2::{Digest, Sha256};
use ureq::tls::{RootCerts, TlsConfig, TlsProvider};

use crate::checksums::SORTED_CDN_DRIVER_CHECKSUMS;
use crate::{
    BIGQUERY_DRIVER_VERSION, Backend, DATABRICKS_DRIVER_VERSION, POSTGRES_DRIVER_VERSION,
    REDSHIFT_DRIVER_VERSION, SALESFORCE_DRIVER_VERSION, SNOWFLAKE_DRIVER_VERSION,
};

static INSTALLABLE_DRIVERS: &[Backend; 6] = &[
    Backend::Snowflake,
    Backend::BigQuery,
    Backend::Postgres,
    Backend::Databricks,
    Backend::Redshift,
    Backend::Salesforce,
];

#[derive(Debug)]
pub enum InstallError {
    /// Generic HTTP error. Set up of the client of request failed.
    Http(ureq::Error),
    /// Error while generating a random file name (very unlikely).
    GetRandom(getrandom::Error),
    /// Unable to determine the cache directory for ADBC driver installation.
    DetermineCacheDir,
    /// Generic IO error.
    Io(io::Error),
    /// Error while decompressing the CDN driver file.
    ZstdDecompress(usize),
    /// Error while creating the destination directory for the driver file.
    CreateDir(io::Error, PathBuf),
    /// Error while creating the destination file for the driver.
    CreateFIle(io::Error, PathBuf),
    /// Error while writing the driver file.
    WriteFile(io::Error),
    /// Error while syncing the driver file to storage media.
    SyncFile(io::Error),
    /// Error while atomically renaming the driver file to its final name.
    RenameFile(io::Error),
    /// SHA256 checksum mismatch: expected {}, got {} (URL: {}).
    ChecksumMismatch(String, String, String),
}

impl fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstallError::Http(error) => write!(f, "HTTP error: {error}"),
            InstallError::GetRandom(error) => write!(f, "getrandom error: {error}"),
            InstallError::DetermineCacheDir => write!(
                f,
                "Unable to determine cache directory for ADBC driver installation in this platform."
            ),

            InstallError::Io(error) => write!(f, "IO error: {error}"),
            InstallError::ZstdDecompress(code) => {
                let msg = zstd_safe::get_error_name(*code);
                write!(f, "Decompression error: {msg}")
            }
            InstallError::CreateDir(error, path_buf) => {
                write!(
                    f,
                    "Unable to create directory {}: {}",
                    path_buf.display(),
                    error
                )
            }
            InstallError::CreateFIle(error, path_buf) => {
                write!(f, "Unable to create file {}: {}", path_buf.display(), error)
            }
            InstallError::WriteFile(error) => write!(f, "Unable to write file: {error}"),
            InstallError::SyncFile(error) => write!(f, "Unable to sync file: {error}"),
            InstallError::RenameFile(error) => write!(f, "Unable to rename file: {error}"),
            InstallError::ChecksumMismatch(expected, got, url) => {
                write!(
                    f,
                    "SHA-256 checksum mismatch: expected {expected}, got {got} (URL: {url})"
                )
            }
        }
    }
}

impl InstallError {
    fn status_for_io_error(e: &io::Error) -> Status {
        match e.kind() {
            io::ErrorKind::NotFound => Status::NotFound,
            io::ErrorKind::PermissionDenied => Status::Unauthorized,
            io::ErrorKind::ConnectionRefused => Status::Unauthorized,
            io::ErrorKind::ConnectionReset => Status::Cancelled,
            io::ErrorKind::HostUnreachable => Status::IO,
            io::ErrorKind::NetworkUnreachable => Status::IO,
            io::ErrorKind::ConnectionAborted => Status::Cancelled,
            io::ErrorKind::NotConnected => Status::InvalidState,
            io::ErrorKind::AddrInUse => Status::IO,
            io::ErrorKind::AddrNotAvailable => Status::IO,
            io::ErrorKind::NetworkDown => Status::IO,
            io::ErrorKind::BrokenPipe => Status::IO,
            io::ErrorKind::AlreadyExists => Status::AlreadyExists,
            io::ErrorKind::WouldBlock => Status::IO,
            io::ErrorKind::NotADirectory => Status::InvalidArguments,
            io::ErrorKind::IsADirectory => Status::InvalidArguments,
            io::ErrorKind::DirectoryNotEmpty => Status::InvalidState,
            io::ErrorKind::ReadOnlyFilesystem => Status::InvalidState,
            io::ErrorKind::StaleNetworkFileHandle => Status::InvalidState,
            io::ErrorKind::InvalidInput => Status::InvalidArguments,
            io::ErrorKind::InvalidData => Status::InvalidData,
            io::ErrorKind::TimedOut => Status::Timeout,
            io::ErrorKind::WriteZero => Status::IO,
            io::ErrorKind::StorageFull => Status::IO,
            io::ErrorKind::NotSeekable => Status::IO,
            io::ErrorKind::FileTooLarge => Status::IO,
            io::ErrorKind::ResourceBusy => Status::IO,
            io::ErrorKind::ExecutableFileBusy => Status::IO,
            io::ErrorKind::Deadlock => Status::InvalidState,
            io::ErrorKind::TooManyLinks => Status::IO,
            io::ErrorKind::ArgumentListTooLong => Status::InvalidArguments,
            io::ErrorKind::Interrupted => Status::Cancelled,
            io::ErrorKind::Unsupported => Status::NotImplemented,
            io::ErrorKind::UnexpectedEof => Status::InvalidData,
            io::ErrorKind::OutOfMemory => Status::Internal,
            io::ErrorKind::Other => Status::Internal,
            _ => Status::Internal,
        }
    }

    pub fn to_adbc_error(&self) -> Error {
        let status = match self {
            InstallError::Http(e) => match e {
                ureq::Error::StatusCode(_) => Status::Internal,
                ureq::Error::Http(_) => Status::Internal,
                ureq::Error::BadUri(_) => Status::InvalidArguments,
                ureq::Error::Protocol(_) => Status::InvalidData,
                ureq::Error::Io(e) => Self::status_for_io_error(e),
                ureq::Error::Timeout(_) => Status::Timeout,
                ureq::Error::HostNotFound => Status::NotFound,
                ureq::Error::RedirectFailed => Status::InvalidState,
                ureq::Error::InvalidProxyUrl => Status::InvalidArguments,
                ureq::Error::ConnectionFailed => Status::Cancelled,
                ureq::Error::BodyExceedsLimit(_) => Status::InvalidData,
                ureq::Error::TooManyRedirects => Status::Internal,
                ureq::Error::Tls(_) => Status::Internal,
                ureq::Error::Pem(_) => Status::InvalidData,
                ureq::Error::Rustls(_) => Status::Internal,
                ureq::Error::RequireHttpsOnly(_) => Status::InvalidArguments,
                ureq::Error::LargeResponseHeader(_, _) => Status::InvalidData,
                ureq::Error::Decompress(_, _) => Status::InvalidData,
                ureq::Error::Json(_) => Status::InvalidData,
                ureq::Error::ConnectProxyFailed(_) => Status::Cancelled,
                ureq::Error::TlsRequired => Status::InvalidArguments,
                ureq::Error::Other(_) => Status::Internal,
                _ => Status::Internal,
            },
            InstallError::GetRandom(_) => Status::Internal,
            InstallError::DetermineCacheDir => Status::Internal,
            InstallError::Io(e) => Self::status_for_io_error(e),
            InstallError::ZstdDecompress(_) => Status::InvalidData,
            InstallError::CreateDir(_, _) => Status::IO,
            InstallError::CreateFIle(_, _) => Status::IO,
            InstallError::WriteFile(_) => Status::IO,
            InstallError::SyncFile(_) => Status::IO,
            InstallError::RenameFile(_) => Status::IO,
            InstallError::ChecksumMismatch(_, _, _) => Status::InvalidData,
        };
        let message = format!("Driver installation error: {self}");
        Error::with_message_and_status(message, status)
    }
}

pub fn format_driver_url(backend_name: &str, version: &str, os: &str) -> String {
    const PUBLIC_DBT_CDN: &str = "public.cdn.getdbt.com";

    // %-encode most non-alphanumeric characters in the version string
    const NON_ALPHANUMERIC: &AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_');
    format!(
        "https://{}/fs/adbc/{}/adbc_driver_{}-{}-{}-{}{}.zst",
        PUBLIC_DBT_CDN,
        backend_name,
        backend_name,
        percent_encoding::utf8_percent_encode(version, NON_ALPHANUMERIC),
        env::consts::ARCH,
        os,
        env::consts::DLL_SUFFIX
    )
}

/// Format the full path to the driver file in the OS cache directory.
///
/// Examples for each platform:
///
/// Linux
///
/// ${XDG_CACHE_HOME}/com.getdbt/adbc/x86_64-unknown-linux-musl/libadbc_driver_snowflake-0.17.0+dbt0.0.1.so
/// or ${HOME}/.cache/com.getdbt/adbc/x86_64-unknown-linux-musl/libadbc_driver_snowflake-0.17.0+dbt0.0.1.so
///
/// macOS
///
/// ${HOME}/Library/Caches/com.getdbt/adbc/aarch64-macos/libadbc_driver_snowflake-0.17.0+dbt0.0.1.dylib
///
/// Windows
///
/// ${FOLDERID_LocalAppData}/com.getdbt/adbc/x86_64-pc-windows-msvc/adbc_driver_snowflake-0.17.0+dbt0.0.1.dll
pub fn format_driver_path(backend_name: &str, version: &str, os: &str) -> Result<PathBuf> {
    const APP_ID: &str = "com.getdbt";
    dirs::cache_dir()
        .map(|cache_dir| {
            let driver_relpath = format!(
                "{}/adbc/{}-{}/{}adbc_driver_{}-{}{}",
                APP_ID,
                env::consts::ARCH,
                os,
                env::consts::DLL_PREFIX,
                backend_name,
                version,
                env::consts::DLL_SUFFIX
            );
            cache_dir.join(driver_relpath)
        })
        .ok_or(InstallError::DetermineCacheDir)
}

pub type Result<T> = std::result::Result<T, InstallError>;

/// XDBC users can call this function to pre-install the driver for the given backend.
///
/// Instead of relying on the automatic installation at connection creation time.
pub fn pre_install_driver(backend: Backend) -> Result<()> {
    if !is_installable_driver(backend) {
        return Ok(());
    }
    let (backend_name, version, target_os) = driver_parameters(backend);
    install_driver_internal(backend_name, version, target_os)
}

/// Pre-install all supported drivers for the current platform.
pub fn pre_install_all_drivers() -> Result<()> {
    for backend in INSTALLABLE_DRIVERS.iter() {
        pre_install_driver(*backend)?;
    }
    Ok(())
}

pub fn is_installable_driver(backend: Backend) -> bool {
    INSTALLABLE_DRIVERS.contains(&backend)
}

#[allow(dead_code)]
const LINUX_TARGET_OS: &str = "manylinux_2_17-linux-gnu";
#[allow(dead_code)]
const MACOS_TARGET_OS: &str = "apple-darwin";
#[allow(dead_code)]
const WINDOWS_TARGET_OS: &str = "pc-windows-msvc";

pub fn driver_parameters(
    backend: Backend,
) -> (
    &'static str, // backend_name
    &'static str, // version
    &'static str, // target_os
) {
    #[cfg(target_os = "linux")]
    const OS: &str = LINUX_TARGET_OS;
    #[cfg(target_os = "macos")]
    const OS: &str = MACOS_TARGET_OS;
    #[cfg(target_os = "windows")]
    const OS: &str = WINDOWS_TARGET_OS;

    debug_assert!(is_installable_driver(backend));
    let (backend_name, version) = match backend {
        Backend::Snowflake => ("snowflake", SNOWFLAKE_DRIVER_VERSION),
        Backend::BigQuery => ("bigquery", BIGQUERY_DRIVER_VERSION),
        Backend::Postgres => ("postgresql", POSTGRES_DRIVER_VERSION),
        Backend::Databricks => ("databricks", DATABRICKS_DRIVER_VERSION),
        Backend::Redshift => ("redshift", REDSHIFT_DRIVER_VERSION),
        Backend::Salesforce => ("salesforce", SALESFORCE_DRIVER_VERSION),
        _ => unreachable!("driver_parameters() called with backend={:?}", backend),
    };
    (backend_name, version, OS)
}

fn find_expected_checksum_internal(
    backend_name: &str,
    version: &str,
    os: &str,
    arch: &str,
) -> Option<&'static str> {
    let checksums = SORTED_CDN_DRIVER_CHECKSUMS.as_ref();
    for i in 0..checksums.len() - 1 {
        debug_assert!(
            checksums[i] < checksums[i + 1],
            "SORTED_CDN_DRIVER_CHECKSUMS must be sorted"
        );
    }
    let query = (backend_name, os, arch, version);
    checksums
        .binary_search_by(|(elem, _)| elem.cmp(&query))
        .ok()
        .map(|index| checksums[index].1)
}

/// Find the expected SHA-256 checksum for the compressed driver file.
fn find_expected_checksum(backend_name: &str, version: &str, os: &str) -> Option<&'static str> {
    find_expected_checksum_internal(backend_name, version, os, env::consts::ARCH)
}

pub fn install_driver_internal(backend_name: &str, version: &str, target_os: &str) -> Result<()> {
    let full_driver_path = format_driver_path(backend_name, version, target_os)?;
    let url = format_driver_url(backend_name, version, target_os);
    let checksum = find_expected_checksum(backend_name, version, target_os);
    download_zst_driver_file(&url, &full_driver_path, checksum)
}

/// Unguessable temporary file name generator.
fn tmpname(
    prefix: impl AsRef<str>,
    rand_len: usize,
    suffix: impl AsRef<str>,
) -> core::result::Result<OsString, getrandom::Error> {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    // get random data directly from the OS entropy source
    let mut seeds = vec![0; rand_len];
    getrandom::getrandom(&mut seeds)?;

    let mut name_bytes = Vec::with_capacity(rand_len);
    for b in seeds {
        name_bytes.push(CHARS[(b as usize) % CHARS.len()]);
    }
    // safe to .unwrap() as we know the bytes are valid UTF-8
    let name = std::str::from_utf8(&name_bytes).unwrap();
    let full_name = format!("{}{}{}", prefix.as_ref(), name, suffix.as_ref());
    Ok(OsString::from(full_name))
}

/// [zstd_safe::WriteBuf] implementation backed by a [Vec<u8>] buffer.
struct ZstdWriteBuffer {
    inner: Vec<u8>,
}

impl ZstdWriteBuffer {
    fn with_fixed_capacity(capacity: usize) -> Self {
        let buffer = Vec::with_capacity(capacity);
        Self { inner: buffer }
    }
}

unsafe impl zstd_safe::WriteBuf for ZstdWriteBuffer {
    fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.inner.as_mut_ptr()
    }

    unsafe fn filled_until(&mut self, n: usize) {
        // SAFETY: n <= self.capacity() (invariant)
        unsafe {
            self.inner.set_len(n);
        }
    }
}

const DRIVER_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);

/// Download a Zstandard-compressed file from the given URL and save (atomically and durably)
/// it to the fully-qualified destination path.
pub fn download_zst_driver_file<P: AsRef<Path>>(
    url: &str,
    destination: P,
    expected_sha256sum: Option<&str>,
) -> Result<()> {
    debug_assert!(
        destination.as_ref().is_absolute(),
        "destination path must be absolute"
    );

    // Configure the HTTP agent
    let http_agent = {
        // Use Rustls as the TLS provider but on the OS for the root certificates.
        //
        // [1]: https://github.com/dbt-labs/dbt-fusion/issues/147
        let tls_config = TlsConfig::builder()
            .provider(TlsProvider::Rustls)
            .root_certs(RootCerts::PlatformVerifier)
            .build();
        let http_config = ureq::Agent::config_builder()
            .tls_config(tls_config)
            .timeout_global(Some(DRIVER_DOWNLOAD_TIMEOUT))
            .build();
        ureq::Agent::new_with_config(http_config)
    };

    let mut response = http_agent.get(url).call().map_err(InstallError::Http)?;

    // Generate a random file name and create an empty temporary file
    let tmp_path = {
        let tmp_name = tmpname(".", 15, ".download").map_err(InstallError::GetRandom)?;
        // ensure the destination exists and create it if necessary
        let parent = destination.as_ref().parent().ok_or_else(|| {
            let message = format!(
                "destination path must be an absolute path: {}",
                destination.as_ref().display()
            );
            let error = io::Error::new(io::ErrorKind::InvalidInput, message);
            InstallError::Io(error)
        })?;
        std::fs::create_dir_all(parent).map_err(|e| InstallError::CreateDir(e, parent.into()))?;
        // write the file in the same directory as the destination to ensure
        // the rename operation is atomic (happens on the same filesystem)
        parent.join(tmp_name)
    };
    let mut tmp = std::fs::File::create(&tmp_path)
        .map_err(|e| InstallError::CreateFIle(e, tmp_path.clone()))?;

    // Download and Zstandard decompression buffers
    const DECOMPRESSION_FACTOR: usize = 4;
    let mut zstd_insize_hint = zstd_safe::DCtx::in_size(); // ~200KB
    let mut download_buffer = Vec::with_capacity(zstd_insize_hint);
    let write_buffer_capacity = (DECOMPRESSION_FACTOR * download_buffer.capacity()).max(64 * 1024);
    let mut inner_write_buffer = ZstdWriteBuffer::with_fixed_capacity(write_buffer_capacity);
    let mut write_buffer = zstd_safe::OutBuffer::around(&mut inner_write_buffer);

    // Zstandard decompression context and SHA-256 hasher
    let mut dctx = zstd_safe::DCtx::create();
    let mut hasher = Sha256::new();

    // Try to decompress all the data in the download_buffer, if not possible, the remaining data
    // will be moved to the beginning of the Vec which will be resized to the remaining size.
    let mut decompress_step = |download_buffer: &mut Vec<u8>,
                               write_buffer: &mut zstd_safe::OutBuffer<'_, ZstdWriteBuffer>|
     -> core::result::Result<usize, InstallError> {
        debug_assert!(!download_buffer.is_empty());
        // To simplify things, we keep the compressed data always at the beginning of the
        // download_buffer, so an InBuffer around it can be created every time we use it.
        let mut in_buffer = zstd_safe::InBuffer::around(download_buffer.as_slice());

        // let prev_write_buffer_pos = write_buffer.pos();
        let next_insize_hint = dctx
            .decompress_stream(write_buffer, &mut in_buffer)
            .map_err(InstallError::ZstdDecompress)?;
        if in_buffer.pos() == 0 {
            return Ok(0);
        }
        let remaining = in_buffer.pos()..download_buffer.len();
        // println!(
        //     "Decompressed {} bytes to {} bytes ({} bytes remaining in the download_buffer)",
        //     in_buffer.pos,
        //     write_buffer.pos() - prev_write_buffer_pos,
        //     remaining.len(),
        // );

        // Update the hasher with the downloaded data that was decompressed.
        hasher.update(&download_buffer[0..in_buffer.pos()]);

        // Move remaining data to the beginning of the download buffer.
        // (only happens when there isn't enough space in the write buffer)
        let remaining_len = remaining.len();
        download_buffer.copy_within(remaining, 0);
        download_buffer.truncate(remaining_len);
        Ok(next_insize_hint)
    };

    let mut download_stream = response.body_mut().as_reader();
    loop {
        if download_buffer.len() < zstd_insize_hint {
            // Ensure there is `zstd_insize_hint` bytes of capacity in `download_buffer`.
            download_buffer.reserve(zstd_insize_hint - download_buffer.len());
            // Try to read what is needed to fill `download_buffer` with at most
            // `zstd_insize_hint` bytes.
            {
                let initial_pos = download_buffer.len();
                // SAFETY: capacity guaranteed above, truncated after the read.
                unsafe { download_buffer.set_len(zstd_insize_hint) };
                debug_assert!(download_buffer.len() > initial_pos);
                let chunk_n = download_stream.read(&mut download_buffer[initial_pos..]);
                match chunk_n {
                    Ok(n) => {
                        // set download_buffer length to include only valid data
                        download_buffer.truncate(initial_pos + n);
                        if n == 0 {
                            break;
                        }
                    }
                    Err(e) => {
                        download_buffer.truncate(initial_pos);
                        return Err(InstallError::Io(e));
                    }
                }
            }
        }
        // Keep asking for more data until we have a good amount of data to decompress.
        if download_buffer.len() < zstd_insize_hint {
            continue;
        }
        // Decompress when we have the right amount of data in the download buffer.
        debug_assert!(!download_buffer.is_empty());
        let hint = decompress_step(&mut download_buffer, &mut write_buffer)?;
        if hint > 0 {
            zstd_insize_hint = hint;
        }
        // If the write buffer is full, drain it to the temporary file.
        if write_buffer.pos() == write_buffer.capacity() {
            tmp.write_all(write_buffer.as_slice())
                .map_err(InstallError::WriteFile)?;
            unsafe {
                write_buffer.set_pos(0);
            }
        }
    }
    // Final download_buffer drain
    while !download_buffer.is_empty() {
        let _ = decompress_step(&mut download_buffer, &mut write_buffer)?;
        tmp.write_all(write_buffer.as_slice())
            .map_err(InstallError::WriteFile)?;
        unsafe {
            write_buffer.set_pos(0);
        }
    }
    // Final write_buffer drain.
    if write_buffer.pos() > 0 {
        tmp.write_all(write_buffer.as_slice())
            .map_err(InstallError::WriteFile)?;
    }

    // Finalize the hash computation and compare it with the expected value.
    let sha256sum = hasher.finalize();
    if let Some(expected) = expected_sha256sum {
        debug_assert!(
            expected.len() == 64,
            "expected SHA-256 checksum must be 64 hex characters"
        );
        const HEX: &[u8] = b"0123456789abcdef";
        let mut got = [0; 64];
        for (i, b) in sha256sum.iter().enumerate() {
            got[i * 2] = HEX[(b >> 4) as usize];
            got[i * 2 + 1] = HEX[(b & 0xf) as usize];
        }
        let got = std::str::from_utf8(&got).unwrap();
        if got != expected {
            return Err(InstallError::ChecksumMismatch(
                expected.to_string(),
                got.to_string(),
                url.to_string(),
            ));
        }
    }

    // fsync() the temp file and atomically rename it to the destination.
    tmp.sync_data().map_err(InstallError::SyncFile)?;
    std::fs::rename(tmp_path, destination.as_ref()).map_err(InstallError::RenameFile)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_format_driver_url() {
        let url = format_driver_url("snowflake", "0.17.0+dbt0.2.0", "manylinux_2_17-linux-gnu");
        assert_eq!(
            url,
            format!(
                "https://public.cdn.getdbt.com/fs/adbc/snowflake/adbc_driver_snowflake-0.17.0%2Bdbt0.2.0-{}-manylinux_2_17-linux-gnu{}.zst",
                env::consts::ARCH,
                env::consts::DLL_SUFFIX
            )
        );
    }

    #[test]
    fn test_format_driver_path() {
        let path =
            format_driver_path("snowflake", "0.17.0+dbt0.2.0", "manylinux_2_17-linux-gnu").unwrap();

        #[cfg(target_os = "windows")]
        let dbt_cache_dir = format!("{}\\com.getdbt", dirs::cache_dir().unwrap().display());
        #[cfg(not(target_os = "windows"))]
        let dbt_cache_dir = format!("{}/com.getdbt", dirs::cache_dir().unwrap().display());

        #[cfg(target_os = "windows")]
        let filename = "adbc_driver_snowflake-0.17.0+dbt0.2.0.dll";
        #[cfg(target_os = "linux")]
        let filename = "libadbc_driver_snowflake-0.17.0+dbt0.2.0.so";
        #[cfg(target_os = "macos")]
        let filename = "libadbc_driver_snowflake-0.17.0+dbt0.2.0.dylib";

        let expected = PathBuf::from(format!(
            "{}/adbc/{}-manylinux_2_17-linux-gnu/{}",
            dbt_cache_dir,
            env::consts::ARCH,
            filename,
        ));
        assert_eq!(path, expected);
    }

    /// Check that the expected SHA-256 checksum is found for each backend, version, target_os and
    /// arch combinations.
    ///
    /// IMPORTANT: If this is failing, it probably means you need to run
    /// `./scripts/gen_cdn_driver_checksums.sh` and try again.
    ///
    /// This test also guarantees that the driver exists on the CDN because the
    /// `gen_cdn_driver_checksums.sh` script can only generate checksums for the drivers it can
    /// download.
    #[test]
    fn test_all_checksums_are_listed() {
        let backend_and_versions = [
            ("snowflake", SNOWFLAKE_DRIVER_VERSION),
            ("bigquery", BIGQUERY_DRIVER_VERSION),
            ("postgresql", POSTGRES_DRIVER_VERSION),
            ("databricks", DATABRICKS_DRIVER_VERSION),
            ("salesforce", SALESFORCE_DRIVER_VERSION),
            ("redshift", REDSHIFT_DRIVER_VERSION),
        ];
        debug_assert!(
            backend_and_versions.len() == INSTALLABLE_DRIVERS.len(),
            "backend_and_versions must have the same length as INSTALLABLE_DRIVERS"
        );
        let target_os_and_archs = [
            (LINUX_TARGET_OS, vec!["x86_64", "aarch64"]),
            (MACOS_TARGET_OS, vec!["x86_64", "aarch64"]),
            (WINDOWS_TARGET_OS, vec!["x86_64"]),
        ];
        for (backend, version) in backend_and_versions.iter() {
            for (target_os, archs) in target_os_and_archs.iter() {
                for arch in archs {
                    let checksum =
                        find_expected_checksum_internal(backend, version, target_os, arch);
                    assert!(
                        checksum.is_some(),
                        "Missing checksum for backend: {backend}, version: {version}, target_os: {target_os}"
                    );
                }
            }
        }
    }
}
