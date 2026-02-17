use std::{
    hash::Hash,
    path::{Path, PathBuf},
};

use normalize_path::NormalizePath;

use crate::stdfs::diff_paths;

/// Self-normalizing path.
/// Case-sensitivity for equality and comparisons are dependent on the OS.
#[derive(Clone, Debug, Default)]
pub struct DbtPath(PathBuf);

#[allow(unused)]
impl DbtPath {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        Self(path.as_ref().normalize())
    }

    pub fn is_relative(&self) -> bool {
        self.0.is_relative()
    }

    pub fn is_absolute(&self) -> bool {
        self.0.is_absolute()
    }

    pub fn join(&self, path: &DbtPath) -> Self {
        Self(self.0.join(path).normalize())
    }

    pub fn get_relative_path(&self, from_absolute_path: &DbtPath) -> Option<Self> {
        Some(Self(
            diff_paths(&self.0, &from_absolute_path.0).ok()?.normalize(),
        ))
    }

    pub fn to_str(&self) -> Option<&str> {
        self.0.to_str()
    }

    pub fn eq_path<P: AsRef<Path>>(&self, path: P) -> bool {
        match (self.0.to_str(), path.as_ref().to_str()) {
            (Some(s1), Some(s2)) => {
                if cfg!(target_os = "linux") {
                    s1.eq(s2)
                } else {
                    s1.eq_ignore_ascii_case(s2)
                }
            }
            _ => false, // Paths contain non-UTF-8 characters
        }
    }
}

impl AsRef<Path> for DbtPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl From<String> for DbtPath {
    fn from(value: String) -> Self {
        Self(PathBuf::from(value).normalize())
    }
}

impl From<&String> for DbtPath {
    fn from(value: &String) -> Self {
        Self(PathBuf::from(value).normalize())
    }
}

impl From<&str> for DbtPath {
    fn from(value: &str) -> Self {
        Self(PathBuf::from(value).normalize())
    }
}

impl PartialEq for DbtPath {
    fn eq(&self, other: &Self) -> bool {
        self.eq_path(other)
    }
}
impl Eq for DbtPath {}

impl Hash for DbtPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Some(s) = self.0.to_str() {
            if cfg!(target_os = "linux") {
                s.hash(state);
            } else {
                // This is not as efficient, but it is the quickest way to
                // achieve a hash based on case-insensitive paths.
                s.to_ascii_lowercase().hash(state);
            }
        }
    }
}
