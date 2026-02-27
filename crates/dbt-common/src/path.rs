use std::{
    ffi::OsStr,
    hash::Hash,
    path::{Component, Display, Path, PathBuf},
};

use normalize_path::NormalizePath;

/// Self-normalizing path. Wrapper around [PathBuf].
/// Case-sensitivity for equality ([PartialEq] and [Eq]) is determined by the OS.
/// Comparisons ([Ord] and [PartialOrd]) are always case-sensitive.
/// Use this instead of [PathBuf].
#[derive(Clone, Debug, Default, Ord, PartialOrd)]
pub struct DbtPath(PathBuf);

#[allow(unused)]
impl DbtPath {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        Self(path.as_ref().normalize())
    }

    /// See [PathBuf::as_path] for documentation.
    pub fn as_path(&self) -> &Path {
        self.0.as_path()
    }

    pub fn to_path_buf(&self) -> PathBuf {
        self.0.clone()
    }

    /// See [Path::file_name] for documentation.
    pub fn file_name(&self) -> Option<&OsStr> {
        self.0.file_name()
    }

    /// See [Path::is_relative] for documentation.
    pub fn is_relative(&self) -> bool {
        self.0.is_relative()
    }

    /// See [Path::is_absolute] for documentation.
    pub fn is_absolute(&self) -> bool {
        self.0.is_absolute()
    }

    /// See [Path::has_root] for documentation.
    pub fn has_root(&self) -> bool {
        self.0.has_root()
    }

    /// See [Path::extension] for documentation.
    pub fn extension(&self) -> Option<&OsStr> {
        self.0.extension()
    }

    /// See [Path::join] for documentation.
    pub fn join(&self, path: &DbtPath) -> Self {
        Self(self.0.join(path.as_path()).normalize())
    }

    /// Case-sensitivity based on the OS.
    pub fn get_relative_path(&self, base_path: &DbtPath) -> Option<Self> {
        Some(Self(
            diff_paths_os_ascii_case(&self.0, &base_path.0)?.normalize(),
        ))
    }

    /// See [Path::to_str] for documentation.
    pub fn to_str(&self) -> Option<&str> {
        self.0.to_str()
    }

    /// See [Path::display] for documentation.
    pub fn display(&self) -> Display<'_> {
        self.0.display()
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
    /// Case-sensitivity based on the OS.
    fn eq(&self, other: &Self) -> bool {
        let str1 = self.0.to_string_lossy();
        let str2 = other.0.to_string_lossy();
        if cfg!(target_os = "linux") {
            str1.eq(&str2)
        } else {
            str1.eq_ignore_ascii_case(&str2)
        }
    }
}
impl Eq for DbtPath {}

impl Hash for DbtPath {
    /// Case-sensitivity based on the OS.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let str = self.0.to_string_lossy();
        if cfg!(target_os = "linux") {
            str.hash(state);
        } else {
            for c in str.chars() {
                c.to_ascii_lowercase().hash(state);
            }
            state.write_u8(0xff);
        }
    }
}

/// Equality is determined by the OS.
fn component_eq_os_ascii_case(component1: &Component<'_>, component2: &Component<'_>) -> bool {
    match (component1, component2) {
        (Component::CurDir, Component::CurDir)
        | (Component::ParentDir, Component::ParentDir)
        | (Component::RootDir, Component::RootDir) => true,
        (Component::Normal(str1), Component::Normal(str2)) => {
            if cfg!(target_os = "linux") {
                str1.eq(str2)
            } else {
                str1.eq_ignore_ascii_case(str2)
            }
        }
        (Component::Prefix(prefix1), Component::Prefix(prefix2)) => {
            if cfg!(target_os = "linux") {
                prefix1.as_os_str().eq(prefix2.as_os_str())
            } else {
                prefix1
                    .as_os_str()
                    .eq_ignore_ascii_case(prefix2.as_os_str())
            }
        }
        _ => false,
    }
}

/// Construct a relative path from a provided base directory path to the provided path.
/// Modified version of https://docs.rs/pathdiff/0.2.3/src/pathdiff/lib.rs.html#43 to support case sensitivity based on the OS.
fn diff_paths_os_ascii_case<P, B>(path: P, base: B) -> Option<PathBuf>
where
    P: AsRef<Path>,
    B: AsRef<Path>,
{
    let path = path.as_ref();
    let base = base.as_ref();

    if path.is_absolute() != base.is_absolute() {
        if path.is_absolute() {
            Some(PathBuf::from(path))
        } else {
            None
        }
    } else {
        let mut ita = path.components();
        let mut itb = base.components();
        let mut comps: Vec<Component> = vec![];
        loop {
            match (ita.next(), itb.next()) {
                (None, None) => break,
                (Some(a), None) => {
                    comps.push(a);
                    comps.extend(ita.by_ref());
                    break;
                }
                (None, _) => comps.push(Component::ParentDir),
                (Some(a), Some(b)) if comps.is_empty() && component_eq_os_ascii_case(&a, &b) => (),
                (Some(a), Some(Component::CurDir)) => comps.push(a),
                (Some(_), Some(Component::ParentDir)) => return None,
                (Some(a), Some(_)) => {
                    comps.push(Component::ParentDir);
                    for _ in itb {
                        comps.push(Component::ParentDir);
                    }
                    comps.push(a);
                    comps.extend(ita.by_ref());
                    break;
                }
            }
        }
        Some(comps.iter().map(|c| c.as_os_str()).collect())
    }
}

/// Based off of https://docs.rs/pathdiff/0.2.3/src/pathdiff/lib.rs.html#171
#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hasher};

    use super::*;

    #[test]
    fn test_absolute() {
        fn abs(path: &str) -> String {
            // Absolute paths look different on Windows vs Unix.
            if cfg!(target_os = "windows") {
                format!("C:\\{}", path)
            } else {
                format!("/{}", path)
            }
        }

        assert_diff_paths(&abs("foo"), &abs("bar"), Some("../foo"));
        assert_diff_paths(&abs("foo"), "bar", Some(&abs("foo")));
        assert_diff_paths("foo", &abs("bar"), None);
        assert_diff_paths("foo", "bar", Some("../foo"));
    }

    #[test]
    fn test_identity() {
        assert_diff_paths(".", ".", Some(""));
        assert_diff_paths("../foo", "../foo", Some(""));
        assert_diff_paths("./foo", "./foo", Some(""));
        assert_diff_paths("/foo", "/foo", Some(""));
        assert_diff_paths("foo", "foo", Some(""));

        assert_diff_paths("../foo/bar/baz", "../foo/bar/baz", Some(""));
        assert_diff_paths("foo/bar/baz", "foo/bar/baz", Some(""));
    }

    #[test]
    fn test_subset() {
        assert_diff_paths("foo", "fo", Some("../foo"));
        assert_diff_paths("fo", "foo", Some("../fo"));
    }

    #[test]
    fn test_empty() {
        assert_diff_paths("", "", Some(""));
        assert_diff_paths("foo", "", Some("foo"));
        assert_diff_paths("", "foo", Some(".."));
    }

    #[test]
    fn test_relative() {
        assert_diff_paths("../foo", "../bar", Some("../foo"));
        assert_diff_paths("../foo", "../foo/bar/baz", Some("../.."));
        assert_diff_paths("../foo/bar/baz", "../foo", Some("bar/baz"));

        assert_diff_paths("foo/bar/baz", "foo", Some("bar/baz"));
        assert_diff_paths("foo/bar/baz", "foo/bar", Some("baz"));
        assert_diff_paths("foo/bar/baz", "foo/bar/baz", Some(""));
        assert_diff_paths("foo/bar/baz", "foo/bar/baz/", Some(""));

        assert_diff_paths("foo/bar/baz/", "foo", Some("bar/baz"));
        assert_diff_paths("foo/bar/baz/", "foo/bar", Some("baz"));
        assert_diff_paths("foo/bar/baz/", "foo/bar/baz", Some(""));
        assert_diff_paths("foo/bar/baz/", "foo/bar/baz/", Some(""));

        assert_diff_paths("foo/bar/baz", "foo/", Some("bar/baz"));
        assert_diff_paths("foo/bar/baz", "foo/bar/", Some("baz"));
        assert_diff_paths("foo/bar/baz", "foo/bar/baz", Some(""));
    }

    #[test]
    fn test_current_directory() {
        assert_diff_paths(".", "foo", Some("../."));
        assert_diff_paths("foo", ".", Some("foo"));
        assert_diff_paths("/foo", "/.", Some("foo"));
    }

    #[test]
    fn test_relative_case_sensitivity() {
        if cfg!(target_os = "linux") {
            assert_diff_paths("FOO/bar/baz", "foo/bar", Some("../../FOO/bar/baz"));
            assert_diff_paths("FOO/bar/BAZ", "foo/bar", Some("../../FOO/bar/BAZ"));
        } else {
            assert_diff_paths("FOO/bar/baz", "foo/bar", Some("baz"));
            assert_diff_paths("FOO/bar/BAZ", "foo/bar", Some("BAZ"));
        }
    }

    #[test]
    fn test_case_sensitivity_eq_for_dbt_path() {
        if cfg!(target_os = "linux") {
            assert!(DbtPath::from("chicken") == DbtPath::from("chicken"));
            assert!(DbtPath::from("CHICKEN") != DbtPath::from("chicken"));
        } else {
            assert!(DbtPath::from("chicken") == DbtPath::from("chicken"));
            assert!(DbtPath::from("CHICKEN") == DbtPath::from("chicken"));
        }
    }

    #[test]
    fn test_case_sensitivity_hash_for_dbt_path() {
        let lowercase_path = DbtPath::from("chicken");
        let uppercase_path = DbtPath::from("CHICKEN");

        let mut lowercase_hasher = DefaultHasher::new();
        let mut uppercase_hasher = DefaultHasher::new();

        lowercase_path.hash(&mut lowercase_hasher);
        uppercase_path.hash(&mut uppercase_hasher);

        if cfg!(target_os = "linux") {
            assert_ne!(lowercase_hasher.finish(), uppercase_hasher.finish());
        } else {
            assert_eq!(lowercase_hasher.finish(), uppercase_hasher.finish());
        }

        assert_ne!(lowercase_hasher.finish(), 0);
        assert_ne!(uppercase_hasher.finish(), 0);
    }

    #[test]
    fn test_case_sensitivity_with_non_utf8_characters() {
        assert!(DbtPath::from("chicken/ñ") != DbtPath::from("chicken/À"));

        if cfg!(target_os = "linux") {
            assert!(DbtPath::from("chicken/À") == DbtPath::from("chicken/À"));
            assert!(DbtPath::from("CHICKEN/À") != DbtPath::from("chicken/À"));
        } else {
            assert!(DbtPath::from("chicken/À") == DbtPath::from("chicken/À"));
            assert!(DbtPath::from("CHICKEN/À") == DbtPath::from("chicken/À"));
        }
    }

    fn assert_diff_paths(path: &str, base: &str, expected: Option<&str>) {
        assert_eq!(
            diff_paths_os_ascii_case(path, base),
            expected.map(|s| s.into())
        );
    }
}
