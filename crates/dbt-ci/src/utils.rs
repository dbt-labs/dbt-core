use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use toml_edit::DocumentMut;

/// Nearest ancestor of `CARGO_MANIFEST_DIR` whose `Cargo.toml` has a
/// `[workspace]` table. Falls back to cwd (then `.`) if no ancestor matches.
pub(crate) fn cargo_workspace_root() -> PathBuf {
    let cwd_fallback = || env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let Some(start) = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .ok()
        .or_else(|| env::current_dir().ok())
    else {
        return cwd_fallback();
    };
    let mut cur: &Path = start.as_path();
    loop {
        if has_workspace_table(cur) {
            return cur.to_path_buf();
        }
        match cur.parent() {
            Some(p) => cur = p,
            None => return cwd_fallback(),
        }
    }
}

fn has_workspace_table(dir: &Path) -> bool {
    let path = dir.join("Cargo.toml");
    let Ok(text) = fs::read_to_string(&path) else {
        return false;
    };
    text.parse::<DocumentMut>()
        .map(|doc| doc.get("workspace").is_some())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_table_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("Cargo.toml"), "[workspace]\nmembers = []\n").unwrap();
        assert!(has_workspace_table(dir.path()));
    }

    #[test]
    fn package_only_manifest_is_not_a_workspace() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        assert!(!has_workspace_table(dir.path()));
    }

    #[test]
    fn commented_workspace_line_is_not_detected() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            "# [workspace] disabled while we split crates\n[package]\nname = \"x\"\n",
        )
        .unwrap();
        assert!(!has_workspace_table(dir.path()));
    }

    #[test]
    fn missing_manifest_is_not_a_workspace() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!has_workspace_table(dir.path()));
    }
}
