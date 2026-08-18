use std::{
    io::Write,
    ops,
    path::{Path, PathBuf},
    range::Range,
};

use dbt_common::{
    ErrorCode, FsResult, err,
    error::WrappedError,
    fs_err,
    pretty_string::{DIM, GREEN, RED},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PythonPackageManager {
    Pip,
    Pipx,
    Uv,
    Poetry,
    Pdm,
    Pipenv,
    Hatch,
    Conda,
    Asdf,
    Mise,
    Pyenv,
    Rye,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackageVersion {
    /// Pin to this exact version.
    Exact(String),
    /// Allow any version considered "compatible" with this one, in
    /// whatever sense the target format's ecosystem uses (a caret range
    /// for Poetry, a PEP 440 compatible-release clause elsewhere, an
    /// explicit `>=x,<y` range for conda).
    Compatible(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageSpec {
    pub name: String,
    pub version: PackageVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PythonManifestFormat {
    Pyproject,
    Pipfile,
    CondaEnvironment,
    SetupCfg,
    Requirements,
}

impl PythonManifestFormat {
    /// Renders `v` as the version-specifier text this format's ecosystem
    /// expects, e.g. `^1.2.3` for Poetry, `~=1.2.3` for a PEP 440 consumer,
    /// `=1.2.3` for conda. The result is a bare specifier with no
    /// surrounding quotes — callers splice it directly into whatever
    /// delimiters already exist in the source file.
    ///
    /// `package_manager` is required to disambiguate `Pyproject`, which can
    /// declare the same dependency under more than one tool's table with
    /// different specifier syntax (a bare `[project.dependencies]` PEP 508
    /// string vs. a `[tool.poetry.dependencies]` caret range).
    pub fn render_version(
        self,
        v: PackageVersion,
        package_manager: Option<PythonPackageManager>,
    ) -> String {
        if self == PythonManifestFormat::Pyproject
            && package_manager == Some(PythonPackageManager::Poetry)
        {
            return render_poetry(&v);
        }
        // A conda `dependencies:` entry uses conda's match-spec syntax, but a
        // nested `- pip:` sub-list under it is a plain pip requirements list.
        if self == PythonManifestFormat::CondaEnvironment
            && package_manager != Some(PythonPackageManager::Pip)
        {
            return render_conda(&v);
        }
        render_pep440(&v)
    }

    /// Finds every place in `text` — the raw source of a manifest in this
    /// format — that declares `package_name`, tagging each match with
    /// whichever package manager's syntax governs it there (consumed by
    /// [`Self::render_version`] to pick the right rendering per match).
    fn find_matches(self, text: &str, package_name: &str) -> FsResult<Vec<Match>> {
        match self {
            Self::Pyproject => find_pyproject_matches(text, package_name),
            Self::Pipfile => find_pipfile_matches(text, package_name),
            Self::CondaEnvironment => find_conda_matches(text, package_name),
            Self::SetupCfg => Ok(find_setup_cfg_matches(text, package_name)),
            Self::Requirements => find_requirements_matches(text, package_name),
        }
    }
}

fn find_pyproject_matches(text: &str, package_name: &str) -> FsResult<Vec<Match>> {
    let doc: toml_edit::Document<String> = text.parse().map_err(|e| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "failed to parse pyproject.toml: {e}"
        )
    })?;
    let mut out = Vec::new();

    // PEP 621 `[project.dependencies]`: an array of PEP 508 requirement
    // strings, e.g. `["dbt-core>=1.5,<2"]`.
    if let Some(deps) = doc
        .get("project")
        .and_then(|p| p.get("dependencies"))
        .and_then(|d| d.as_array())
    {
        out.extend(find_matches_in_requirement_array(
            text,
            deps,
            package_name,
            Some(PythonPackageManager::Pip),
        ));
    }

    if let Some(poetry) = doc.get("tool").and_then(|t| t.get("poetry")) {
        // `[tool.poetry.dependencies]`: a table of `name = "spec"` or
        // `name = { version = "spec", ... }`.
        if let Some(deps) = poetry.get("dependencies").and_then(|d| d.as_table_like()) {
            out.extend(find_matches_in_poetry_table(text, deps, package_name));
        }

        // Legacy (pre-1.2) `[tool.poetry.dev-dependencies]`: same shape as
        // `[tool.poetry.dependencies]`, for the dev-only dependency set.
        if let Some(deps) = poetry
            .get("dev-dependencies")
            .and_then(|d| d.as_table_like())
        {
            out.extend(find_matches_in_poetry_table(text, deps, package_name));
        }

        // `[tool.poetry.group.<name>.dependencies]`: the dependency-group
        // mechanism that superseded `dev-dependencies` in Poetry 1.2,
        // e.g. `[tool.poetry.group.test.dependencies]`.
        if let Some(groups) = poetry.get("group").and_then(|g| g.as_table_like()) {
            for (_, group) in groups.iter() {
                if let Some(deps) = group.get("dependencies").and_then(|d| d.as_table_like()) {
                    out.extend(find_matches_in_poetry_table(text, deps, package_name));
                }
            }
        }
    }

    // Legacy PDM `[tool.pdm.dev-dependencies]`: a table of group name ->
    // array of PEP 508 requirement strings, e.g.
    // `test = ["dbt-core>=1.5,<2"]`.
    if let Some(groups) = doc
        .get("tool")
        .and_then(|t| t.get("pdm"))
        .and_then(|p| p.get("dev-dependencies"))
        .and_then(|d| d.as_table_like())
    {
        for (_, item) in groups.iter() {
            if let Some(array) = item.as_array() {
                out.extend(find_matches_in_requirement_array(
                    text,
                    array,
                    package_name,
                    Some(PythonPackageManager::Pdm),
                ));
            }
        }
    }

    // Legacy uv `[tool.uv.dev-dependencies]`: an array of PEP 508
    // requirement strings, predating uv's adoption of the standardized
    // `[dependency-groups]` (PEP 735) table.
    if let Some(deps) = doc
        .get("tool")
        .and_then(|t| t.get("uv"))
        .and_then(|u| u.get("dev-dependencies"))
        .and_then(|d| d.as_array())
    {
        out.extend(find_matches_in_requirement_array(
            text,
            deps,
            package_name,
            Some(PythonPackageManager::Uv),
        ));
    }

    Ok(out)
}

fn find_pipfile_matches(text: &str, package_name: &str) -> FsResult<Vec<Match>> {
    let doc: toml_edit::Document<String> = text
        .parse()
        .map_err(|e| fs_err!(ErrorCode::InvalidConfig, "failed to parse Pipfile: {e}"))?;
    let mut out = Vec::new();

    for table_name in ["packages", "dev-packages"] {
        let Some(table) = doc.get(table_name).and_then(|t| t.as_table_like()) else {
            continue;
        };
        for (key, item) in table.iter() {
            if normalize_package_name(key) != normalize_package_name(package_name) {
                continue;
            }
            let version_item = item
                .as_table_like()
                .and_then(|t| t.get("version"))
                .unwrap_or(item);
            let Some(span) = version_item.as_value().and_then(|v| v.span()) else {
                continue;
            };
            if let Some(interior) = toml_string_interior_range(text, span) {
                out.push(Match {
                    range: interior,
                    package_manager: Some(PythonPackageManager::Pipenv),
                });
            }
        }
    }

    Ok(out)
}

/// Scans a conda `environment.yml`'s top-level `dependencies:` sequence,
/// plus its nested `pip:` sub-sequence, for `package_name`. Both sequences
/// may use YAML block (`- item`) or flow (`[item, ...]`) syntax, and each
/// item's package spec may be a plain or quoted scalar; parsing the
/// document structurally (rather than scanning lines) handles all of these
/// uniformly, since YAML normalizes block/flow syntax into the same tree.
fn find_conda_matches(text: &str, package_name: &str) -> FsResult<Vec<Match>> {
    let root: dbt_yaml::Value = dbt_yaml::from_str(text).map_err(|e| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "failed to parse environment.yml: {e}"
        )
    })?;

    let Some(deps) = root
        .as_mapping()
        .and_then(|m| m.get("dependencies"))
        .and_then(|d| d.as_sequence())
    else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();
    for item in deps {
        if let Some(range) = find_conda_scalar_match(text, item, package_name) {
            out.push(Match {
                range,
                package_manager: Some(PythonPackageManager::Conda),
            });
            continue;
        }
        // A nested `pip:` list, e.g. `- pip:\n    - dbt-core==1.2.3`.
        let Some(pip_items) = item
            .as_mapping()
            .and_then(|m| m.get("pip"))
            .and_then(|p| p.as_sequence())
        else {
            continue;
        };
        for pip_item in pip_items {
            if let Some(range) = find_conda_scalar_match(text, pip_item, package_name) {
                out.push(Match {
                    range,
                    package_manager: Some(PythonPackageManager::Pip),
                });
            }
        }
    }
    Ok(out)
}

/// Locates `package_name`'s version specifier within a single conda
/// dependency entry (a YAML string scalar such as `dbt-core=1.2.3` or the
/// quoted `"dbt-core=1.2.3"`), returning its absolute byte range in `text`.
/// Returns `None` if `item` isn't a string scalar, or doesn't mention
/// `package_name`.
fn find_conda_scalar_match(
    text: &str,
    item: &dbt_yaml::Value,
    package_name: &str,
) -> Option<ops::Range<usize>> {
    let decoded = item.as_str()?;
    let interior = yaml_scalar_interior_range(text, item.span().start.index, decoded)?;
    let rel = find_specifier_span(&text[interior.clone()], package_name)?;
    Some(interior.start + rel.start..interior.start + rel.end)
}

fn find_setup_cfg_matches(text: &str, package_name: &str) -> Vec<Match> {
    let mut out = Vec::new();
    let mut in_options = false;
    let mut in_install_requires = false;
    let mut key_indent = 0usize;

    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim_start();

        if trimmed.starts_with('[') {
            in_options = trimmed.trim_end() == "[options]";
            in_install_requires = false;
            continue;
        }
        if !in_options {
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("install_requires") {
            let rest = rest.trim_start();
            if let Some(value) = rest.strip_prefix('=') {
                in_install_requires = true;
                key_indent = indent;
                let value = value.trim_start();
                if !value.is_empty() {
                    if let Some(rel) = find_specifier_span(value, package_name) {
                        let base = offset_of(text, value);
                        out.push(Match {
                            range: base + rel.start..base + rel.end,
                            package_manager: Some(PythonPackageManager::Pip),
                        });
                    }
                }
                continue;
            }
        }

        if in_install_requires {
            if trimmed.is_empty() {
                continue;
            }
            if indent <= key_indent {
                in_install_requires = false;
                continue;
            }
            if let Some(rel) = find_specifier_span(trimmed, package_name) {
                let base = offset_of(text, trimmed);
                out.push(Match {
                    range: base + rel.start..base + rel.end,
                    package_manager: Some(PythonPackageManager::Pip),
                });
            }
        }
    }
    out
}

/// The part of a requirements-file line before any `#` comment — a trailing
/// `\` or a `--hash=` inside a comment is prose, not syntax.
fn requirement_line_code(line: &str) -> &str {
    line.split('#').next().unwrap_or(line)
}

/// Returns the leading package-name token of a requirement entry, or `None`
/// if the entry doesn't start with one.
fn requirement_name(text: &str) -> Option<&str> {
    let rest = text.trim_start();
    let len = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
        .unwrap_or(rest.len());
    (len > 0).then(|| &rest[..len])
}

/// Finds every requirement declaring `package_name` in a pip requirements
/// file.
///
/// pip joins a physical line ending in `\` with the one that follows into a
/// single logical requirement, and hash pins (`--hash=sha256:...`, as emitted
/// by `pip-compile`) hang off the requirement they pin that way. Replacing a
/// version specifier inside such a block would swallow the continuation
/// backslash and orphan its hashes — and a hash can't be recomputed from the
/// new pin here anyway — so a match inside one is rejected rather than
/// mis-spliced.
fn find_requirements_matches(text: &str, package_name: &str) -> FsResult<Vec<Match>> {
    let mut out = Vec::new();
    let mut lines = text.lines().peekable();
    while let Some(first) = lines.next() {
        let code = requirement_line_code(first);
        let mut continues = code.trim_end().ends_with('\\');
        let mut is_block = continues || code.contains("--hash=");
        loop {
            let next_is_hash = lines.peek().is_some_and(|l| {
                l.starts_with([' ', '\t']) && l.trim_start().starts_with("--hash=")
            });
            if !continues && !next_is_hash {
                break;
            }
            let Some(next) = lines.next() else { break };
            is_block = true;
            continues = requirement_line_code(next).trim_end().ends_with('\\');
        }

        let trimmed = first.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('-') {
            // Blank, a comment, or a pip option (`-e`, `-r`, `--hash=...`,
            // ...) — package names never start with `-`.
            continue;
        }

        if is_block {
            let declares_target = requirement_name(trimmed).is_some_and(|name| {
                normalize_package_name(name) == normalize_package_name(package_name)
            });
            if declares_target {
                return err!(
                    ErrorCode::InvalidConfig,
                    "cannot update {package_name} in this requirements file: its entry is \
                     hash-pinned or spans a line continuation, so the pin and its hashes have \
                     to be regenerated together (e.g. with pip-compile)"
                );
            }
            continue;
        }

        if let Some(rel) = find_specifier_span(trimmed, package_name) {
            let base = offset_of(text, trimmed);
            out.push(Match {
                range: base + rel.start..base + rel.end,
                package_manager: Some(PythonPackageManager::Pip),
            });
        }
    }
    Ok(out)
}

fn render_poetry(v: &PackageVersion) -> String {
    match v {
        PackageVersion::Exact(ver) => format!("=={ver}"),
        PackageVersion::Compatible(ver) => format!("^{ver}"),
    }
}

fn render_conda(v: &PackageVersion) -> String {
    match v {
        PackageVersion::Exact(ver) => format!("={ver}"),
        PackageVersion::Compatible(ver) => match next_minor(ver) {
            Some(upper) => format!(">={ver},<{upper}"),
            None => format!(">={ver}"),
        },
    }
}

fn render_pep440(v: &PackageVersion) -> String {
    match v {
        PackageVersion::Exact(ver) => format!("=={ver}"),
        PackageVersion::Compatible(ver) => format!("~={ver}"),
    }
}

/// Bumps `major.minor[.patch]` to `major.(minor+1).0`. Used to build an
/// explicit upper bound for ecosystems (conda) that have no native
/// "compatible release" operator. Returns `None` if `version` doesn't start
/// with at least `major.minor`.
fn next_minor(version: &str) -> Option<String> {
    let mut parts = version.split('.');
    let major: u64 = parts.next()?.parse().ok()?;
    let minor: u64 = parts.next()?.parse().ok()?;
    Some(format!("{major}.{}.0", minor + 1))
}

/// Normalizes a PyPI package name per PEP 503: case-insensitive, with
/// runs of `-`, `_`, and `.` treated as equivalent separators.
fn normalize_package_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if c == '-' || c == '_' || c == '.' {
            if !out.ends_with('-') {
                out.push('-');
            }
        } else {
            out.push(c.to_ascii_lowercase());
        }
    }
    out
}

/// Locates the version-specifier portion of a PEP 508 / conda-match-spec
/// style requirement token (`name[extras]<specifier>`), returning its byte
/// range within `text`. `text` is expected to hold a single requirement,
/// e.g. one line of `requirements.txt` (leading `-` list markers already
/// stripped) or one array/list entry.
///
/// Returns `None` if `text` doesn't name `package_name`, or if it names the
/// package with no specifier to replace (a bare `dbt-core` entry).
///
/// This is intentionally not a full PEP 508 parser: it does not validate
/// environment markers or extras syntax, it just skips past them.
fn find_specifier_span(text: &str, package_name: &str) -> Option<ops::Range<usize>> {
    let leading_ws = text.len() - text.trim_start().len();
    let rest = &text[leading_ws..];

    let name_len = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
        .unwrap_or(rest.len());
    if name_len == 0 {
        return None;
    }
    let name = &rest[..name_len];
    if normalize_package_name(name) != normalize_package_name(package_name) {
        return None;
    }

    let mut spec_start = leading_ws + name_len;
    if text[spec_start..].starts_with('[') {
        let close = text[spec_start..].find(']')?;
        spec_start += close + 1;
    }

    let spec_region = &text[spec_start..];
    let spec_region = spec_region.trim_end_matches(['\r', '\n']);
    let spec_len = spec_region.find([';', '#']).unwrap_or(spec_region.len());
    let spec_end = spec_start + spec_region[..spec_len].trim_end().len();

    if spec_start == spec_end {
        return None;
    }
    Some(spec_start..spec_end)
}

/// Finds the byte range of `raw`'s interior — the contents between a
/// matching pair of TOML quote characters (`"..."` or `'...'`) — excluding
/// the quotes themselves. Returns `None` if `raw` (the text at `span`) is
/// not a plain quoted string, or if its interior contains a backslash: a
/// backslash means the raw source and the decoded string value could
/// differ (an escape sequence), and we only ever operate on raw source
/// bytes, so we conservatively refuse rather than risk mis-splicing.
fn toml_string_interior_range(text: &str, span: ops::Range<usize>) -> Option<ops::Range<usize>> {
    let raw = text.get(span.clone())?;
    let mut chars = raw.chars();
    let quote = chars.next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    if raw.len() < 2 || !raw.ends_with(quote) {
        return None;
    }
    let inner = span.start + 1..span.end - 1;
    if text[inner.clone()].contains('\\') {
        return None;
    }
    Some(inner)
}

/// Returns the raw byte range of a YAML scalar's content, given the byte
/// offset of its first raw character (`start`) and its already-decoded
/// value: the bytes between the quotes for a quoted scalar, or the whole
/// token for a plain one.
///
/// `dbt_yaml::Span::end` is documented as approximate ("may contain leading
/// and trailing whitespace") and has been observed to run past a scalar
/// into a following sibling's indentation and `-` marker, so it isn't safe
/// to use as an end boundary. Instead this derives the range from `start`
/// and verifies it by checking that the raw bytes there decode to exactly
/// `decoded` with no escaping — a mismatch means an escape sequence was
/// used (the raw source and the decoded value differ), and we only ever
/// operate on raw source bytes, so we conservatively refuse rather than
/// risk mis-splicing.
fn yaml_scalar_interior_range(
    text: &str,
    start: usize,
    decoded: &str,
) -> Option<ops::Range<usize>> {
    let range = match text.as_bytes().get(start) {
        Some(b @ (b'"' | b'\'')) => {
            let interior = start + 1..start + 1 + decoded.len();
            if text.as_bytes().get(interior.end) != Some(b) {
                return None;
            }
            interior
        }
        _ => start..start + decoded.len(),
    };
    if text.get(range.clone())? == decoded {
        Some(range)
    } else {
        None
    }
}

/// One location within a manifest's raw text that needs to change, plus
/// which tool's syntax applies there (needed to pick the right rendering
/// for [`PythonManifestFormat::render_version`]).
struct Match {
    range: ops::Range<usize>,
    package_manager: Option<PythonPackageManager>,
}

/// Scans an array of PEP 508 requirement strings (a PEP 621
/// `[project.dependencies]`-shaped array) for `package_name`, tagging any
/// match with `package_manager`.
fn find_matches_in_requirement_array(
    text: &str,
    array: &toml_edit::Array,
    package_name: &str,
    package_manager: Option<PythonPackageManager>,
) -> Vec<Match> {
    let mut out = Vec::new();
    for item in array.iter() {
        let Some(span) = item.span() else { continue };
        let Some(interior) = toml_string_interior_range(text, span) else {
            continue;
        };
        if let Some(rel) = find_specifier_span(&text[interior.clone()], package_name) {
            out.push(Match {
                range: interior.start + rel.start..interior.start + rel.end,
                package_manager,
            });
        }
    }
    out
}

/// Scans a Poetry-shaped dependency table (`[tool.poetry.dependencies]` and
/// its `dev-dependencies`/`group.*.dependencies` siblings) for
/// `package_name`. Entries are `name = "spec"` or
/// `name = { version = "spec", ... }`; either way we replace the whole
/// `version` string, since (unlike a PEP 508 array entry) it holds nothing
/// but the specifier.
fn find_matches_in_poetry_table(
    text: &str,
    table: &dyn toml_edit::TableLike,
    package_name: &str,
) -> Vec<Match> {
    let mut out = Vec::new();
    for (key, item) in table.iter() {
        if normalize_package_name(key) != normalize_package_name(package_name) {
            continue;
        }
        let version_item = item
            .as_table_like()
            .and_then(|t| t.get("version"))
            .unwrap_or(item);
        let Some(span) = version_item.as_value().and_then(|v| v.span()) else {
            continue;
        };
        if let Some(interior) = toml_string_interior_range(text, span) {
            out.push(Match {
                range: interior,
                package_manager: Some(PythonPackageManager::Poetry),
            });
        }
    }
    out
}

/// Byte offset of the substring `sub` within `text`, given `sub` is a slice
/// derived from `text` (e.g. via `.lines()` or `.trim_start()`).
fn offset_of(text: &str, sub: &str) -> usize {
    sub.as_ptr() as usize - text.as_ptr() as usize
}

/// Splits `text` into lines as half-open byte ranges in ascending order (so
/// `.enumerate()` gives each line's zero-based index), each excluding its
/// trailing line terminator (`\n` or `\r\n`). Matches `str::lines()`'s
/// notion of a line — in particular, a trailing `\n` does not produce a
/// final empty line, and a `\r` unpaired with a following `\n` is kept.
fn lines(text: &str) -> impl Iterator<Item = ops::Range<usize>> + '_ {
    let mut pos = 0;
    std::iter::from_fn(move || {
        if pos >= text.len() {
            return None;
        }
        let start = pos;
        let Some(newline) = text[start..].find('\n').map(|i| start + i) else {
            pos = text.len();
            return Some(start..text.len());
        };
        pos = newline + 1;
        let end = if text[start..newline].ends_with('\r') {
            newline - 1
        } else {
            newline
        };
        Some(start..end)
    })
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Writes `contents` to a temp file next to `path` (so the final rename is
/// same-filesystem and therefore atomic), then renames it over `path`.
///
/// Unlike a truncate-then-write, a failure here (a full disk, the process
/// being killed) can only ever leave the *temp* file incomplete — `path`
/// itself is either untouched or, once the rename completes, fully replaced.
/// There's no window where `path` is observably truncated or half-written.
///
/// `PythonManifest` deliberately never keeps a handle open on `path` across
/// this call: on Windows, `MoveFileExW(..., MOVEFILE_REPLACE_EXISTING)`
/// reports `ERROR_ACCESS_DENIED` if any handle to the destination is still
/// open, even one granting `FILE_SHARE_DELETE` — that share mode permits the
/// rename to proceed once every handle closes, but not while one remains.
fn write_atomic(
    path: &Path,
    contents: &[u8],
    permissions: std::fs::Permissions,
) -> std::io::Result<()> {
    let dir = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    // `NamedTempFile` unlinks on drop unless it is persisted, so every early
    // return below — including a failed rename, which hands the file back
    // inside the error — cleans the staged file up.
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(contents)?;
    // Permissions before the sync, so they're durable alongside the bytes.
    tmp.as_file().set_permissions(permissions)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map(|_| ()).map_err(|e| e.error)
}

pub struct PythonManifest {
    format: PythonManifestFormat,
    // The original file path is saved to provide better error messages,
    path: PathBuf,
    // The full text of the file as of the last read (either the initial
    // `open`, or the last successful `ManifestReplacements::apply_to`).
    // `get_version_replacement` computes byte ranges against this snapshot,
    // so it must stay in lockstep with `checksum_sha256` below.
    contents: String,
    // Checksum of `contents`, with which it must stay in lockstep. Every
    // `ManifestReplacements` records this value as its `source_checksum` at
    // creation time, which is what gates whether that batch may be applied.
    checksum_sha256: [u8; 32],
}

const MANIFEST_CANDIDATES: &[(&str, PythonManifestFormat)] = &[
    ("pyproject.toml", PythonManifestFormat::Pyproject),
    ("Pipfile", PythonManifestFormat::Pipfile),
    ("environment.yml", PythonManifestFormat::CondaEnvironment),
    ("environment.yaml", PythonManifestFormat::CondaEnvironment),
    ("setup.cfg", PythonManifestFormat::SetupCfg),
    ("requirements.txt", PythonManifestFormat::Requirements),
];

impl PythonManifest {
    /// Walks `cwd` and its ancestors looking for the first directory that
    /// contains a recognized manifest file, per [`MANIFEST_CANDIDATES`]
    /// (checked in that order — the modern, structured formats first).
    ///
    /// Always checks `cwd`, then walks up at most 64 parent directories.
    pub fn detect(cwd: &Path) -> FsResult<Option<Self>> {
        // A sanity bound so a manifest-less cwd doesn't stat its way to the
        // filesystem root; not a limit on how projects may be laid out.
        const MAX_ANCESTOR_HOPS: usize = 64;

        for dir in cwd.ancestors().take(MAX_ANCESTOR_HOPS + 1) {
            for (filename, format) in MANIFEST_CANDIDATES {
                let path = dir.join(filename);
                if path.is_file() {
                    return Self::open(*format, path).map(Some);
                }
            }
        }
        Ok(None)
    }

    fn open(format: PythonManifestFormat, path: PathBuf) -> FsResult<Self> {
        // `symlink_metadata` doesn't follow the link, unlike the `is_file()`
        // check in `detect` above. A later edit replaces whatever's at `path`
        // via rename (see `write_atomic`), which would replace the symlink
        // itself with a plain file and leave its real target untouched and
        // stale -- so a symlinked manifest is rejected rather than edited.
        let symlink_meta = std::fs::symlink_metadata(&path).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "failed to open Python manifest at {}",
                path.display()
            )
            .with_cause(WrappedError::Io(e))
        })?;
        if symlink_meta.file_type().is_symlink() {
            return err!(
                ErrorCode::InvalidConfig,
                "{} is a symlink; refusing to edit a Python manifest through a symlink",
                path.display()
            );
        }

        let bytes = std::fs::read(&path).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "failed to read Python manifest at {}",
                path.display()
            )
            .with_cause(WrappedError::Io(e))
        })?;

        let checksum_sha256 = sha256(&bytes);
        let contents = String::from_utf8(bytes).map_err(|_| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "{} is not valid UTF-8",
                path.display()
            )
        })?;

        Ok(Self {
            format,
            path,
            contents,
            checksum_sha256,
        })
    }

    pub fn format(&self) -> PythonManifestFormat {
        self.format
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Finds every place in the manifest that declares `spec.name`, and
    /// renders the replacement text each of them needs to pin
    /// `spec.version`. Returns `None` if the manifest doesn't declare the
    /// package at all.
    pub fn get_version_replacement(
        &self,
        spec: &PackageSpec,
    ) -> FsResult<Option<ManifestReplacements>> {
        let matches = self.format.find_matches(&self.contents, &spec.name)?;

        if matches.is_empty() {
            return Ok(None);
        }

        let replacements = matches
            .into_iter()
            .map(|m| ManifestReplacement {
                range_replace: Range {
                    start: m.range.start,
                    end: m.range.end,
                },
                replacement: self
                    .format
                    .render_version(spec.version.clone(), m.package_manager),
            })
            .collect();

        Ok(Some(ManifestReplacements {
            replacements,
            source_checksum: self.checksum_sha256,
        }))
    }
}

// Holds all of the replacements that should be made to a manifest. This
// is necessary because some formats, such as pyproject.toml, can contain
// multiple tables of dependencies, and we will occasionally want to update
// more than one table. However, we always want to apply all replacements in
// a single batch.
pub struct ManifestReplacements {
    replacements: Vec<ManifestReplacement>,
    // Checksum of the manifest text the ranges in `replacements` were computed
    // against. Applying the batch to anything else would splice at stale
    // offsets, so this is what `apply_locked` validates the file against.
    source_checksum: [u8; 32],
}

impl ManifestReplacements {
    /// Writes the pending changes to `out` as colored, line-numbered diff
    /// hunks — a little surrounding context, then each affected line as it
    /// reads in `manifest` now and as it would read after `apply_to` — in
    /// file order, ready to print to a terminal so a user can review the
    /// change before it overwrites the file. Changes close enough to share
    /// context share a hunk, and changes to the same line render as a single
    /// pair. Reads only `manifest`'s in-memory snapshot; doesn't touch the
    /// file on disk.
    pub fn diff(&self, manifest: &PythonManifest, out: &mut dyn Write) -> FsResult<()> {
        const CONTEXT_LINES: usize = 2;

        let io_err = |e: std::io::Error| {
            fs_err!(ErrorCode::IoError, "failed to write diff").with_cause(WrappedError::Io(e))
        };

        let text = &manifest.contents;
        let all_lines: Vec<ops::Range<usize>> = lines(text).collect();
        let width = all_lines.len().max(1).to_string().len();

        let mut ordered: Vec<&ManifestReplacement> = self.replacements.iter().collect();
        ordered.sort_by_key(|r| r.range_replace.start);

        // Resolve every replacement to the single line it edits, collecting
        // the ones sharing a line so that line renders as one `-`/`+` pair.
        let mut edited_lines: Vec<(usize, Vec<&ManifestReplacement>)> = Vec::new();
        for r in ordered {
            let range: ops::Range<usize> = r.range_replace.into();
            if text.get(range.clone()).is_none() {
                return err!(
                    ErrorCode::Unexpected,
                    "replacement range {:?} is out of bounds for {}",
                    range,
                    manifest.path.display()
                );
            }

            let Some(line_idx) = all_lines
                .iter()
                .position(|l| l.start <= range.start && range.end <= l.end)
            else {
                return err!(
                    ErrorCode::Unexpected,
                    "replacement range {:?} does not fall within a single line of {}",
                    range,
                    manifest.path.display()
                );
            };

            match edited_lines.last_mut() {
                Some((idx, sharing_line)) if *idx == line_idx => sharing_line.push(r),
                _ => edited_lines.push((line_idx, vec![r])),
            }
        }

        // Edited lines whose context windows overlap render as a single hunk,
        // so a line never prints both as a `-`/`+` pair and as a neighbor's
        // context.
        let mut hunk_start = 0;
        let mut hunks_written = 0;
        while hunk_start < edited_lines.len() {
            let mut hunk_end = hunk_start + 1;
            while hunk_end < edited_lines.len()
                && edited_lines[hunk_end].0.saturating_sub(CONTEXT_LINES)
                    <= edited_lines[hunk_end - 1].0 + CONTEXT_LINES
            {
                hunk_end += 1;
            }
            let hunk = &edited_lines[hunk_start..hunk_end];
            hunk_start = hunk_end;

            if hunks_written > 0 {
                writeln!(out).map_err(io_err)?;
            }
            hunks_written += 1;

            let window_start = hunk[0].0.saturating_sub(CONTEXT_LINES);
            let window_end = (hunk[hunk.len() - 1].0 + 1 + CONTEXT_LINES).min(all_lines.len());

            let mut next_edit = 0;
            for (offset, line) in all_lines[window_start..window_end].iter().enumerate() {
                let line_idx = window_start + offset;
                let line = line.clone();
                let old_line = &text[line.clone()];

                let Some((_, sharing_line)) =
                    hunk.get(next_edit).filter(|(idx, _)| *idx == line_idx)
                else {
                    writeln!(
                        out,
                        "{}{}",
                        DIM.apply_to(format!("  {:>width$} | ", line_idx + 1)),
                        old_line
                    )
                    .map_err(io_err)?;
                    continue;
                };
                next_edit += 1;

                // Splicing back to front keeps the not-yet-applied offsets
                // valid.
                let mut new_line = old_line.to_string();
                for r in sharing_line.iter().rev() {
                    let range: ops::Range<usize> = r.range_replace.into();
                    new_line.replace_range(
                        range.start - line.start..range.end - line.start,
                        &r.replacement,
                    );
                }

                writeln!(
                    out,
                    "{}{}{}",
                    RED.apply_to("-"),
                    DIM.apply_to(format!(" {:>width$} | ", line_idx + 1)),
                    RED.apply_to(old_line)
                )
                .map_err(io_err)?;
                writeln!(
                    out,
                    "{}{}{}",
                    GREEN.apply_to("+"),
                    DIM.apply_to(format!(" {:>width$} | ", line_idx + 1)),
                    GREEN.apply_to(&new_line)
                )
                .map_err(io_err)?;
            }
        }

        Ok(())
    }

    /// Verifies `manifest.path` still holds the exact text these
    /// replacements were computed from, and — only if so — splices in every
    /// replacement and writes the result back.
    ///
    /// Reads and writes the path directly rather than through a handle held
    /// on `manifest` (see `write_atomic`'s doc comment for why); the
    /// checksum comparison below is what guards against a concurrent edit.
    pub fn apply_to(&self, manifest: &mut PythonManifest) -> FsResult<()> {
        let bytes = std::fs::read(&manifest.path).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "failed to read {}",
                manifest.path.display()
            )
            .with_cause(WrappedError::Io(e))
        })?;

        if sha256(&bytes) != self.source_checksum {
            return err!(
                ErrorCode::MergeConflict,
                "{} no longer holds the contents these replacements were computed from; aborting rather than splicing at stale offsets",
                manifest.path.display()
            );
        }

        let mut text = String::from_utf8(bytes).map_err(|_| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "{} is not valid UTF-8",
                manifest.path.display()
            )
        })?;

        // Apply back-to-front so earlier ranges stay valid as later splices
        // shift the string length.
        let mut ordered: Vec<&ManifestReplacement> = self.replacements.iter().collect();
        ordered.sort_by_key(|r| std::cmp::Reverse(r.range_replace.start));
        for r in ordered {
            let range: ops::Range<usize> = r.range_replace.into();
            if text.get(range.clone()).is_none() {
                return err!(
                    ErrorCode::Unexpected,
                    "replacement range {:?} is out of bounds for {}",
                    range,
                    manifest.path.display()
                );
            }
            text.replace_range(range, &r.replacement);
        }

        let permissions = std::fs::metadata(&manifest.path)
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "failed to write {}",
                    manifest.path.display()
                )
                .with_cause(WrappedError::Io(e))
            })?
            .permissions();

        write_atomic(&manifest.path, text.as_bytes(), permissions).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "failed to write {}",
                manifest.path.display()
            )
            .with_cause(WrappedError::Io(e))
        })?;

        manifest.checksum_sha256 = sha256(text.as_bytes());
        manifest.contents = text;

        Ok(())
    }
}

pub struct ManifestReplacement {
    // The half-open range of bytes to replace
    range_replace: Range<usize>,
    replacement: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_package_manager_serializes_to_spec_contract() {
        let cases = [
            (PythonPackageManager::Pip, "\"pip\""),
            (PythonPackageManager::Pipx, "\"pipx\""),
            (PythonPackageManager::Uv, "\"uv\""),
            (PythonPackageManager::Poetry, "\"poetry\""),
            (PythonPackageManager::Pdm, "\"pdm\""),
            (PythonPackageManager::Pipenv, "\"pipenv\""),
            (PythonPackageManager::Hatch, "\"hatch\""),
            (PythonPackageManager::Conda, "\"conda\""),
            (PythonPackageManager::Asdf, "\"asdf\""),
            (PythonPackageManager::Mise, "\"mise\""),
            (PythonPackageManager::Pyenv, "\"pyenv\""),
            (PythonPackageManager::Rye, "\"rye\""),
        ];
        for (variant, expected) in cases {
            assert_eq!(serde_json::to_string(&variant).unwrap(), expected);
        }
    }

    mod normalize_package_name_tests {
        use super::*;

        #[test]
        fn treats_dash_underscore_dot_and_case_as_equivalent() {
            assert_eq!(normalize_package_name("dbt-core"), "dbt-core");
            assert_eq!(normalize_package_name("dbt_core"), "dbt-core");
            assert_eq!(normalize_package_name("dbt.core"), "dbt-core");
            assert_eq!(normalize_package_name("DBT-Core"), "dbt-core");
            assert_eq!(normalize_package_name("dbt--core"), "dbt-core");
        }
    }

    mod find_specifier_span_tests {
        use super::*;

        #[test]
        fn finds_simple_range_specifier() {
            let text = "dbt-core>=1.2.3,<2.0";
            let range = find_specifier_span(text, "dbt-core").unwrap();
            assert_eq!(&text[range], ">=1.2.3,<2.0");
        }

        #[test]
        fn skips_extras() {
            let text = "dbt-core[extra]==1.2.3";
            let range = find_specifier_span(text, "dbt-core").unwrap();
            assert_eq!(&text[range], "==1.2.3");
        }

        #[test]
        fn stops_at_environment_marker() {
            let text = "dbt-core>=1.2.3; python_version>='3.8'";
            let range = find_specifier_span(text, "dbt-core").unwrap();
            assert_eq!(&text[range], ">=1.2.3");
        }

        #[test]
        fn stops_at_comment() {
            let text = "dbt-core==1.2.3  # pinned for reasons";
            let range = find_specifier_span(text, "dbt-core").unwrap();
            assert_eq!(&text[range], "==1.2.3");
        }

        #[test]
        fn returns_none_for_name_mismatch() {
            assert!(find_specifier_span("other-package==1.2.3", "dbt-core").is_none());
        }

        #[test]
        fn returns_none_for_bare_requirement_with_no_specifier() {
            assert!(find_specifier_span("dbt-core", "dbt-core").is_none());
        }

        #[test]
        fn matches_are_name_normalized() {
            let text = "dbt_core==1.2.3";
            let range = find_specifier_span(text, "dbt-core").unwrap();
            assert_eq!(&text[range], "==1.2.3");
        }
    }

    mod render_version_tests {
        use super::*;

        #[test]
        fn pep440_consumer_renders_exact_and_compatible() {
            assert_eq!(
                PythonManifestFormat::Requirements
                    .render_version(PackageVersion::Exact("1.2.3".into()), None),
                "==1.2.3"
            );
            assert_eq!(
                PythonManifestFormat::Requirements
                    .render_version(PackageVersion::Compatible("1.2.3".into()), None),
                "~=1.2.3"
            );
        }

        #[test]
        fn poetry_table_renders_caret_range() {
            assert_eq!(
                PythonManifestFormat::Pyproject.render_version(
                    PackageVersion::Compatible("1.2.3".into()),
                    Some(PythonPackageManager::Poetry)
                ),
                "^1.2.3"
            );
            assert_eq!(
                PythonManifestFormat::Pyproject.render_version(
                    PackageVersion::Exact("1.2.3".into()),
                    Some(PythonPackageManager::Poetry)
                ),
                "==1.2.3"
            );
        }

        #[test]
        fn pyproject_without_poetry_manager_uses_pep440() {
            assert_eq!(
                PythonManifestFormat::Pyproject.render_version(
                    PackageVersion::Compatible("1.2.3".into()),
                    Some(PythonPackageManager::Pip)
                ),
                "~=1.2.3"
            );
        }

        #[test]
        fn conda_renders_explicit_range() {
            assert_eq!(
                PythonManifestFormat::CondaEnvironment
                    .render_version(PackageVersion::Exact("1.2.3".into()), None),
                "=1.2.3"
            );
            assert_eq!(
                PythonManifestFormat::CondaEnvironment
                    .render_version(PackageVersion::Compatible("1.2.3".into()), None),
                ">=1.2.3,<1.3.0"
            );
        }

        #[test]
        fn conda_compatible_falls_back_without_minor() {
            assert_eq!(
                PythonManifestFormat::CondaEnvironment
                    .render_version(PackageVersion::Compatible("2".into()), None),
                ">=2"
            );
        }
    }

    mod detect_tests {
        use super::*;

        #[test]
        fn detects_pyproject_toml() {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(
                tmp.path().join("pyproject.toml"),
                "[project]\nname = \"x\"\n",
            )
            .unwrap();

            let manifest = PythonManifest::detect(tmp.path()).unwrap().unwrap();
            assert_eq!(manifest.format(), PythonManifestFormat::Pyproject);
            assert_eq!(manifest.path(), tmp.path().join("pyproject.toml"));
        }

        #[test]
        fn prefers_pyproject_toml_over_requirements_txt() {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(tmp.path().join("requirements.txt"), "dbt-core==1.2.3\n").unwrap();
            std::fs::write(
                tmp.path().join("pyproject.toml"),
                "[project]\nname = \"x\"\n",
            )
            .unwrap();

            let manifest = PythonManifest::detect(tmp.path()).unwrap().unwrap();
            assert_eq!(manifest.format(), PythonManifestFormat::Pyproject);
        }

        #[test]
        fn walks_up_to_an_ancestor_directory() {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(tmp.path().join("requirements.txt"), "dbt-core==1.2.3\n").unwrap();
            let nested = tmp.path().join("a").join("b");
            std::fs::create_dir_all(&nested).unwrap();

            let manifest = PythonManifest::detect(&nested).unwrap().unwrap();
            assert_eq!(manifest.path(), tmp.path().join("requirements.txt"));
        }

        #[test]
        fn stops_walking_up_past_the_hop_limit() {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(tmp.path().join("requirements.txt"), "dbt-core==1.2.3\n").unwrap();
            let mut nested = tmp.path().to_path_buf();
            for _ in 0..75 {
                nested.push("a");
            }
            std::fs::create_dir_all(&nested).unwrap();

            assert!(PythonManifest::detect(&nested).unwrap().is_none());
        }

        #[test]
        #[cfg(unix)]
        fn rejects_a_symlinked_manifest() {
            let tmp = tempfile::tempdir().unwrap();
            let real = tmp.path().join("real-requirements.txt");
            std::fs::write(&real, "dbt-core==1.2.3\n").unwrap();
            std::os::unix::fs::symlink(&real, tmp.path().join("requirements.txt")).unwrap();

            let Err(err) = PythonManifest::detect(tmp.path()) else {
                panic!("a symlinked manifest must be rejected");
            };
            assert_eq!(err.code, ErrorCode::InvalidConfig);
        }
    }

    mod version_replacement_tests {
        use super::*;

        fn manifest_with(dir: &Path, filename: &str, content: &str) -> PythonManifest {
            std::fs::write(dir.join(filename), content).unwrap();
            PythonManifest::detect(dir).unwrap().unwrap()
        }

        fn spec(version: PackageVersion) -> PackageSpec {
            PackageSpec {
                name: "dbt-core".to_string(),
                version,
            }
        }

        /// Writes `content` as `filename`, replaces `dbt-core`'s version with
        /// `version`, and returns the resulting file contents.
        fn round_trip(filename: &str, content: &str, version: PackageVersion) -> String {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(tmp.path(), filename, content);
            let replacements = manifest
                .get_version_replacement(&spec(version))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();
            std::fs::read_to_string(manifest.path()).unwrap()
        }

        #[test]
        fn pyproject_pep621_array_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "pyproject.toml",
                "[project]\nname = \"x\"\ndependencies = [\"dbt-core>=1.2.3,<2\"]\n",
            );
            let mut manifest = manifest;
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Compatible("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains(r#""dbt-core~=1.5.0""#), "got: {after}");
        }

        #[test]
        fn pyproject_poetry_table_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "pyproject.toml",
                "[tool.poetry.dependencies]\ndbt-core = \"^1.2.3\"\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Compatible("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains(r#"dbt-core = "^1.5.0""#), "got: {after}");
        }

        #[test]
        fn pyproject_uv_legacy_dev_dependencies_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "pyproject.toml",
                "[tool.uv]\ndev-dependencies = [\"dbt-core>=1.2.3,<2\"]\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Compatible("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains(r#""dbt-core~=1.5.0""#), "got: {after}");
        }

        #[test]
        fn pyproject_pdm_legacy_dev_dependencies_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "pyproject.toml",
                "[tool.pdm.dev-dependencies]\ntest = [\"dbt-core>=1.2.3,<2\"]\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains(r#""dbt-core==1.5.0""#), "got: {after}");
        }

        #[test]
        fn pyproject_poetry_legacy_dev_dependencies_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "pyproject.toml",
                "[tool.poetry.dev-dependencies]\ndbt-core = \"^1.2.3\"\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Compatible("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains(r#"dbt-core = "^1.5.0""#), "got: {after}");
        }

        #[test]
        fn pyproject_poetry_group_dependencies_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "pyproject.toml",
                "[tool.poetry.group.test.dependencies]\ndbt-core = \"^1.2.3\"\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Compatible("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains(r#"dbt-core = "^1.5.0""#), "got: {after}");
        }

        #[test]
        fn pipfile_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "Pipfile",
                "[packages]\ndbt-core = \"==1.2.3\"\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains(r#"dbt-core = "==1.5.0""#), "got: {after}");
        }

        #[test]
        fn requirements_txt_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "other-package==0.1.0\ndbt-core==1.2.3\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert_eq!(after, "other-package==0.1.0\ndbt-core==1.5.0\n");
        }

        #[test]
        fn requirements_txt_same_line_hash_pin_is_rejected() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "dbt-core==1.2.3 --hash=sha256:abc123\n",
            );
            let Err(err) =
                manifest.get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
            else {
                panic!("a hash-pinned requirement must be rejected");
            };
            assert_eq!(err.code, ErrorCode::InvalidConfig);
        }

        #[test]
        fn requirements_txt_continued_hash_block_is_rejected() {
            let tmp = tempfile::tempdir().unwrap();
            let content = "other-package==0.1.0\n\
                 dbt-core==1.2.3 \\\n    \
                 --hash=sha256:abc123 \\\n    \
                 --hash=sha256:def456\n";
            let manifest = manifest_with(tmp.path(), "requirements.txt", content);
            let Err(err) =
                manifest.get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
            else {
                panic!("a hash-pinned requirement must be rejected");
            };
            assert_eq!(err.code, ErrorCode::InvalidConfig);

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert_eq!(after, content, "the manifest must be left untouched");
        }

        #[test]
        fn requirements_txt_bare_line_continuation_is_rejected() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(tmp.path(), "requirements.txt", "dbt-core==1.2.3 \\\n");
            let Err(err) =
                manifest.get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
            else {
                panic!("splicing over a continuation backslash must be rejected");
            };
            assert_eq!(err.code, ErrorCode::InvalidConfig);
        }

        #[test]
        fn requirements_txt_comment_does_not_continue_onto_the_next_line() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "# pinned by policy \\\ndbt-core==1.2.3\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert_eq!(after, "# pinned by policy \\\ndbt-core==1.5.0\n");
        }

        #[test]
        fn requirements_txt_hash_block_for_another_package_still_allows_an_edit() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "other-package==0.1.0 \\\n    --hash=sha256:abc123\ndbt-core==1.2.3\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert_eq!(
                after,
                "other-package==0.1.0 \\\n    --hash=sha256:abc123\ndbt-core==1.5.0\n"
            );
        }

        #[test]
        fn setup_cfg_multiline_install_requires_round_trips() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "setup.cfg",
                "[options]\ninstall_requires =\n    other-package==0.1.0\n    dbt-core>=1.2.3\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Compatible("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            let after = std::fs::read_to_string(manifest.path()).unwrap();
            assert!(after.contains("dbt-core~=1.5.0"), "got: {after}");
        }

        #[test]
        fn conda_environment_top_level_round_trips() {
            let after = round_trip(
                "environment.yml",
                "name: env\ndependencies:\n  - python=3.11\n  - dbt-core=1.2.3\n",
                PackageVersion::Exact("1.5.0".into()),
            );
            assert!(after.contains("dbt-core=1.5.0"), "got: {after}");
        }

        #[test]
        fn conda_environment_nested_pip_list_uses_pip_syntax() {
            let after = round_trip(
                "environment.yml",
                "name: env\ndependencies:\n  - python=3.11\n  - pip:\n    - dbt-core==1.2.3\n",
                PackageVersion::Compatible("1.5.0".into()),
            );
            assert!(after.contains("dbt-core~=1.5.0"), "got: {after}");
        }

        #[test]
        fn conda_environment_double_quoted_scalar_round_trips() {
            let after = round_trip(
                "environment.yml",
                "name: env\ndependencies:\n  - python=3.11\n  - \"dbt-core=1.2.3\"\n",
                PackageVersion::Exact("1.5.0".into()),
            );
            assert!(after.contains("\"dbt-core=1.5.0\""), "got: {after}");
        }

        #[test]
        fn conda_environment_single_quoted_scalar_round_trips() {
            let after = round_trip(
                "environment.yml",
                "name: env\ndependencies:\n  - python=3.11\n  - 'dbt-core=1.2.3'\n",
                PackageVersion::Exact("1.5.0".into()),
            );
            assert!(after.contains("'dbt-core=1.5.0'"), "got: {after}");
        }

        #[test]
        fn conda_environment_nested_pip_quoted_scalar_round_trips() {
            let after = round_trip(
                "environment.yml",
                "name: env\ndependencies:\n  - python=3.11\n  - pip:\n    - \"dbt-core==1.2.3\"\n",
                PackageVersion::Compatible("1.5.0".into()),
            );
            assert!(after.contains("\"dbt-core~=1.5.0\""), "got: {after}");
        }

        #[test]
        fn conda_environment_flow_style_top_level_round_trips() {
            let after = round_trip(
                "environment.yml",
                "name: env\ndependencies: [python=3.11, \"other-package==0.1.0\", dbt-core=1.2.3]\n",
                PackageVersion::Exact("1.5.0".into()),
            );
            assert!(after.contains("dbt-core=1.5.0"), "got: {after}");
            assert!(after.contains("\"other-package==0.1.0\""), "got: {after}");
        }

        #[test]
        fn conda_environment_flow_style_nested_pip_round_trips() {
            let after = round_trip(
                "environment.yml",
                "name: env\ndependencies:\n  - python=3.11\n  - pip: [dbt-core==1.2.3]\n",
                PackageVersion::Compatible("1.5.0".into()),
            );
            assert!(after.contains("dbt-core~=1.5.0"), "got: {after}");
        }

        #[test]
        fn conda_environment_quoted_scalar_with_escape_is_conservatively_skipped() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "environment.yml",
                "name: env\ndependencies:\n  - \"dbt-core=1.2.3\\n\"\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap();
            assert!(
                replacements.is_none(),
                "should not risk splicing an escaped scalar"
            );
        }

        #[test]
        fn missing_package_returns_none() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(tmp.path(), "requirements.txt", "other-package==0.1.0\n");
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap();
            assert!(replacements.is_none());
        }

        fn diff_to_string(
            replacements: &ManifestReplacements,
            manifest: &PythonManifest,
        ) -> String {
            let mut buf = Vec::new();
            replacements.diff(manifest, &mut buf).unwrap();
            String::from_utf8(buf).unwrap()
        }

        fn diff_err(
            replacements: &ManifestReplacements,
            manifest: &PythonManifest,
        ) -> Box<dbt_common::FsError> {
            let mut buf = Vec::new();
            replacements.diff(manifest, &mut buf).unwrap_err()
        }

        #[test]
        fn diff_shows_old_and_new_lines_with_line_numbers_without_touching_the_file() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "other-package==0.1.0\ndbt-core==1.2.3\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");

            let diff = diff_to_string(&replacements, &manifest);
            assert!(diff.contains("- 2 | dbt-core==1.2.3"), "got: {diff}");
            assert!(diff.contains("+ 2 | dbt-core==1.5.0"), "got: {diff}");
            // The other line is included as context, unmarked.
            assert!(diff.contains("  1 | other-package==0.1.0"), "got: {diff}");

            let on_disk = std::fs::read_to_string(manifest.path()).unwrap();
            assert_eq!(on_disk, "other-package==0.1.0\ndbt-core==1.2.3\n");
        }

        #[test]
        fn diff_orders_multiple_replacements_by_file_position() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "pyproject.toml",
                "[tool.poetry.dependencies]\ndbt-core = \"^1.2.3\"\n\n[tool.poetry.group.test.dependencies]\ndbt-core = \"^1.2.3\"\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Compatible("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");

            let diff = diff_to_string(&replacements, &manifest);
            assert!(diff.contains("- 2 | dbt-core"), "got: {diff}");
            assert!(diff.contains("- 5 | dbt-core"), "got: {diff}");

            let first = diff.find("- 2 | dbt-core").unwrap();
            let second = diff.find("- 5 | dbt-core").unwrap();
            assert!(first < second, "expected line 2 before line 5: {diff}");
        }

        #[test]
        fn diff_strips_carriage_returns_from_crlf_line_endings() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "other-package==0.1.0\r\ndbt-core==1.2.3\r\nthird-package==3.0.0\r\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");

            let diff = diff_to_string(&replacements, &manifest);
            assert!(!diff.contains('\r'), "got: {diff:?}");
            assert!(diff.contains("- 2 | dbt-core==1.2.3\n"), "got: {diff:?}");
            assert!(diff.contains("+ 2 | dbt-core==1.5.0\n"), "got: {diff:?}");
            assert!(
                diff.contains("  1 | other-package==0.1.0\n"),
                "got: {diff:?}"
            );
            assert!(
                diff.contains("  3 | third-package==3.0.0\n"),
                "got: {diff:?}"
            );
        }

        #[test]
        fn diff_rejects_a_replacement_spanning_more_than_one_line() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "dbt-core==1.2.3\nother-package==0.1.0\n",
            );
            let replacements = ManifestReplacements {
                replacements: vec![ManifestReplacement {
                    range_replace: Range { start: 9, end: 25 },
                    replacement: "1.5.0".into(),
                }],
                source_checksum: manifest.checksum_sha256,
            };

            let err = diff_err(&replacements, &manifest);
            assert_eq!(err.code, ErrorCode::Unexpected);
            assert!(
                err.context.contains("does not fall within a single line"),
                "got: {}",
                err.context
            );
        }

        #[test]
        fn diff_rejects_a_replacement_starting_past_the_last_line() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(tmp.path(), "requirements.txt", "dbt-core==1.2.3\n");
            let replacements = ManifestReplacements {
                replacements: vec![ManifestReplacement {
                    range_replace: Range { start: 16, end: 16 },
                    replacement: "other-package==0.1.0\n".into(),
                }],
                source_checksum: manifest.checksum_sha256,
            };

            let err = diff_err(&replacements, &manifest);
            assert_eq!(err.code, ErrorCode::Unexpected);
            assert!(
                err.context.contains("does not fall within a single line"),
                "got: {}",
                err.context
            );
        }

        #[test]
        fn diff_renders_nearby_replacements_as_one_hunk() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "alpha==0.1.0\ndbt-core==1.2.3\nmiddle==0.0.1\ndbt-core==1.2.3\nomega==9.9.9\n",
            );
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");

            let diff = diff_to_string(&replacements, &manifest);
            for expected in [
                "  1 | alpha==0.1.0\n",
                "- 2 | dbt-core==1.2.3\n",
                "+ 2 | dbt-core==1.5.0\n",
                "  3 | middle==0.0.1\n",
                "- 4 | dbt-core==1.2.3\n",
                "+ 4 | dbt-core==1.5.0\n",
                "  5 | omega==9.9.9\n",
            ] {
                assert_eq!(
                    diff.matches(expected).count(),
                    1,
                    "expected {expected:?} exactly once, got: {diff}"
                );
            }
            // Neither changed line may also appear as unmarked context.
            assert!(!diff.contains("  2 | "), "got: {diff}");
            assert!(!diff.contains("  4 | "), "got: {diff}");
            // A single hunk, so no blank-line separator.
            assert!(!diff.contains("\n\n"), "got: {diff}");
        }

        #[test]
        fn diff_combines_replacements_sharing_a_line_into_one_pair() {
            let tmp = tempfile::tempdir().unwrap();
            let manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "dbt-core==1.2.3 other-pkg==2.0.0\n",
            );
            // Given in reverse file order, to exercise `diff`'s sort.
            let replacements = ManifestReplacements {
                replacements: vec![
                    ManifestReplacement {
                        range_replace: Range { start: 27, end: 32 },
                        replacement: "9.9.9".into(),
                    },
                    ManifestReplacement {
                        range_replace: Range { start: 10, end: 15 },
                        replacement: "1.10.0".into(),
                    },
                ],
                source_checksum: manifest.checksum_sha256,
            };

            let diff = diff_to_string(&replacements, &manifest);
            assert_eq!(
                diff,
                "- 1 | dbt-core==1.2.3 other-pkg==2.0.0\n\
                 + 1 | dbt-core==1.10.0 other-pkg==9.9.9\n",
                "got: {diff}"
            );
        }

        #[test]
        fn apply_to_rejects_a_manifest_modified_since_it_was_read() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(tmp.path(), "requirements.txt", "dbt-core==1.2.3\n");
            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");

            // Someone else edits the file after we read it but before we apply.
            std::fs::write(manifest.path(), "dbt-core==1.2.3\nother-package==2.0.0\n").unwrap();

            let err = replacements.apply_to(&mut manifest).unwrap_err();
            assert_eq!(err.code, ErrorCode::MergeConflict);
        }

        #[test]
        fn apply_to_rejects_a_batch_computed_before_an_earlier_apply() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(
                tmp.path(),
                "requirements.txt",
                "dbt-core==1.2.3\nother-package==0.1.0\n",
            );

            // Both batches are computed from the same snapshot, so the second
            // one's byte ranges are only valid until the first is applied.
            let first = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.10.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            let stale = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.6.0".into())))
                .unwrap()
                .expect("dbt-core is declared");

            first.apply_to(&mut manifest).unwrap();
            let after_first = std::fs::read_to_string(manifest.path()).unwrap();

            let err = stale.apply_to(&mut manifest).unwrap_err();
            assert_eq!(err.code, ErrorCode::MergeConflict);
            assert_eq!(
                std::fs::read_to_string(manifest.path()).unwrap(),
                after_first,
                "a rejected batch must leave the manifest untouched"
            );
        }

        #[test]
        fn a_batch_recomputed_after_an_apply_still_applies() {
            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(tmp.path(), "requirements.txt", "dbt-core==1.2.3\n");

            let first = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            first.apply_to(&mut manifest).unwrap();

            let second = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.6.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            second.apply_to(&mut manifest).unwrap();

            assert_eq!(
                std::fs::read_to_string(manifest.path()).unwrap(),
                "dbt-core==1.6.0\n"
            );
        }

        #[test]
        #[cfg(unix)]
        fn apply_to_replaces_the_manifest_via_atomic_rename() {
            use std::os::unix::fs::MetadataExt;

            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(tmp.path(), "requirements.txt", "dbt-core==1.2.3\n");
            let original_ino = std::fs::metadata(manifest.path()).unwrap().ino();

            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            // A real rename produces a new inode at the same path, rather
            // than truncating and rewriting the original file's contents in
            // place -- there's no window where a reader could observe a
            // truncated or partially-written manifest.
            let new_ino = std::fs::metadata(manifest.path()).unwrap().ino();
            assert_ne!(original_ino, new_ino);
            assert_eq!(
                std::fs::read_to_string(manifest.path()).unwrap(),
                "dbt-core==1.5.0\n"
            );

            let entries: Vec<_> = std::fs::read_dir(tmp.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert_eq!(
                entries,
                vec![std::ffi::OsString::from("requirements.txt")],
                "the staged file should not survive a successful apply"
            );
        }

        #[test]
        #[cfg(unix)]
        fn apply_to_preserves_the_manifest_permissions() {
            use std::os::unix::fs::PermissionsExt;

            let tmp = tempfile::tempdir().unwrap();
            let mut manifest = manifest_with(tmp.path(), "requirements.txt", "dbt-core==1.2.3\n");
            std::fs::set_permissions(manifest.path(), std::fs::Permissions::from_mode(0o640))
                .unwrap();

            let replacements = manifest
                .get_version_replacement(&spec(PackageVersion::Exact("1.5.0".into())))
                .unwrap()
                .expect("dbt-core is declared");
            replacements.apply_to(&mut manifest).unwrap();

            // The staged file is created private to the owner, so the mode has
            // to be carried over explicitly before it replaces the manifest.
            let mode = std::fs::metadata(manifest.path())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o640);
        }
    }
}
