use crate::publish::Environment;
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct PypiPublishArgs {
    /// `staging`, `prod`, or `test-pypi`.
    #[arg(long, value_enum)]
    pub environment: Environment,

    /// Wheel source dir. Defaults to `<workspace>/target/wheels`.
    #[arg(long)]
    pub dist: Option<PathBuf>,

    /// Restrict to wheels stamped with this SemVer.
    #[arg(long)]
    pub version: Option<String>,
}

#[derive(Args, Debug)]
pub struct PackArgs {
    /// Directory of pre-built binaries, one per cargo target triple
    /// (`.exe` suffix marks Windows).
    #[arg(long, value_name = "DIR")]
    pub binaries_dir: PathBuf,

    /// Release SemVer; translated to PEP 440 for the wheel filename.
    #[arg(long)]
    pub version: String,

    /// Output dir. Defaults to `<workspace>/target/wheels`.
    #[arg(long)]
    pub out: Option<String>,

    /// Script name inside the wheel. Defaults to `[project].name`.
    #[arg(long)]
    pub bin_name: Option<String>,
}

#[derive(Args, Debug)]
pub struct BumpCargoVersionArgs {
    /// New workspace SemVer. See README for accepted shapes.
    pub version: String,

    /// Skip `cargo update --workspace --offline` after writing.
    #[arg(long)]
    pub no_lockfile: bool,
}

#[derive(Args, Debug)]
pub struct HomebrewRenderArgs {
    /// Directory of release tarballs, one per cargo target triple.
    /// Filename pattern: `{tarball-prefix}{version}-{target}.tar.gz`.
    /// SHA256 of each tarball is computed in-process.
    /// Mutually exclusive with `--sha256sums`; exactly one must be provided.
    #[arg(
        long,
        value_name = "DIR",
        conflicts_with = "sha256sums",
        required_unless_present = "sha256sums"
    )]
    pub tarballs_dir: Option<PathBuf>,

    /// `sha256sum`-format manifest file (one `<hex>  <filename>` line per
    /// tarball). Filenames must still match `{tarball-prefix}{version}-…`.
    /// Use this when the manifest is already produced upstream and there's
    /// no reason to re-download / re-hash the tarballs.
    /// Mutually exclusive with `--tarballs-dir`; exactly one must be provided.
    #[arg(
        long,
        value_name = "FILE",
        conflicts_with = "tarballs_dir",
        required_unless_present = "tarballs_dir"
    )]
    pub sha256sums: Option<PathBuf>,

    /// Release SemVer (must match the version baked into the tarball filenames).
    #[arg(long)]
    pub version: String,

    /// HTTPS URL template for tarball downloads.
    /// Placeholders: `{filename}`, `{version}`, `{target}`.
    #[arg(long)]
    pub url_template: String,

    /// Output path for the rendered formula.
    /// Defaults to `<workspace>/target/homebrew/Formula/<formula-name>.rb`.
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Formula class/file name. Defaults to `[project].name` from pyproject.toml.
    #[arg(long)]
    pub formula_name: Option<String>,

    /// Name of the binary inside each tarball. Defaults to `dbt`.
    #[arg(long, default_value = "dbt")]
    pub binary_name: String,

    /// Name the binary should be installed as (Homebrew's rename-on-install
    /// via `bin.install "X" => "Y"`). Defaults to `--formula-name`, which
    /// matches the historic behavior. Use this when the installed command
    /// should differ from BOTH the tarball's filename and the formula's
    /// filename — e.g. `dbt-core.rb` installs the binary as `dbt`.
    #[arg(long)]
    pub install_as: Option<String>,

    /// Other formula names that conflict with this one (repeatable). Use
    /// when two formulas would install a binary at the same path — e.g.
    /// `dbt` and `dbt-core` both install `/<prefix>/bin/dbt`, so each
    /// formula declares the other as a conflict.
    #[arg(long = "conflicts-with", action = clap::ArgAction::Append)]
    pub conflicts_with: Vec<String>,

    /// Tarball filename prefix.
    /// Defaults to `fs-v` (so files are `fs-v{version}-{target}.tar.gz`).
    #[arg(long, default_value = "fs-v")]
    pub tarball_prefix: String,
}

#[derive(Args, Debug)]
pub struct HomebrewPublishArgs {
    /// Path to a rendered `.rb` file (e.g. produced by `homebrew render`).
    #[arg(long)]
    pub formula: PathBuf,

    /// Tap repository URL, e.g. `https://github.com/dbt-labs/homebrew-dbt.git`.
    #[arg(long)]
    pub tap_repo: String,

    /// Tap branch.
    #[arg(long, default_value = "main")]
    pub tap_branch: String,

    /// Release SemVer (used in the commit message only).
    #[arg(long)]
    pub version: String,

    /// Name of the env var to read the GitHub PAT from. The PAT needs
    /// `repo` scope on the tap repo. Defaults to `HOMEBREW_TAP_REPO_TOKEN`.
    #[arg(long, default_value = "HOMEBREW_TAP_REPO_TOKEN")]
    pub token_env: String,

    /// Override the commit author name on the tap commit. If unset, uses
    /// whatever `git config user.name` is set to in the cloned tap.
    #[arg(long)]
    pub commit_author: Option<String>,

    /// Override the commit author email on the tap commit. If unset, uses
    /// whatever `git config user.email` is set to in the cloned tap.
    #[arg(long)]
    pub commit_email: Option<String>,

    /// Render, stage, and show the diff but do NOT push.
    #[arg(long)]
    pub dry_run: bool,
}
