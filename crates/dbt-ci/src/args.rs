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
