use clap::{Parser, Subcommand};
use dbt_ci::{BumpCargoVersionArgs, PackArgs, PypiPublishArgs};
use std::process::ExitCode;

#[derive(Parser, Debug)]
#[command(
    name = "cargo-ci",
    bin_name = "cargo ci",
    about = "Release-pipeline commands for dbt-fusion",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Write `[workspace.package].version` and refresh `Cargo.lock`.
    #[command(name = "bump-cargo-version")]
    BumpCargoVersion(BumpCargoVersionArgs),

    /// Python wheel commands.
    #[command(subcommand)]
    Pypi(PypiCmd),
}

#[derive(Subcommand, Debug)]
enum PypiCmd {
    /// Pack pre-built binaries into per-platform wheels.
    Pack(PackArgs),

    /// Publish wheels from `--dist`.
    Publish(PypiPublishArgs),
}

fn main() -> ExitCode {
    match Cli::parse().cmd {
        Cmd::BumpCargoVersion(args) => dbt_ci::bump_cargo_version::execute(args),
        Cmd::Pypi(PypiCmd::Pack(args)) => dbt_ci::pack::execute(args),
        Cmd::Pypi(PypiCmd::Publish(args)) => dbt_ci::publish::execute(args),
    }
}
