use std::ffi::OsString;

use crate::warn_error_options::WarnErrorOptions;

pub trait CliParserTrait {
    type CliType;

    /// Parse from `std::env::args_os()`, [exit][clap::Error::exit] on error.
    fn parse(&self) -> Self::CliType;

    /// Parse from `std::env::args_os()`, return Err on error.
    fn try_parse(&self) -> Result<Self::CliType, clap::Error>;

    /// Parse from iterator, [exit][clap::Error::exit] on error.
    fn parse_from<I, T>(&self, itr: I) -> Self::CliType
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone;

    /// Parse from iterator, return Err on error.
    fn try_parse_from<I, T>(&self, itr: I) -> Result<Self::CliType, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone;

    /// Derive the `--fail-fast` value from the parsed CLI.
    ///
    /// This is necessary to keep the generic test code
    /// until we unify both dbt-cli and dbt-sa-cli into
    /// a single one.
    fn fail_fast_flag(&self, cli: &Self::CliType) -> bool;

    /// Extract warn-error options from the parsed CLI.
    fn warn_error_options(&self, _cli: &Self::CliType) -> Option<WarnErrorOptions> {
        None
    }
}
