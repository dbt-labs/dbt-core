use std::ffi::OsString;

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
}
