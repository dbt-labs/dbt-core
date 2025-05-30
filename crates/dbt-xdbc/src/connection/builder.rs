//! A builder for a [`Connection`]
//!
//!

use std::fmt;

use adbc_core::{
    error::Result,
    options::{OptionConnection, OptionValue},
};

use crate::{builder::BuilderIter, Connection, Database};

/// A builder for [`Connection`].
///
/// The builder can be used to initialize a [`Connection`] with
/// [`Builder::build`].
#[derive(Clone, Default)]
pub struct Builder {
    // This builder defines 0 static options and all options go into the `other` field.
    /// Ordered list of connection options.
    pub other: Vec<(OptionConnection, OptionValue)>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Builder");
        for (name, value) in &self.other {
            dbg.field(name.as_ref(), &value);
        }
        dbg.finish()
    }
}

impl Builder {
    pub fn with_typed_option(
        &mut self,
        option: OptionConnection,
        value: OptionValue,
    ) -> Result<&mut Self> {
        // TODO(felipecrv): add validations for options like AutoCommit when we add Postgres support
        self.other.push((option, value));
        Ok(self)
    }

    pub fn with_option(
        &mut self,
        name: OptionConnection,
        value: impl Into<String>,
    ) -> Result<&mut Self> {
        self.with_typed_option(name, OptionValue::String(value.into()))
    }

    pub fn with_named_option(
        &mut self,
        name: impl AsRef<str>,
        value: impl Into<String>,
    ) -> Result<&mut Self> {
        let option = OptionConnection::Other(name.as_ref().to_string());
        self.with_typed_option(option, OptionValue::String(value.into()))
    }

    /// Attempt to initialize a [`Connection`] using the values provided to
    /// this builder using the provided [`Database`].
    pub fn build(self, database: &mut Box<dyn Database>) -> Result<Box<dyn Connection>> {
        let iter = self.into_iter();
        let opts = iter.collect::<Vec<_>>();
        database.new_connection_with_opts(opts)
    }
}

impl IntoIterator for Builder {
    type Item = (OptionConnection, OptionValue);
    type IntoIter = BuilderIter<OptionConnection, 0>;

    fn into_iter(self) -> Self::IntoIter {
        let fixed = [];
        BuilderIter::new(fixed, self.other)
    }
}
