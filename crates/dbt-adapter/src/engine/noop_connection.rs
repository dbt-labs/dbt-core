use arrow_schema::Schema;
use dbt_adbc::*;

pub struct NoopConnection;

impl Connection for NoopConnection {
    fn new_statement(&mut self) -> adbc_core::error::Result<Box<dyn Statement>> {
        // Return an error instead of panicking so callers can handle gracefully
        Err(adbc_core::error::Error::with_message_and_status(
            "NoopConnection does not support statement creation",
            adbc_core::error::Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // No-op for cancel - nothing to cancel
        Ok(())
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        // No-op for commit - no transaction state
        Ok(())
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        // No-op for rollback - no transaction state
        Ok(())
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        // Return an error instead of panicking
        Err(adbc_core::error::Error::with_message_and_status(
            "NoopConnection does not support table schema retrieval",
            adbc_core::error::Status::NotImplemented,
        ))
    }

    fn get_objects<'a>(
        &'a self,
        _depth: adbc_core::options::ObjectDepth,
        _catalog: Option<&'a str>,
        _db_schema: Option<&'a str>,
        _table_name: Option<&'a str>,
        _table_type: Option<Vec<&'a str>>,
        _column_name: Option<&'a str>,
    ) -> adbc_core::error::Result<Box<dyn arrow_array::RecordBatchReader + Send + 'a>> {
        // Return an error instead of panicking
        Err(adbc_core::error::Error::with_message_and_status(
            "NoopConnection does not support object retrieval",
            adbc_core::error::Status::NotImplemented,
        ))
    }
}
