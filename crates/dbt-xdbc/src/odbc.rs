use crate::{odbc_api::*, str_from_sqlstate};
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use arrow_array::builder::{BooleanBuilder, GenericByteBuilder, PrimitiveBuilder};
use arrow_array::types::*;
use arrow_array::{Array, ArrowPrimitiveType, OffsetSizeTrait, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use odbc_sys::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

fn error_with_diagnostics(error: Error, handle_type: HandleType, handle: Handle) -> Error {
    let mut augmented_error = error;

    // get message, if any, and SQLSTATE from the first diagnostic record (SQL_RETURN_CODE)
    let record_number = HeaderDiagnosticIdentifier::ReturnCode;
    let diagnostic = sql_get_diag_rec(handle_type, handle, record_number as SmallInt);
    if let Ok(Some(diagnostic)) = diagnostic {
        if !diagnostic.message.is_empty() {
            augmented_error
                .message
                .push_str(&format!(": {}", diagnostic.message));
        }
        debug_assert!(augmented_error.sqlstate == [0; 5]);
        augmented_error.sqlstate = diagnostic.sqlstate;
    }

    let interesting_records = [
        HeaderDiagnosticIdentifier::Number as SmallInt,
        // HeaderDiagnosticIdentifier::RowCount as SmallInt,
        // HeaderDiagnosticIdentifier::SqlState as SmallInt,
        // HeaderDiagnosticIdentifier::Native as SmallInt,
        HeaderDiagnosticIdentifier::MessageText as SmallInt,
        // HeaderDiagnosticIdentifier::DynamicFunction as SmallInt,
        // HeaderDiagnosticIdentifier::ClassOrigin as SmallInt,
        // HeaderDiagnosticIdentifier::SubclassOrigin as SmallInt,
        // HeaderDiagnosticIdentifier::ConnectionName as SmallInt,
        // HeaderDiagnosticIdentifier::ServerName as SmallInt,
        // HeaderDiagnosticIdentifier::DynamicFunctionCode as SmallInt,
        // HeaderDiagnosticIdentifier::CursorRowCount as SmallInt,
        // HeaderDiagnosticIdentifier::RowNumber as SmallInt,
        // HeaderDiagnosticIdentifier::ColumnNumber as SmallInt,
    ];
    for record_number in interesting_records {
        let diagnostic = sql_get_diag_rec(handle_type, handle, record_number);
        if let Ok(Some(diagnostic)) = diagnostic {
            if !diagnostic.message.is_empty() {
                augmented_error
                    .message
                    .push_str(&format!("\n  {}", diagnostic.message));
            }
        }
    }
    augmented_error
}

#[derive(Debug)]
pub struct OdbcEnvInner {
    henv: HEnv,
}

unsafe impl Send for OdbcEnvInner {}
unsafe impl Sync for OdbcEnvInner {}

impl Drop for OdbcEnvInner {
    fn drop(&mut self) {
        // XXX: investigate errors deallocating the environment handle
        let _ = sql_free_handle(HandleType::Env, self.henv as Handle);
    }
}

/// Thin wrapper around an ODBC environment handle.
#[derive(Clone, Debug)]
pub struct OdbcEnv {
    inner: Arc<OdbcEnvInner>,
}

impl OdbcEnv {
    pub fn try_new() -> Result<Self> {
        let henv = sql_alloc_handle(HandleType::Env, 0 as Handle)? as HEnv;
        let result = {
            sql_set_env_attr(
                henv,
                EnvironmentAttribute::OdbcVersion,
                OptionValue::Int(AttrOdbcVersion::Odbc3 as i64),
            )?;
            let inner = OdbcEnvInner { henv };
            Ok(inner)
        };
        match result {
            Ok(inner) => Ok(Self {
                inner: Arc::new(inner),
            }),
            Err(e) => {
                sql_free_handle(HandleType::Env, henv as Handle).expect("free ODBC environment");
                Err(e)
            }
        }
    }

    pub fn new_connection(&self, connection_string: &str) -> Result<ManagedOdbcConnection> {
        // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlconnect-function?view=sql-server-ver16#code-example

        // allocate the connection handle with the env handle as parent
        let hdbc = sql_alloc_handle(HandleType::Dbc, self.inner.henv as Handle)? as HDbc;
        let result = {
            // TODO(felipecrv): make ODBC connection timeout configurable
            let timeout_s = 10;
            sql_set_conn_attr(hdbc, ConnectionAttribute::LoginTimeout, timeout_s.into())?;

            // connect to the database (will populate the allocated handle)
            match sql_driver_connect(hdbc, connection_string) {
                Ok(_) => Ok(()),
                Err(e) => Err(error_with_diagnostics(e, HandleType::Dbc, hdbc as Handle)),
            }
        };
        match result {
            Ok(()) => {
                let conn = ManagedOdbcConnection {
                    hdbc,
                    _parent: self.inner.clone(),
                };
                Ok(conn)
            }
            Err(e) => {
                sql_free_handle(HandleType::Dbc, hdbc as Handle).expect("free ODBC connection");
                Err(e)
            }
        }
    }
}

/// Thin wrapper around an ODBC connection handle.
#[derive(Debug)]
pub struct ManagedOdbcConnection {
    /// Always valid ODBC connection handle (invariant).
    #[allow(dead_code)]
    hdbc: HDbc,
    /// Connections are tied to an environment.
    _parent: Arc<OdbcEnvInner>,
}

unsafe impl Send for ManagedOdbcConnection {}
unsafe impl Sync for ManagedOdbcConnection {}

impl ManagedOdbcConnection {
    pub fn new_statement(this: Arc<ManagedOdbcConnection>) -> Result<ManagedOdbcStatement> {
        let hstmt = sql_alloc_handle(HandleType::Stmt, this.hdbc as Handle)? as HStmt;
        let stmt = ManagedOdbcStatement::new(hstmt, this);
        Ok(stmt)
    }

    pub fn cancel(&self) -> Result<()> {
        sql_cancel_handle(HandleType::Dbc, self.hdbc as Handle)
    }

    pub fn end_transaction(&self, completion_type: CompletionType) -> Result<()> {
        sql_end_tran(HandleType::Dbc, self.hdbc as Handle, completion_type)
    }
}

struct StmtState {
    /// Always valid ODBC statement handle (invariant).
    hstmt: HStmt,
    /// The SQL query string populated by set_sql_query().
    sql_query: Option<String>,
    /// Has prepare() been called?
    prepared: bool,
    /// Has execute() been called and returned data?
    has_data_after_execute: bool,
    /// There can be only one active cursor per statement.
    ///
    /// Any action that changes the state of the statement will increment this value.
    /// This effectively invalidates any cursor that was created before the change.
    ///
    /// When a cursor reads data from the statement, it has to first validate that the
    /// cursor is still active by comparing this value with the one it had when it
    /// was created.
    active_cursor: u64,
}

impl Drop for StmtState {
    fn drop(&mut self) {
        sql_free_handle(HandleType::Stmt, self.hstmt as Handle).expect("free ODBC statement");
    }
}

pub struct ManagedOdbcStatementInner {
    /// Mutex-protected statement state.
    state: Mutex<StmtState>,
    /// Statements are tied to a connection.
    _parent: Arc<ManagedOdbcConnection>,
}

impl ManagedOdbcStatementInner {
    const INITIAL_BATCH_CAPACITY: usize = 8;

    pub(self) fn lock_state(
        &self,
    ) -> std::result::Result<MutexGuard<'_, StmtState>, PoisonError<MutexGuard<'_, StmtState>>>
    {
        self.state.lock()
    }

    pub fn cancel(&self) -> Result<()> {
        let state = &mut *self.lock_state().unwrap();
        state.sql_query = None;
        state.prepared = false;
        state.has_data_after_execute = false;
        state.active_cursor += 1; // invalidate acrive cursor
        sql_cancel_handle(HandleType::Stmt, state.hstmt as Handle)
    }

    pub fn set_sql_query(&self, query: &str) -> Result<()> {
        let state = &mut *self.lock_state().unwrap();
        if state.prepared {
            return Err(Error::with_message_and_status(
                "cannot change query after preparing",
                Status::InvalidState,
            ));
        }
        state.sql_query = Some(query.to_string());
        state.prepared = false;
        state.has_data_after_execute = false;
        state.active_cursor += 1; // invalidate active cursor
        Ok(())
    }

    fn prepare_internal(&self, state: &mut MutexGuard<'_, StmtState>) -> Result<()> {
        match state.sql_query {
            Some(ref query) => {
                match sql_prepare(state.hstmt, query) {
                    Ok(_) => {}
                    Err(e) => {
                        let e = error_with_diagnostics(e, HandleType::Stmt, state.hstmt as Handle);
                        return Err(e);
                    }
                }
                state.prepared = true;
                state.has_data_after_execute = false;
                state.active_cursor += 1; // invalidate active cursor
                Ok(())
            }
            None => Err(Error::with_message_and_status(
                "no query to prepare",
                Status::InvalidState,
            )),
        }
    }

    pub fn prepare(&self) -> Result<()> {
        let mut state = self.lock_state().unwrap();
        self.prepare_internal(&mut state)
    }

    pub fn execute(&self) -> Result<()> {
        let mut state = self.lock_state().unwrap();
        // ADBC let's execute() be called after set_sql_query() without prepare(),
        // so we call ODBC prepare here if it's not been done yet.
        if !state.prepared {
            self.prepare_internal(&mut state)?;
        }
        sql_execute(state.hstmt)
            .map_err(|e| error_with_diagnostics(e, HandleType::Stmt, state.hstmt as Handle))
            .map(|v| {
                state.has_data_after_execute = v.is_some();
            })
    }

    pub fn batch_reader(self: &Arc<Self>) -> Result<impl RecordBatchReader + Send> {
        let stmt = self.clone();
        let (has_data, active_cursor) = {
            let state = stmt.lock_state().unwrap();
            (state.has_data_after_execute, state.active_cursor)
        };
        Cursor::try_new(stmt, has_data, active_cursor)
    }
}

pub struct ManagedOdbcStatement {
    inner: Arc<ManagedOdbcStatementInner>,
}

impl ManagedOdbcStatement {
    pub fn new(hstmt: HStmt, parent: Arc<ManagedOdbcConnection>) -> Self {
        let state = StmtState {
            hstmt,
            sql_query: None,
            prepared: false,
            has_data_after_execute: false,
            active_cursor: 0,
        };
        let inner = ManagedOdbcStatementInner {
            state: Mutex::new(state),
            _parent: parent,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn cancel(&self) -> Result<()> {
        self.inner.cancel()
    }

    pub fn set_sql_query(&self, query: &str) -> Result<()> {
        self.inner.set_sql_query(query)
    }

    pub fn prepare(&self) -> Result<()> {
        self.inner.prepare()
    }

    pub fn execute(&self) -> Result<()> {
        self.inner.execute()
    }

    pub fn batch_reader(&self) -> Result<impl RecordBatchReader + Send> {
        self.inner.batch_reader()
    }
}

unsafe impl Send for ManagedOdbcStatementInner {}
unsafe impl Sync for ManagedOdbcStatementInner {}

#[inline(never)]
fn to_arrow_error(hstmt: HStmt, error: Error) -> ArrowError {
    let error = error_with_diagnostics(error, HandleType::Stmt, hstmt as Handle);
    ArrowError::ExternalError(Box::new(error))
}

/// Implementation of an Arrow RecordBatchReader that reads data from an ODBC statement.
pub struct Cursor {
    stmt: Arc<ManagedOdbcStatementInner>,
    /// This value should match the active_cursor from the statement before any data is read.
    active_cursor: u64,
    schema: Arc<Schema>,
    suggested_batch_size: usize,
    /// The call to SqlExecute() DID NOT return SQL_NO_DATA.
    has_data: bool,
    /// If true, the first record batch has been fetched successfully.
    fetched_first: bool,
    /// If true, an error has been seen and the cursor is done.
    seen_error: bool,
}

impl RecordBatchReader for Cursor {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Iterator for Cursor {
    type Item = core::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_data || self.seen_error {
            return None;
        }
        match self.fetch_next_record_batch(self.suggested_batch_size) {
            Ok(Some(batch)) => {
                self.fetched_first = true;
                Some(Ok(batch))
            }
            Ok(None) => None,
            Err(e) => {
                self.seen_error = true;
                Some(Err(e))
            }
        }
    }
}

impl Cursor {
    pub fn try_new(
        stmt: Arc<ManagedOdbcStatementInner>,
        has_data: bool,
        active_cursor: u64,
    ) -> Result<Self> {
        let schema = {
            let state = stmt.lock_state().unwrap();
            OdbcRecordBatchBuilder::derive_arrow_schema(&state)
        }?;
        let cursor = Self {
            stmt,
            active_cursor,
            schema: Arc::new(schema),
            suggested_batch_size: 1024,
            has_data,
            fetched_first: false,
            seen_error: false,
        };
        Ok(cursor)
    }

    fn fetch_rowset(
        &self,
        state: &mut MutexGuard<'_, StmtState>,
        record_batch_builder: &mut OdbcRecordBatchBuilder,
        max_rowset_size: usize,
    ) -> core::result::Result<usize, ArrowError> {
        const INVALID_CURSOR_STATE: &str = "24000";
        let mut rowset_size = 0;
        loop {
            if rowset_size == max_rowset_size {
                break;
            }
            if sql_fetch(state.hstmt)
                .or_else(|e| {
                    let error = error_with_diagnostics(e, HandleType::Stmt, state.hstmt as Handle);
                    if !self.fetched_first
                        && str_from_sqlstate(&error.sqlstate) == INVALID_CURSOR_STATE
                    {
                        // The Databricks ODBC driver fails to return SQL_NO_DATA on SqlExecute()
                        // calls DDL statements that return no data. This may lead to the the
                        // creation of a batch reader that calls SqlFetch() on the statement to
                        // load the data. That will, in turn, fail with a generic error code, but
                        // SQLSTATE will say "invalid cursor state", so with some risk of not
                        // reporting all theoretically possible errors, we ignore this error
                        // and return None instead.
                        Ok(None)
                    } else {
                        let arrow_error = ArrowError::ExternalError(Box::new(error));
                        Err(arrow_error)
                    }
                })?
                .is_some()
            {
                record_batch_builder.append_row(state)?;
                rowset_size += 1;
            } else {
                break;
            }
        }
        Ok(rowset_size)
    }

    fn fetch_next_record_batch(
        &self,
        suggested_batch_size: usize,
    ) -> core::result::Result<Option<RecordBatch>, ArrowError> {
        debug_assert!(suggested_batch_size > 0);
        let mut state = self.stmt.lock_state().unwrap();

        // validate this cursor before starting to read data
        if self.active_cursor != state.active_cursor {
            let adbc_error =
                Error::with_message_and_status("cursor is no longer valid", Status::InvalidState);
            let arrow_error = ArrowError::ExternalError(Box::new(adbc_error));
            return Err(arrow_error);
        }

        let mut record_batch_builder = OdbcRecordBatchBuilder::with_capacity(
            self.schema.clone(),
            ManagedOdbcStatementInner::INITIAL_BATCH_CAPACITY,
        );
        if self.fetch_rowset(&mut state, &mut record_batch_builder, suggested_batch_size)? > 0 {
            let batch = record_batch_builder.finish()?;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}

const ODBC_DATA_TYPE: &str = "ODBC:data_type";

struct OdbcRecordBatchBuilder {
    schema: Arc<Schema>,
    column_builders: Vec<Box<dyn ColumnBuilder>>,
}

impl OdbcRecordBatchBuilder {
    pub fn with_capacity(schema: Arc<Schema>, capacity: usize) -> Self {
        let column_builders = schema
            .fields()
            .iter()
            .map(|field| Self::create_column_builder(field, capacity))
            .collect::<Vec<Box<dyn ColumnBuilder>>>();
        Self {
            schema,
            column_builders,
        }
    }

    /// Derive an Arrow schema from the ODBC statement.
    pub fn derive_arrow_schema(state: &MutexGuard<'_, StmtState>) -> Result<Schema> {
        let mut buffer = Vec::new();

        let num_cols = sql_num_result_cols(state.hstmt)?;
        let mut fields = Vec::with_capacity(num_cols as usize);

        for col_idx in 0..num_cols {
            let col = sql_describe_col(state.hstmt, (col_idx as u16) + 1, &mut buffer)?;
            let field = derive_arrow_field(state.hstmt, col);
            fields.push(field);
        }

        let schema = Schema::new(fields);
        Ok(schema)
    }

    /// Create a column builder for the given Arrow data type.
    fn create_column_builder(field: &Field, capacity: usize) -> Box<dyn ColumnBuilder> {
        match field.data_type() {
            // Boolean builder
            DataType::Boolean => Box::new(BooleanColumnBuilder::with_capacity(capacity)),
            // Primitive builders
            DataType::Int16 => {
                Box::new(PrimitiveColumnBuilder::<Int16Type>::with_capacity(capacity))
            }
            DataType::UInt16 => Box::new(PrimitiveColumnBuilder::<UInt16Type>::with_capacity(
                capacity,
            )),
            DataType::Int32 => {
                Box::new(PrimitiveColumnBuilder::<Int32Type>::with_capacity(capacity))
            }
            DataType::UInt32 => Box::new(PrimitiveColumnBuilder::<UInt32Type>::with_capacity(
                capacity,
            )),
            DataType::Int64 => {
                Box::new(PrimitiveColumnBuilder::<Int64Type>::with_capacity(capacity))
            }
            DataType::UInt64 => Box::new(PrimitiveColumnBuilder::<UInt64Type>::with_capacity(
                capacity,
            )),
            DataType::Float32 => Box::new(PrimitiveColumnBuilder::<Float32Type>::with_capacity(
                capacity,
            )),
            DataType::Float64 => Box::new(PrimitiveColumnBuilder::<Float64Type>::with_capacity(
                capacity,
            )),
            // Byte builders
            DataType::Utf8 => {
                let odbc_type = field.metadata().get(ODBC_DATA_TYPE).unwrap();
                let char_type = match odbc_type.as_str() {
                    "VARCHAR" => CDataType::Char,
                    "EXT_W_VARCHAR" => CDataType::WChar,
                    _ => unreachable!(),
                };
                let builder = StringColumnBuilder::with_capacity(char_type, capacity, capacity * 2);
                Box::new(builder)
            }
            DataType::Binary => {
                let char_type = CDataType::Binary;
                let builder = BinaryColumnBuilder::with_capacity(char_type, capacity, capacity * 2);
                Box::new(builder)
            }
            _ => todo!("create_column_builder: {:?}", field),
        }
    }

    /// Append a row to the column builders.
    ///
    /// Must be called after a successful [`SQLFetchScroll`] ODBC call.
    pub fn append_row(
        &mut self,
        state: &mut MutexGuard<'_, StmtState>,
    ) -> core::result::Result<(), ArrowError> {
        for i in 0..self.column_builders.len() {
            self.column_builders[i].append_from_column(state.hstmt, i as u16)?;
        }
        Ok(())
    }

    pub fn finish(self) -> core::result::Result<RecordBatch, ArrowError> {
        let column_arrays = self
            .column_builders
            .into_iter()
            .map(|mut builder| builder.finish())
            .collect::<Vec<Arc<dyn Array>>>();
        RecordBatch::try_new(self.schema.clone(), column_arrays)
    }
}

fn derive_arrow_field(hstmt: HStmt, col: ColumnDescription) -> Field {
    let nullable = col.nullable().unwrap_or(true);
    let mut metadata: HashMap<String, String> = HashMap::new();
    let data_type = match col.sql_data_type {
        SqlDataType::EXT_BIT => {
            metadata.insert(ODBC_DATA_TYPE.into(), "EXT_BIT".into());
            DataType::Boolean
        }
        SqlDataType::SMALLINT => {
            metadata.insert(ODBC_DATA_TYPE.into(), "SMALLINT".into());
            let is_unsigned = col.is_unsigned(hstmt).unwrap_or(false);
            if is_unsigned {
                DataType::UInt16
            } else {
                DataType::Int16
            }
        }
        SqlDataType::INTEGER => {
            metadata.insert(ODBC_DATA_TYPE.into(), "INTEGER".into());
            let is_unsigned = col.is_unsigned(hstmt).unwrap_or(false);
            if is_unsigned {
                DataType::UInt32
            } else {
                DataType::Int32
            }
        }
        SqlDataType::EXT_BIG_INT => {
            metadata.insert(ODBC_DATA_TYPE.into(), "EXT_BIG_INT".into());
            let is_unsigned = col.is_unsigned(hstmt).unwrap_or(false);
            if is_unsigned {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        }
        SqlDataType::REAL => {
            metadata.insert(ODBC_DATA_TYPE.into(), "REAL".into());
            DataType::Float32
        }
        SqlDataType::DOUBLE => {
            metadata.insert(ODBC_DATA_TYPE.into(), "DOUBLE".into());
            DataType::Float64
        }
        SqlDataType::VARCHAR => {
            metadata.insert(ODBC_DATA_TYPE.into(), "VARCHAR".into());
            DataType::Utf8
        }
        SqlDataType::EXT_W_VARCHAR => {
            metadata.insert(ODBC_DATA_TYPE.into(), "EXT_W_VARCHAR".into());
            DataType::Utf8
        }
        SqlDataType::EXT_VAR_BINARY => {
            metadata.insert(ODBC_DATA_TYPE.into(), "EXT_VAR_BINARY".into());
            DataType::Binary
        }
        _ => todo!("derive_arrow_data_type: {:?}", col.sql_data_type),
    };
    Field::new(col.name, data_type, nullable).with_metadata(metadata)
}

// Column builders ------------------------------------------------------------

trait ColumnBuilder {
    /// Append a value from the ODBC column to the Arrow column builder.
    ///
    /// The column index is 0-based.
    fn append_from_column(
        &mut self,
        hstmt: HStmt,
        col_idx: u16,
    ) -> core::result::Result<(), ArrowError>;

    /// Finish building the Arrow column.
    fn finish(&mut self) -> Arc<dyn Array>;
}

// Boolean column builders ----------------------------------------------------

struct BooleanColumnBuilder {
    builder: BooleanBuilder,
}

impl BooleanColumnBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: BooleanBuilder::with_capacity(capacity),
        }
    }
}

impl ColumnBuilder for BooleanColumnBuilder {
    fn append_from_column(
        &mut self,
        hstmt: HStmt,
        col_idx: u16,
    ) -> core::result::Result<(), ArrowError> {
        sql_get_data_primitive::<bool>(hstmt, col_idx + 1)
            .map_err(|e| to_arrow_error(hstmt, e))
            .map(|v| self.builder.append_option(v))
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }
}

// Primitive column builders --------------------------------------------------

struct PrimitiveColumnBuilder<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
}

impl<T: ArrowPrimitiveType> PrimitiveColumnBuilder<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity),
        }
    }
}

macro_rules! make_primitive_column_builder {
    ($arrow_data_ty:ty) => {
        impl ColumnBuilder for PrimitiveColumnBuilder<$arrow_data_ty> {
            fn append_from_column(
                &mut self,
                hstmt: HStmt,
                col_idx: u16,
            ) -> core::result::Result<(), ArrowError> {
                type Native = <$arrow_data_ty as ArrowPrimitiveType>::Native;
                sql_get_data_primitive::<Native>(hstmt, col_idx + 1)
                    .map_err(|e| to_arrow_error(hstmt, e))
                    .map(|v| self.builder.append_option(v))
            }

            fn finish(&mut self) -> Arc<dyn Array> {
                Arc::new(self.builder.finish())
            }
        }
    };
}

make_primitive_column_builder!(Int16Type);
make_primitive_column_builder!(UInt16Type);
make_primitive_column_builder!(Int32Type);
make_primitive_column_builder!(UInt32Type);
make_primitive_column_builder!(Int64Type);
make_primitive_column_builder!(UInt64Type);
make_primitive_column_builder!(Float32Type);
make_primitive_column_builder!(Float64Type);

// Byte column builders -------------------------------------------------------

struct GenericByteColumnBuilder<T: ByteArrayType> {
    /// WChar, Char, Binary, etc.
    c_data_type: CDataType,
    buffer: Vec<u8>,
    builder: GenericByteBuilder<T>,
}

impl<T: ByteArrayType> GenericByteColumnBuilder<T> {
    pub fn with_capacity(
        c_data_type: CDataType,
        item_capacity: usize,
        data_capacity: usize,
    ) -> Self {
        debug_assert!(
            c_data_type == CDataType::Char
                || c_data_type == CDataType::WChar
                || c_data_type == CDataType::Binary
        );
        Self {
            c_data_type,
            buffer: Vec::new(),
            builder: GenericByteBuilder::<T>::with_capacity(item_capacity, data_capacity),
        }
    }
}

impl<O: OffsetSizeTrait> ColumnBuilder for GenericByteColumnBuilder<GenericStringType<O>> {
    fn append_from_column(
        &mut self,
        hstmt: HStmt,
        col_idx: u16,
    ) -> core::result::Result<(), ArrowError> {
        // Redshift: Even when the driver says the column is EXT_W_VARCHAR, the data must be
        // retrieved as Char.
        let c_data_type = if self.c_data_type == CDataType::WChar {
            CDataType::Char
        } else {
            self.c_data_type
        };
        let is_valid_data = sql_get_data_bytes(hstmt, c_data_type, col_idx + 1, &mut self.buffer)
            .map_err(|e| to_arrow_error(hstmt, e))?
            .is_some();
        if is_valid_data {
            // XXX: there are many quirks related to string encoding in ODBC drivers.
            // Review this and fix it based on knowledge available in ODBC codebases.
            let s = String::from_utf8_lossy(&self.buffer);
            self.builder.append_value(s.as_ref());
        } else {
            self.builder.append_null();
        }
        Ok(())
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }
}

impl<O: OffsetSizeTrait> ColumnBuilder for GenericByteColumnBuilder<GenericBinaryType<O>> {
    fn append_from_column(
        &mut self,
        hstmt: HStmt,
        col_idx: u16,
    ) -> core::result::Result<(), ArrowError> {
        let is_valid_data =
            sql_get_data_bytes(hstmt, self.c_data_type, col_idx + 1, &mut self.buffer)
                .map_err(|e| to_arrow_error(hstmt, e))?
                .is_some();
        if is_valid_data {
            self.builder.append_value(self.buffer.as_slice());
        } else {
            self.builder.append_null();
        }
        Ok(())
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }
}

type GenericStringColumnBuilder<O> = GenericByteColumnBuilder<GenericStringType<O>>;
type StringColumnBuilder = GenericStringColumnBuilder<i32>;
// type LargeStringColumnBuilder = GenericStringColumnBuilder<i64>;

type GenericBinaryColumnBuilder<O> = GenericByteColumnBuilder<GenericBinaryType<O>>;
type BinaryColumnBuilder = GenericBinaryColumnBuilder<i32>;
// type LargeBinaryColumnBuilder = GenericBinaryColumnBuilder<i64>;
