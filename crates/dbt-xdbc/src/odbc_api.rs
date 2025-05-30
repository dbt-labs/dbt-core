//! Relatively safe (use with care) ODBC API for Rust
//!
//! This module provides a wrapper around the ODBC API for Rust. It is designed to be used by
//! the `odbc.rs` module, which provides a higher-level interface to the ODBC API that is
//! totally internal to the XDBC crate.

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use odbc_sys::*;
use std::ffi::c_char;

/// Convert an ODBC return code to a [`Result`] from `adbc_core`.
///
/// This is the only place where we convert ODBC return codes to `Result` and
/// the only dependency on `adbc_core` for this module.
pub fn retcode_to_result(retcode: SqlReturn) -> Result<()> {
    match retcode {
        SqlReturn::SUCCESS => Ok(()),
        SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        SqlReturn::ERROR => Err(Error::with_message_and_status(
            "ODBC error",
            Status::Internal,
        )),
        SqlReturn::INVALID_HANDLE => Err(Error::with_message_and_status(
            "ODBC error: INVALID_HANDLE",
            Status::InvalidArguments,
        )),
        SqlReturn::STILL_EXECUTING => Err(Error::with_message_and_status(
            "ODBC error: STILL_EXECUTING",
            Status::Internal,
        )),
        SqlReturn::NEED_DATA => Err(Error::with_message_and_status(
            "ODBC error: NEED_DATA",
            Status::Internal,
        )),
        SqlReturn::NO_DATA => Err(Error::with_message_and_status(
            "ODBC error: NO_DATA",
            Status::NotFound,
        )),
        SqlReturn(code) => Err(Error::with_message_and_status(
            format!("ODBC error: {}", code),
            Status::Internal,
        )),
    }
}

pub fn sql_alloc_handle(handle_type: HandleType, input_handle: Handle) -> Result<Handle> {
    let mut handle: Handle = std::ptr::null_mut();
    let retcode = unsafe { SQLAllocHandle(handle_type, input_handle, &mut handle) };
    retcode_to_result(retcode).map(|_| handle)
}

pub fn sql_set_env_attr(henv: HEnv, attr: EnvironmentAttribute, value: OptionValue) -> Result<()> {
    let retcode = {
        match value {
            OptionValue::Int(i) => {
                let i = i as Integer;
                unsafe { SQLSetEnvAttr(henv, attr, i as Pointer, 0) }
            }
            OptionValue::String(s) => {
                // XXX: think about encoding?
                // XXX: support only OptionValue::Bytes instead?
                let ptr = s.as_bytes().as_ptr() as Pointer;
                let string_length = s.len() as Integer;
                unsafe { SQLSetEnvAttr(henv, attr, ptr, string_length) }
            }
            _ => SqlReturn::ERROR,
        }
    };
    retcode_to_result(retcode)
}

pub fn sql_set_conn_attr(hdbc: HDbc, attr: ConnectionAttribute, value: OptionValue) -> Result<()> {
    let retcode = {
        match value {
            OptionValue::Int(i) => {
                let i = i as Integer;
                unsafe { SQLSetConnectAttr(hdbc, attr, i as Pointer, 0) }
            }
            OptionValue::String(s) => {
                let ptr = s.as_bytes().as_ptr() as Pointer;
                let string_length = s.len() as Integer;
                unsafe { SQLSetConnectAttr(hdbc, attr, ptr, string_length) }
            }
            _ => SqlReturn::ERROR,
        }
    };
    retcode_to_result(retcode)
}

pub fn sql_free_handle(handle_type: HandleType, handle: Handle) -> Result<()> {
    let retcode = unsafe { SQLFreeHandle(handle_type, handle) };
    retcode_to_result(retcode)
}

/// SQLDriverConnectW() wrapper.
pub fn sql_driver_connect(connection_handle: HDbc, connection_string: &str) -> Result<()> {
    let connection_wstring: Vec<u16> = connection_string.encode_utf16().collect();
    // Possible return values:
    //
    // Success:
    // - SQL_SUCCESS
    // - SQL_SUCCESS_WITH_INFO
    //
    // Failure:
    // - SQL_ERROR
    // - SQL_INVALID_HANDLE
    // = SQL_NO_DATA
    // - SQL_STILL_EXECUTING
    let retcode = unsafe {
        SQLDriverConnectW(
            connection_handle,
            std::ptr::null_mut(), // window_handle
            connection_wstring.as_ptr(),
            connection_wstring.len() as SmallInt,
            std::ptr::null_mut(), // out_conn_str
            0,                    // out_conn_str_len
            std::ptr::null_mut(), // out_conn_str_len_ptr
            DriverConnectOption::NoPrompt,
        )
    };
    retcode_to_result(retcode)
}

/// ODBC diagnostic information in a way that is easier to convert to an ADBC error.
pub struct Diagnostic {
    /// A SQLSTATE error code, if provided, as defined by the SQL:2003 standard.
    /// If not set, it should be set to `\0\0\0\0\0`.
    pub sqlstate: [c_char; 5],
    /// A human-readable error message.
    pub message: String,
    /// Additional metadata. Introduced in ADBC 1.1.0.
    pub details: Option<Vec<(String, Vec<u8>)>>,
}

impl From<Diagnostic> for Error {
    fn from(diagnostic: Diagnostic) -> Self {
        Error {
            message: diagnostic.message,
            status: Status::Internal,
            vendor_code: 0,
            sqlstate: diagnostic.sqlstate,
            details: diagnostic.details,
        }
    }
}

/// SQLGetDiagRec() wrapper.
pub fn sql_get_diag_rec(
    handle_type: HandleType,
    handle: Handle,
    record_number: SmallInt, // HeaderDiagnosticIdentifier
) -> Result<Option<Diagnostic>> {
    // buffer for SQL State
    let state_buffer = [b'\0'; 6];
    let state: *mut Char = state_buffer.as_ptr() as *mut Char;

    // buffer for record_number=SQL_DIAG_NATIVE
    let native_error_ptr: *mut Integer = std::ptr::null_mut();

    // small stack buffer for initial SQLGetDiagRec() call
    let buffer = [0u8; 64];
    let message_text: *mut Char = buffer.as_ptr() as *mut Char;
    let mut text_length: SmallInt = 0;
    let text_length_ptr: *mut SmallInt = &mut text_length;

    let retcode = unsafe {
        // XXX: consider using SQLGetDiagRecW as well
        SQLGetDiagRec(
            handle_type,
            handle,
            record_number as SmallInt,
            state,
            native_error_ptr,
            message_text,
            buffer.len() as SmallInt,
            text_length_ptr,
        )
    };
    if retcode == SqlReturn::NO_DATA {
        return Ok(None);
    }
    retcode_to_result(retcode)?;
    let sqlstate: [i8; 5] = unsafe { *(&state_buffer as *const _ as *const [i8; 5]) };

    let message = {
        if text_length < buffer.len() as SmallInt {
            String::from_utf8_lossy(&buffer[0..text_length as usize]).to_string()
        } else {
            // XXX: refactor this function to re-use a buffer across calls
            let mut heap_buffer = vec![0; text_length as usize + 1];
            let retcode = unsafe {
                SQLGetDiagRec(
                    handle_type,
                    handle,
                    record_number as SmallInt,
                    state,
                    native_error_ptr,
                    heap_buffer.as_mut_ptr() as *mut Char,
                    heap_buffer.len() as SmallInt,
                    text_length_ptr,
                )
            };
            retcode_to_result(retcode)?;
            debug_assert!(text_length >= 0);
            String::from_utf8_lossy(&heap_buffer[..text_length as usize]).to_string()
        }
    };

    let diagnostic = Diagnostic {
        sqlstate,
        message,
        details: None, // TODO(felipecrv): implement details
    };
    Ok(Some(diagnostic))
}

pub fn sql_cancel_handle(handle_type: HandleType, handle: Handle) -> Result<()> {
    let retcode = unsafe { SQLCancelHandle(handle_type, handle) };
    retcode_to_result(retcode)
}

pub fn sql_end_tran(
    handle_type: HandleType,
    handle: Handle,
    completion_type: CompletionType,
) -> Result<()> {
    let retcode = unsafe { SQLEndTran(handle_type, handle, completion_type) };
    retcode_to_result(retcode)
}

#[allow(dead_code)]
pub fn sql_get_stmt_attr_int(hstmt: HStmt, attr: StatementAttribute) -> Result<Integer> {
    let mut value: Integer = 0;
    let retcode = unsafe {
        SQLGetStmtAttr(
            hstmt,
            attr,
            &mut value as *mut Integer as Pointer,
            0,
            std::ptr::null_mut(),
        )
    };
    retcode_to_result(retcode).map(|_| value)
}

#[allow(dead_code)]
pub fn sql_set_stmt_attr_int(hstmt: HStmt, attr: StatementAttribute, value: Integer) -> Result<()> {
    let retcode = unsafe {
        SQLGetStmtAttr(
            hstmt,
            attr,
            &value as *const Integer as Pointer,
            0,
            std::ptr::null_mut(),
        )
    };
    retcode_to_result(retcode)
}

pub fn sql_prepare(hstmt: HStmt, query: &str) -> Result<()> {
    let query_wstring: Vec<u16> = query.encode_utf16().collect();
    let retcode = unsafe {
        SQLPrepareW(
            hstmt,
            query_wstring.as_ptr(),
            query_wstring.len() as Integer,
        )
    };
    retcode_to_result(retcode)
}

pub fn sql_execute(hstmt: HStmt) -> Result<Option<()>> {
    let retcode = unsafe { SQLExecute(hstmt) };
    if retcode == SqlReturn::NO_DATA {
        Ok(None)
    } else {
        retcode_to_result(retcode)?;
        Ok(Some(()))
    }
}

pub fn sql_num_result_cols(hstmt: HStmt) -> Result<SmallInt> {
    let mut num_cols: SmallInt = 0;
    let retcode = unsafe { SQLNumResultCols(hstmt, &mut num_cols as *mut SmallInt) };
    retcode_to_result(retcode)?;
    Ok(num_cols)
}

pub fn sql_fetch(hstm: HStmt) -> Result<Option<()>> {
    let retcode = unsafe { SQLFetch(hstm) };
    if retcode == SqlReturn::NO_DATA {
        Ok(None)
    } else {
        retcode_to_result(retcode)?;
        Ok(Some(()))
    }
}

pub struct ColumnDescription {
    // 1-based column number.
    pub number: USmallInt,
    /// The column name.
    pub name: String,
    /// The concise SQL data type (SQL_DESC_CONCISE_TYPE).
    pub sql_data_type: SqlDataType,

    // https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size-decimal-digits-transfer-octet-length-and-display-size?view=sql-server-ver16
    #[allow(dead_code)]
    pub size: ULen,
    #[allow(dead_code)]
    pub decimal_digits: SmallInt,

    /// Can be UNKNOWN, NULLABLE, or NO_NULLS.
    pub nullability: Nullability,
}

pub fn sql_get_col_attr_int(
    hstmt: HStmt,
    col_number: USmallInt, // 1-based
    field: Desc,
) -> Result<Len> {
    let mut value: Len = 0;
    let retcode = unsafe {
        SQLColAttribute(
            hstmt,
            col_number,
            field,
            std::ptr::null_mut() as Pointer,
            0,
            std::ptr::null_mut() as *mut SmallInt,
            &mut value as *mut Len,
        )
    };
    retcode_to_result(retcode)?;
    Ok(value)
}

impl ColumnDescription {
    pub fn nullable(&self) -> Option<bool> {
        if self.nullability == Nullability::NULLABLE {
            return Some(true);
        }
        if self.nullability == Nullability::NO_NULLS {
            return Some(false);
        }
        debug_assert!(self.nullability == Nullability::UNKNOWN);
        None
    }

    pub fn is_unsigned(&self, hstmt: HStmt) -> Result<bool> {
        let is_unsigned = sql_get_col_attr_int(hstmt, self.number, Desc::Unsigned)?;
        Ok(is_unsigned != 0)
    }
}

pub fn sql_describe_col(
    hstmt: HStmt,
    col_number: USmallInt,
    col_name_buffer: &mut Vec<u16>,
) -> Result<ColumnDescription> {
    col_name_buffer.resize(col_name_buffer.capacity().max(16), 0);
    let mut col_name_len: SmallInt = 0;

    // In ODBC 3.x, SQL_TYPE_DATE, SQL_TYPE_TIME, or SQL_TYPE_TIMESTAMP is returned in
    // *DataTypePtr for date, time, or timestamp data, respectively; in ODBC 2.x,
    // SQL_DATE, SQL_TIME, or SQL_TIMESTAMP is returned.
    let mut sql_data_type: SqlDataType = SqlDataType::UNKNOWN_TYPE;

    // If the column size cannot be determined, the driver returns 0.
    let mut col_size: ULen = 0;
    let mut decimal_digits: SmallInt = 0;

    let mut nullability = Nullability::UNKNOWN;

    let retcode = unsafe {
        SQLDescribeColW(
            hstmt,
            col_number,
            col_name_buffer.as_mut_ptr() as *mut WChar,
            col_name_buffer.len() as SmallInt,
            &mut col_name_len as *mut SmallInt,
            &mut sql_data_type as *mut SqlDataType,
            &mut col_size as *mut ULen,
            &mut decimal_digits as *mut SmallInt,
            &mut nullability,
        )
    };
    if retcode == SqlReturn::SUCCESS_WITH_INFO {
        if col_name_len < 0 {
            panic!(
                "SQLDescribeColW() returned SUCCESS_WITH_INFO, but col_name_len < 0: {}",
                col_name_len
            );
        }
        if (col_name_len as usize) + 1 > col_name_buffer.len() {
            // The buffer was too small. Resize it and try again.
            col_name_buffer.resize(col_name_len as usize + 1, 0);
            return sql_describe_col(hstmt, col_number, col_name_buffer);
        }
    }
    retcode_to_result(retcode)?;

    let col_name = String::from_utf16_lossy(&col_name_buffer[0..col_name_len as usize]);

    let description = ColumnDescription {
        number: col_number,
        name: col_name,
        sql_data_type,
        size: col_size,
        decimal_digits,
        nullability,
    };
    Ok(description)
}

pub trait PrimitiveType: Sized + Copy + Default {
    type Type;
    const C_DATA_TYPE: CDataType;
}

impl PrimitiveType for bool {
    type Type = bool;
    const C_DATA_TYPE: CDataType = CDataType::Bit;
}

impl PrimitiveType for i16 {
    type Type = i16;
    const C_DATA_TYPE: CDataType = CDataType::SShort;
}

impl PrimitiveType for u16 {
    type Type = u16;
    const C_DATA_TYPE: CDataType = CDataType::UShort;
}

impl PrimitiveType for i32 {
    type Type = i32;
    const C_DATA_TYPE: CDataType = CDataType::SLong;
}

impl PrimitiveType for u32 {
    type Type = u32;
    const C_DATA_TYPE: CDataType = CDataType::ULong;
}

impl PrimitiveType for i64 {
    type Type = i64;
    const C_DATA_TYPE: CDataType = CDataType::SBigInt;
}

impl PrimitiveType for u64 {
    type Type = u64;
    const C_DATA_TYPE: CDataType = CDataType::UBigInt;
}

impl PrimitiveType for f32 {
    type Type = f32;
    const C_DATA_TYPE: CDataType = CDataType::Float;
}

impl PrimitiveType for f64 {
    type Type = f64;
    const C_DATA_TYPE: CDataType = CDataType::Double;
}

#[allow(dead_code)]
pub fn sql_get_data_primitive<P: PrimitiveType<Type = P>>(
    hstmt: HStmt,
    col_number: USmallInt, // 1-based
) -> Result<Option<P::Type>> {
    let mut indicator: Len = 0;
    let mut value: P::Type = Default::default();
    let retcode = unsafe {
        SQLGetData(
            hstmt,
            col_number,
            P::C_DATA_TYPE,
            &mut value as *mut P as Pointer,
            0,
            &mut indicator as *mut Len,
        )
    };
    retcode_to_result(retcode)?;
    if indicator < 0 {
        // the other possible value is NO_TOTAL which only applies to variable-length types
        debug_assert!(indicator == NULL_DATA);
        return Ok(None);
    }
    Ok(Some(value))
}

pub fn sql_get_data_bytes(
    hstmt: HStmt,
    target_type: CDataType,
    col_number: USmallInt, // 1-based
    out: &mut Vec<u8>,
) -> Result<Option<()>> {
    debug_assert!(
        target_type == CDataType::Binary
            || target_type == CDataType::Char
            || target_type == CDataType::WChar,
        "target_type to sql_get_data_bytes() must be a variable-length type"
    );
    let mut text_len_or_ind: Len = 0; // also used as the NULL "indicator"
    let text_len_or_ind_ptr: *mut Len = &mut text_len_or_ind;

    // First SQLGetData() call with at least 16 bytes of space.
    out.resize(out.len().max(16), 0);
    let retcode = unsafe {
        SQLGetData(
            hstmt,
            col_number,
            target_type,
            out.as_mut_ptr() as Pointer,
            out.len() as Len,
            text_len_or_ind_ptr as *mut Len,
        )
    };
    retcode_to_result(retcode)?;
    if text_len_or_ind >= 0 {
        // The data fits in the buffer. Adjust it and return immediately.
        out.resize(text_len_or_ind as usize, 0);
        return Ok(Some(()));
    }
    if text_len_or_ind == NULL_DATA {
        out.clear(); // Value is NULL. Avoid buffer misuse.
        return Ok(None);
    }

    // When SQL_NO_TOTAL is returned, we have to call SQLGetData() many times
    // until all the data for the byte string is fetched.
    if text_len_or_ind == NO_TOTAL {
        let grow_buffer = |out: &mut Vec<u8>| {
            // increase output size to read the next block of data:
            // either double the size or add 64KiB, whichever is smaller
            let new_size = out.len() + out.len().min(1usize << 16);
            out.resize(new_size, 0);
            new_size
        };

        let mut total_bytes_read = out.len() - 1; // don't count the trailing \0
        grow_buffer(out); // establish the loop invariant: block_size >= 2
        loop {
            let block_size = out.len() - total_bytes_read;
            debug_assert!(block_size >= 2, "loop invariant: block_size >= 2");
            let retcode = unsafe {
                SQLGetData(
                    hstmt,
                    col_number,
                    target_type,
                    out.as_mut_ptr().add(total_bytes_read) as Pointer,
                    block_size as Len,
                    text_len_or_ind_ptr as *mut Len,
                )
            };
            if retcode == SqlReturn::NO_DATA {
                break;
            }
            retcode_to_result(retcode)?;
            if text_len_or_ind >= 0 {
                // The returned length can be higher than the block size! Very confusing,
                // but it let's us know exactly how much space to allocate (if any) for the
                // final block.
                if (text_len_or_ind as usize) < block_size {
                    total_bytes_read += text_len_or_ind as usize;
                    break;
                } else {
                    let new_size = total_bytes_read + text_len_or_ind as usize + 1;
                    out.resize(new_size, 0);
                    total_bytes_read += block_size - 1; // don't count the trailing \0

                    // The loop-invariant (block_size' >= 2) is preserved. Proof:
                    //
                    // Let total_bytes_read be the previous value of total_bytes_read.
                    // And total_bytes_read' be `total_bytes_read + block_size - 1`.
                    //
                    //   block_size' == new_size - total_bytes_read'             (by def.)
                    //               == (total_bytes_read + text_len_or_ind + 1)
                    //                  - (total_bytes_read + block_size - 1)    (by def.)
                    //               == text_len_or_ind - block_size + 2         (by arith.)
                    //
                    // Subtracting (block_size - 2) from both sides of the else-assumption:
                    //
                    //   text_len_or_ind - (block_size - 2) >= block_size - (block_size - 2)
                    //   text_len_or_ind - block_size + 2 >= 2                   (by arith.)
                    //   block_size' >= 2                                        (by def. of block_size')
                }
            } else if text_len_or_ind == NO_TOTAL {
                total_bytes_read += block_size - 1; // don't count the trailing \0
                grow_buffer(out); // preserve the loop invariant
            } else {
                panic!(
                    "after the first SQL_NO_TOTAL, SQLGetData should produce a \
positive text_len value or the SQL_NO_TOTAL indicator, but it produced {}",
                    text_len_or_ind
                );
            }
        }
        out.resize(total_bytes_read, 0);
        Ok(Some(()))
    } else {
        panic!(
            "expected SQL_NO_TOTAL, but SQLGetData() produced {}",
            text_len_or_ind
        );
    }
}
