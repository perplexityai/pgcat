/// Helper functions to send one-off protocol messages
/// and handle TcpStream (TCP socket).
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{debug, error};
use md5::{Digest, Md5};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::client::PREPARED_STATEMENT_COUNTER;
use crate::config::get_config;
use crate::errors::Error;

use crate::constants::MESSAGE_TERMINATOR;
use crate::plugins::StatementPlan;
use crate::server::PGCAT_APPLICATION_NAME;
use crate::sql_observer::SharedSnap;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, Cursor};
use std::mem;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Postgres data type mappings
/// used in RowDescription ('T') message.
pub enum DataType {
    Text,
    Int4,
    Numeric,
    Bool,
    Oid,
    AnyArray,
    Any,
}

impl From<&DataType> for i32 {
    fn from(data_type: &DataType) -> i32 {
        match data_type {
            DataType::Text => 25,
            DataType::Int4 => 23,
            DataType::Numeric => 1700,
            DataType::Bool => 16,
            DataType::Oid => 26,
            DataType::AnyArray => 2277,
            DataType::Any => 2276,
        }
    }
}

/// Tell the client that authentication handshake completed successfully.
pub async fn auth_ok<S>(stream: &mut S) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let mut auth_ok = BytesMut::with_capacity(9);

    auth_ok.put_u8(b'R');
    auth_ok.put_i32(8);
    auth_ok.put_i32(0);

    write_all(stream, auth_ok).await
}

/// Generate md5 password challenge.
pub async fn md5_challenge<S>(stream: &mut S) -> Result<[u8; 4], Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    // let mut rng = rand::thread_rng();
    let salt: [u8; 4] = [
        rand::random(),
        rand::random(),
        rand::random(),
        rand::random(),
    ];

    let mut res = BytesMut::new();
    res.put_u8(b'R');
    res.put_i32(12);
    res.put_i32(5); // MD5
    res.put_slice(&salt[..]);

    write_all(stream, res).await?;
    Ok(salt)
}

/// Give the client the process_id and secret we generated
/// used in query cancellation.
pub async fn backend_key_data<S>(
    stream: &mut S,
    backend_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let mut key_data = BytesMut::from(&b"K"[..]);
    key_data.put_i32(12);
    key_data.put_i32(backend_id);
    key_data.put_i32(secret_key);

    write_all(stream, key_data).await
}

/// Construct a `Q`: Query message.
pub fn simple_query(query: &str) -> BytesMut {
    let mut res = BytesMut::from(&b"Q"[..]);
    let query = format!("{}\0", query);

    res.put_i32(query.len() as i32 + 4);
    res.put_slice(query.as_bytes());

    res
}

/// Tell the client we're ready for another query.
pub async fn send_ready_for_query<S>(stream: &mut S) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    write_all(stream, ready_for_query(false)).await
}

/// Send the startup packet the server. We're pretending we're a Pg client.
/// This tells the server which user we are and what database we want.
pub async fn startup<S>(stream: &mut S, user: &str, database: &str) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let mut bytes = BytesMut::with_capacity(25);

    bytes.put_i32(196608); // Protocol number

    // User
    bytes.put(&b"user\0"[..]);
    bytes.put_slice(user.as_bytes());
    bytes.put_u8(0);

    // Application name
    bytes.put(&b"application_name\0"[..]);
    bytes.put_slice(PGCAT_APPLICATION_NAME.as_bytes());
    bytes.put_u8(0);

    // Database
    bytes.put(&b"database\0"[..]);
    bytes.put_slice(database.as_bytes());
    bytes.put_u8(0);
    bytes.put_u8(0); // Null terminator

    let len = bytes.len() as i32 + 4i32;

    let mut startup = BytesMut::with_capacity(len as usize);

    startup.put_i32(len);
    startup.put(bytes);

    match stream.write_all(&startup).await {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::SocketError(format!(
            "Error writing startup to server socket - Error: {:?}",
            err
        ))),
    }
}

pub async fn ssl_request(stream: &mut TcpStream) -> Result<(), Error> {
    let mut bytes = BytesMut::with_capacity(12);

    bytes.put_i32(8);
    bytes.put_i32(80877103);

    match stream.write_all(&bytes).await {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::SocketError(format!(
            "Error writing SSLRequest to server socket - Error: {:?}",
            err
        ))),
    }
}

/// Parse the params the server sends as a key/value format.
pub fn parse_params(mut bytes: BytesMut) -> Result<HashMap<String, String>, Error> {
    let mut result = HashMap::new();
    let mut buf = Vec::new();
    let mut tmp = String::new();

    while bytes.has_remaining() {
        let mut c = bytes.get_u8();

        // Null-terminated C-strings.
        while c != 0 {
            tmp.push(c as char);
            c = bytes.get_u8();
        }

        if !tmp.is_empty() {
            buf.push(tmp.clone());
            tmp.clear();
        }
    }

    // Expect pairs of name and value
    // and at least one pair to be present.
    if buf.len() % 2 != 0 || buf.len() < 2 {
        return Err(Error::ClientBadStartup);
    }

    let mut i = 0;
    while i < buf.len() {
        let name = buf[i].clone();
        let value = buf[i + 1].clone();
        let _ = result.insert(name, value);
        i += 2;
    }

    Ok(result)
}

/// Parse StartupMessage parameters.
/// e.g. user, database, application_name, etc.
pub fn parse_startup(bytes: BytesMut) -> Result<HashMap<String, String>, Error> {
    let result = parse_params(bytes)?;

    // Minimum required parameters
    // I want to have the user at the very minimum, according to the protocol spec.
    if !result.contains_key("user") {
        return Err(Error::ClientBadStartup);
    }

    Ok(result)
}

/// Create md5 password hash given a salt.
pub fn md5_hash_password(user: &str, password: &str, salt: &[u8]) -> Vec<u8> {
    let mut md5 = Md5::new();

    // First pass
    md5.update(password.as_bytes());
    md5.update(user.as_bytes());

    let output = md5.finalize_reset();

    // Second pass
    md5_hash_second_pass(&(format!("{:x}", output)), salt)
}

pub fn md5_hash_second_pass(hash: &str, salt: &[u8]) -> Vec<u8> {
    let mut md5 = Md5::new();
    // Second pass
    md5.update(hash);
    md5.update(salt);

    let mut password = format!("md5{:x}", md5.finalize())
        .chars()
        .map(|x| x as u8)
        .collect::<Vec<u8>>();
    password.push(0);

    password
}

/// Send password challenge response to the server.
/// This is the MD5 challenge.
pub async fn md5_password<S>(
    stream: &mut S,
    user: &str,
    password: &str,
    salt: &[u8],
) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let password = md5_hash_password(user, password, salt);

    let mut message = BytesMut::with_capacity(password.len() + 5);

    message.put_u8(b'p');
    message.put_i32(password.len() as i32 + 4);
    message.put_slice(&password[..]);

    write_all(stream, message).await
}

pub async fn md5_password_with_hash<S>(stream: &mut S, hash: &str, salt: &[u8]) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let password = md5_hash_second_pass(hash, salt);
    let mut message = BytesMut::with_capacity(password.len() + 5);

    message.put_u8(b'p');
    message.put_i32(password.len() as i32 + 4);
    message.put_slice(&password[..]);

    write_all(stream, message).await
}

/// Implements a response to our custom `SET SHARDING KEY`
/// and `SET SERVER ROLE` commands.
/// This tells the client we're ready for the next query.
pub async fn custom_protocol_response_ok<S>(stream: &mut S, message: &str) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let mut res = BytesMut::with_capacity(25);

    let set_complete = BytesMut::from(&format!("{}\0", message)[..]);
    let len = (set_complete.len() + 4) as i32;

    // CommandComplete
    res.put_u8(b'C');
    res.put_i32(len);
    res.put_slice(&set_complete[..]);

    write_all_half(stream, &res).await?;
    send_ready_for_query(stream).await
}

/// Send a custom error message to the client.
/// Tell the client we are ready for the next query and no rollback is necessary.
/// Docs on error codes: <https://www.postgresql.org/docs/12/errcodes-appendix.html>.
pub async fn error_response<S>(stream: &mut S, message: &str) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    error_response_terminal(stream, message).await?;
    send_ready_for_query(stream).await
}

/// Send a custom error message to the client, followed by a ReadyForQuery
/// with the transaction status (`I`, `T` or `E`).
pub async fn error_response_with_status<S>(
    stream: &mut S,
    message: &str,
    status: u8,
) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    error_response_terminal(stream, message).await?;
    write_all(stream, ready_for_query_status(status)).await
}

/// Send a custom error message to the client.
/// Tell the client we are ready for the next query and no rollback is necessary.
/// Docs on error codes: <https://www.postgresql.org/docs/12/errcodes-appendix.html>.
pub async fn error_response_terminal<S>(stream: &mut S, message: &str) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    write_all_half(stream, &error_message(message)).await
}

/// Give an error message a SQLSTATE. The client gets it at ERROR severity, and
/// its connection stays usable.
pub fn with_sqlstate(code: &str, message: &str) -> String {
    format!("\0{}\0{}", code, message)
}

/// The SQLSTATE a message was given with `with_sqlstate`, and the message.
pub fn sqlstate_of(message: &str) -> (Option<&str>, &str) {
    match message
        .strip_prefix('\0')
        .and_then(|rest| rest.split_once('\0'))
    {
        Some((code, message)) if code.len() == 5 => (Some(code), message),
        _ => (None, message),
    }
}

/// `message`, or `message` with the SQLSTATE `code` if it doesn't have one.
pub fn or_sqlstate(code: &str, message: &str) -> String {
    match sqlstate_of(message) {
        (Some(_), _) => message.to_string(),
        (None, message) => with_sqlstate(code, message),
    }
}

/// An ErrorResponse message, as sent by `error_response_terminal`.
pub fn error_message(message: &str) -> BytesMut {
    match sqlstate_of(message) {
        (Some(code), message) => error_fields("ERROR", code, message),
        // Not sure how much this matters: system_error, see Appendix A.
        (None, message) => error_fields("FATAL", "58000", message),
    }
}

/// pgcat is shutting down and closed the client connection before forwarding
/// anything the client sent after its last ReadyForQuery. Nothing sent on the
/// connection since then ran, so the client can send it again on a new
/// connection. Sent FATAL to clients closed at shutdown, and to clients that
/// connect while pgcat accepts only admin connections. Class PS is pgcat's
/// own: Postgres defines no class starting with P but P0 (PL/pgSQL).
pub const SHUTTING_DOWN: &str = "PS001";
/// The server connection failed after pgcat forwarded the client's request,
/// so the request may have run: connection_failure. Sent FATAL. Also sent
/// FATAL, instead of `SHUTTING_DOWN`, to a client closed at shutdown after
/// pgcat forwarded something it sent since its last ReadyForQuery, or a
/// plugin hook was cut off while it queried the server for it.
pub const SERVER_CONNECTION_LOST: &str = "08006";
/// pgcat got no server connection for the request in time, so it sent the
/// server nothing: too_many_connections, Postgres's answer when it has no
/// backend slot. Sent ERROR when the client can go on, and FATAL when
/// `checkout_failure_limit` closes the connection.
pub const NO_SERVER_CONNECTION: &str = "53300";

/// A FATAL ErrorResponse with SQLSTATE `code`. pgcat closes the client
/// connection after sending it.
pub fn fatal_message(code: &str, message: &str) -> BytesMut {
    error_fields("FATAL", code, message)
}

/// Tell the client why pgcat closes its connection, with SQLSTATE `code`.
/// No ReadyForQuery follows.
pub async fn fatal_response<S>(stream: &mut S, code: &str, message: &str) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    write_all_half(stream, &fatal_message(code, message)).await
}

/// An ErrorResponse with `severity`, SQLSTATE `code`, and `message`.
fn error_fields(severity: &str, code: &str, message: &str) -> BytesMut {
    let message = message.replace('\0', " ");

    let mut error = BytesMut::new();

    // Error level
    error.put_u8(b'S');
    error.put_slice(format!("{}\0", severity).as_bytes());

    // Error level (non-translatable)
    error.put_u8(b'V');
    error.put_slice(format!("{}\0", severity).as_bytes());

    // Error code
    error.put_u8(b'C');
    error.put_slice(format!("{}\0", code).as_bytes());

    // The short error message.
    error.put_u8(b'M');
    error.put_slice(format!("{}\0", message).as_bytes());

    // No more fields follow.
    error.put_u8(0);

    // Compose the two message reply.
    let mut res = BytesMut::with_capacity(error.len() + 5);

    res.put_u8(b'E');
    res.put_i32(error.len() as i32 + 4);
    res.put(error);

    res
}

/// Tell the client its credentials were rejected (FATAL 28P01). Only the user name
/// goes into the message.
pub async fn authentication_failed<S>(stream: &mut S, user: &str) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let mut error = BytesMut::new();

    // Error level
    error.put_u8(b'S');
    error.put_slice(&b"FATAL\0"[..]);

    // Error level (non-translatable)
    error.put_u8(b'V');
    error.put_slice(&b"FATAL\0"[..]);

    // Error code: not sure how much this matters.
    error.put_u8(b'C');
    error.put_slice(&b"28P01\0"[..]); // system_error, see Appendix A.

    // The short error message.
    error.put_u8(b'M');
    error.put_slice(format!("password authentication failed for user \"{}\"\0", user).as_bytes());

    // No more fields follow.
    error.put_u8(0);

    // Compose the two message reply.
    let mut res = BytesMut::new();

    res.put_u8(b'E');
    res.put_i32(error.len() as i32 + 4);

    res.put(error);

    write_all(stream, res).await
}

/// Respond to a SHOW SHARD command.
pub async fn show_response<S>(stream: &mut S, name: &str, value: &str) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    // A SELECT response consists of:
    // 1. RowDescription
    // 2. One or more DataRow
    // 3. CommandComplete
    // 4. ReadyForQuery

    // The final messages sent to the client
    let mut res = BytesMut::new();

    // RowDescription
    res.put(row_description(&vec![(name, DataType::Text)]));

    // DataRow
    res.put(data_row(&vec![value.to_string()]));

    // CommandComplete
    res.put(command_complete("SELECT 1"));

    write_all_half(stream, &res).await?;
    send_ready_for_query(stream).await
}

pub fn row_description(columns: &Vec<(&str, DataType)>) -> BytesMut {
    let mut res = BytesMut::new();
    let mut row_desc = BytesMut::new();

    // how many columns we are storing
    row_desc.put_i16(columns.len() as i16);

    for (name, data_type) in columns {
        // Column name
        row_desc.put_slice(format!("{}\0", name).as_bytes());

        // Doesn't belong to any table
        row_desc.put_i32(0);

        // Doesn't belong to any table
        row_desc.put_i16(0);

        // Text
        row_desc.put_i32(data_type.into());

        // Text size = variable (-1)
        let type_size = match data_type {
            DataType::Text => -1,
            DataType::Int4 => 4,
            DataType::Numeric => -1,
            DataType::Bool => 1,
            DataType::Oid => 4,
            DataType::AnyArray => -1,
            DataType::Any => -1,
        };

        row_desc.put_i16(type_size);

        // Type modifier: none that I know
        row_desc.put_i32(-1);

        // Format being used: text (0), binary (1)
        row_desc.put_i16(0);
    }

    res.put_u8(b'T');
    res.put_i32(row_desc.len() as i32 + 4);
    res.put(row_desc);

    res
}

/// Create a DataRow message.
pub fn data_row(row: &Vec<String>) -> BytesMut {
    let mut res = BytesMut::new();
    let mut data_row = BytesMut::new();

    data_row.put_i16(row.len() as i16);

    for column in row {
        let column = column.as_bytes();
        data_row.put_i32(column.len() as i32);
        data_row.put_slice(column);
    }

    res.put_u8(b'D');
    res.put_i32(data_row.len() as i32 + 4);
    res.put(data_row);

    res
}

pub fn data_row_nullable(row: &Vec<Option<String>>) -> BytesMut {
    let mut res = BytesMut::new();
    let mut data_row = BytesMut::new();

    data_row.put_i16(row.len() as i16);

    for column in row {
        if let Some(column) = column {
            let column = column.as_bytes();
            data_row.put_i32(column.len() as i32);
            data_row.put_slice(column);
        } else {
            data_row.put_i32(-1_i32);
        }
    }

    res.put_u8(b'D');
    res.put_i32(data_row.len() as i32 + 4);
    res.put(data_row);

    res
}

/// Create a CommandComplete message.
pub fn command_complete(command: &str) -> BytesMut {
    let cmd = BytesMut::from(format!("{}\0", command).as_bytes());
    let mut res = BytesMut::new();
    res.put_u8(b'C');
    res.put_i32(cmd.len() as i32 + 4);
    res.put(cmd);
    res
}

/// Create a notify message.
pub fn notify(message: &str, details: String) -> BytesMut {
    let mut notify_cmd = BytesMut::new();

    notify_cmd.put_slice("SNOTICE\0".as_bytes());
    notify_cmd.put_slice("C00000\0".as_bytes());
    notify_cmd.put_slice(format!("M{}\0", message).as_bytes());
    notify_cmd.put_slice(format!("D{}\0", details).as_bytes());

    // this extra byte says that is the end of the package
    notify_cmd.put_u8(0);

    let mut res = BytesMut::new();
    res.put_u8(b'N');
    res.put_i32(notify_cmd.len() as i32 + 4);
    res.put(notify_cmd);

    res
}

pub fn flush() -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'H');
    bytes.put_i32(4);
    bytes
}

pub fn sync() -> BytesMut {
    let mut bytes = BytesMut::with_capacity(mem::size_of::<u8>() + mem::size_of::<i32>());
    bytes.put_u8(b'S');
    bytes.put_i32(4);
    bytes
}

pub fn parse_complete() -> BytesMut {
    let mut bytes = BytesMut::with_capacity(mem::size_of::<u8>() + mem::size_of::<i32>());

    bytes.put_u8(b'1');
    bytes.put_i32(4);
    bytes
}

pub fn bind_complete() -> BytesMut {
    let mut bytes = BytesMut::with_capacity(mem::size_of::<u8>() + mem::size_of::<i32>());

    bytes.put_u8(b'2');
    bytes.put_i32(4);
    bytes
}

pub fn no_data() -> BytesMut {
    let mut bytes = BytesMut::with_capacity(mem::size_of::<u8>() + mem::size_of::<i32>());

    bytes.put_u8(b'n');
    bytes.put_i32(4);
    bytes
}

/// ParameterDescription ('t') listing the parameter type OIDs.
pub fn parameter_description(param_types: &[i32]) -> BytesMut {
    let mut bytes = BytesMut::with_capacity(7 + 4 * param_types.len());

    bytes.put_u8(b't');
    bytes.put_i32(4 + 2 + 4 * param_types.len() as i32);
    bytes.put_i16(param_types.len() as i16);
    for oid in param_types {
        bytes.put_i32(*oid);
    }
    bytes
}

/// The query text of a Query or Parse message, without copying it.
pub fn query_text(message: &[u8]) -> &[u8] {
    let mut body = message.get(5..).unwrap_or_default();

    // Parse: skip the statement name.
    if message.first() == Some(&b'P') {
        if let Some(end) = body.iter().position(|byte| *byte == 0) {
            body = &body[end + 1..];
        }
    }

    match body.iter().position(|byte| *byte == 0) {
        Some(end) => &body[..end],
        None => body,
    }
}

/// Parameter type OIDs of a ParameterDescription ('t') message.
pub fn parameter_types(message: &[u8]) -> Result<Vec<i32>, Error> {
    let mut body = message
        .get(5..)
        .ok_or_else(|| Error::ProtocolSyncError("short ParameterDescription".into()))?;

    if body.len() < 2 {
        return Err(Error::ProtocolSyncError(
            "short ParameterDescription".into(),
        ));
    }
    let count = body.get_i16().max(0) as usize;

    if body.len() < 4 * count {
        return Err(Error::ProtocolSyncError(
            "short ParameterDescription".into(),
        ));
    }

    Ok((0..count).map(|_| body.get_i32()).collect())
}

/// One column of a RowDescription ('T') message.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_id: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    /// 0 for text, 1 for binary. Always 0 when describing a statement.
    pub format: i16,
}

/// RowDescription ('T') message.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowDescription {
    pub fields: Vec<FieldDescription>,
}

impl TryFrom<&[u8]> for RowDescription {
    type Error = Error;

    fn try_from(message: &[u8]) -> Result<RowDescription, Error> {
        let short = || Error::ProtocolSyncError("short RowDescription".into());

        let mut body = message.get(5..).ok_or_else(short)?;
        if body.len() < 2 {
            return Err(short());
        }
        let count = body.get_i16().max(0) as usize;
        let mut fields = Vec::with_capacity(count);

        for _ in 0..count {
            let end = body.iter().position(|byte| *byte == 0).ok_or_else(short)?;
            let name = String::from_utf8_lossy(&body[..end]).to_string();
            body.advance(end + 1);

            if body.len() < 18 {
                return Err(short());
            }

            fields.push(FieldDescription {
                name,
                table_oid: body.get_i32(),
                column_id: body.get_i16(),
                type_oid: body.get_i32(),
                type_size: body.get_i16(),
                type_modifier: body.get_i32(),
                format: body.get_i16(),
            });
        }

        Ok(RowDescription { fields })
    }
}

impl From<&RowDescription> for BytesMut {
    fn from(description: &RowDescription) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i16(description.fields.len() as i16);

        for field in &description.fields {
            body.put_slice(field.name.as_bytes());
            body.put_u8(0);
            body.put_i32(field.table_oid);
            body.put_i16(field.column_id);
            body.put_i32(field.type_oid);
            body.put_i16(field.type_size);
            body.put_i32(field.type_modifier);
            body.put_i16(field.format);
        }

        let mut message = BytesMut::with_capacity(body.len() + 5);
        message.put_u8(b'T');
        message.put_i32(body.len() as i32 + 4);
        message.put(body);
        message
    }
}

/// DataRow ('D') message. A `None` value is NULL.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DataRow {
    pub values: Vec<Option<Bytes>>,
}

impl TryFrom<&[u8]> for DataRow {
    type Error = Error;

    fn try_from(message: &[u8]) -> Result<DataRow, Error> {
        let short = || Error::ProtocolSyncError("short DataRow".into());

        let mut body = message.get(5..).ok_or_else(short)?;
        if body.len() < 2 {
            return Err(short());
        }
        let count = body.get_i16().max(0) as usize;
        let mut values = Vec::with_capacity(count);

        for _ in 0..count {
            if body.len() < 4 {
                return Err(short());
            }
            let len = body.get_i32();

            if len < 0 {
                values.push(None);
            } else {
                let len = len as usize;
                if body.len() < len {
                    return Err(short());
                }
                values.push(Some(Bytes::copy_from_slice(&body[..len])));
                body.advance(len);
            }
        }

        Ok(DataRow { values })
    }
}

impl From<&DataRow> for BytesMut {
    fn from(row: &DataRow) -> BytesMut {
        let len = 4
            + 2
            + row
                .values
                .iter()
                .map(|value| 4 + value.as_ref().map_or(0, |value| value.len()))
                .sum::<usize>();

        let mut message = BytesMut::with_capacity(len + 1);
        message.put_u8(b'D');
        message.put_i32(len as i32);
        message.put_i16(row.values.len() as i16);

        for value in &row.values {
            match value {
                Some(value) => {
                    message.put_i32(value.len() as i32);
                    message.put_slice(value);
                }
                None => message.put_i32(-1),
            }
        }

        message
    }
}

/// Expand Bind result format codes (none, one for all, or one per column)
/// to one code per column.
pub fn column_formats(result_formats: &[i16], columns: usize) -> Vec<i16> {
    match result_formats.len() {
        0 => vec![0; columns],
        1 => vec![result_formats[0]; columns],
        _ => (0..columns)
            .map(|i| result_formats.get(i).copied().unwrap_or(0))
            .collect(),
    }
}

/// Split a buffer of complete protocol messages into individual messages
/// (code + length + body).
pub fn split_messages(buf: &[u8]) -> Result<Vec<BytesMut>, Error> {
    let mut messages = Vec::new();
    let mut offset = 0;

    while offset < buf.len() {
        if buf.len() - offset < 5 {
            return Err(Error::ProtocolSyncError(
                "truncated message header".to_string(),
            ));
        }

        let len = i32::from_be_bytes([
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
        ]);

        if len < 4 || buf.len() - offset - 1 < len as usize {
            return Err(Error::ProtocolSyncError(format!(
                "truncated message '{}'",
                buf[offset] as char
            )));
        }

        let end = offset + 1 + len as usize;
        messages.push(BytesMut::from(&buf[offset..end]));
        offset = end;
    }

    Ok(messages)
}

pub fn ready_for_query(in_transaction: bool) -> BytesMut {
    ready_for_query_status(if in_transaction { b'T' } else { b'I' })
}

/// A ReadyForQuery with the transaction status: `I` idle, `T` in a transaction,
/// `E` in a failed transaction.
pub fn ready_for_query_status(status: u8) -> BytesMut {
    let mut bytes = BytesMut::with_capacity(
        mem::size_of::<u8>() + mem::size_of::<i32>() + mem::size_of::<u8>(),
    );

    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(status);

    bytes
}

/// Write all data in the buffer to the TcpStream.
pub async fn write_all<S>(stream: &mut S, buf: BytesMut) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    match stream.write_all(&buf).await {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::SocketError(format!(
            "Error writing to socket - Error: {:?}",
            err
        ))),
    }
}

/// Write all the data in the buffer to the TcpStream, write owned half (see mpsc).
pub async fn write_all_half<S>(stream: &mut S, buf: &BytesMut) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    match stream.write_all(buf).await {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::SocketError(format!(
            "Error writing to socket - Error: {:?}",
            err
        ))),
    }
}

pub async fn write_all_flush<S>(stream: &mut S, buf: &[u8]) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    match stream.write_all(buf).await {
        Ok(_) => match stream.flush().await {
            Ok(_) => Ok(()),
            Err(err) => Err(Error::SocketError(format!(
                "Error flushing socket - Error: {:?}",
                err
            ))),
        },
        Err(err) => Err(Error::SocketError(format!(
            "Error writing to socket - Error: {:?}",
            err
        ))),
    }
}

/// Memory reserved for a message before its body arrives.
const MESSAGE_PREALLOCATION: usize = 64 * 1024;

/// Read a complete message from the socket.
pub async fn read_message<S>(stream: &mut S) -> Result<BytesMut, Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
{
    let code = match stream.read_u8().await {
        Ok(code) => code,
        Err(err) => {
            return Err(Error::SocketError(format!(
                "Error reading message code from socket - Error {:?}",
                err
            )))
        }
    };

    let len = match stream.read_i32().await {
        Ok(len) => len,
        Err(err) => {
            return Err(Error::SocketError(format!(
                "Error reading message len from socket - Code: {:?}, Error: {:?}",
                code, err
            )))
        }
    };

    // The length counts itself.
    if len < mem::size_of::<i32>() as i32 {
        return Err(Error::SocketError(format!(
            "Error reading message from socket - Code: {:?} - Length {:?}, Error: {:?}",
            code, len, "Unexpected length value for message"
        )));
    }

    // The length is the peer's claim: past the first MESSAGE_PREALLOCATION
    // bytes, memory is reserved as the body arrives.
    let size = len as usize + 1;
    let mut bytes = BytesMut::with_capacity(size.min(MESSAGE_PREALLOCATION));

    bytes.put_u8(code);
    bytes.put_i32(len);

    while bytes.len() < size {
        let remaining = size - bytes.len();
        if bytes.len() == bytes.capacity() {
            // Doubling, so a large body is copied a bounded number of times.
            bytes.reserve(remaining.min(bytes.capacity()));
        }
        // Not past this message: the reader may hold the next one.
        let mut body = (&mut *stream).take(remaining as u64);
        let error = match body.read_buf(&mut bytes).await {
            Ok(0) => std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
            Ok(_) => continue,
            Err(err) => err,
        };
        return Err(Error::SocketError(format!(
            "Error reading message from socket - Code: {:?}, Error: {:?}",
            code, error
        )));
    }

    Ok(bytes)
}

pub fn server_parameter_message(key: &str, value: &str) -> BytesMut {
    let mut server_info = BytesMut::new();

    let null_byte_size = 1;
    let len: usize =
        mem::size_of::<i32>() + key.len() + null_byte_size + value.len() + null_byte_size;

    server_info.put_slice("S".as_bytes());
    server_info.put_i32(len.try_into().unwrap());
    server_info.put_slice(key.as_bytes());
    server_info.put_bytes(0, 1);
    server_info.put_slice(value.as_bytes());
    server_info.put_bytes(0, 1);

    server_info
}

pub fn configure_socket(stream: &TcpStream) {
    let sock_ref = SockRef::from(stream);
    let conf = get_config();

    #[cfg(target_os = "linux")]
    match sock_ref.set_tcp_user_timeout(Some(Duration::from_millis(conf.general.tcp_user_timeout)))
    {
        Ok(_) => (),
        Err(err) => error!("Could not configure tcp_user_timeout for socket: {}", err),
    }

    match sock_ref.set_keepalive(true) {
        Ok(_) => {
            match sock_ref.set_tcp_keepalive(
                &TcpKeepalive::new()
                    .with_interval(Duration::from_secs(conf.general.tcp_keepalives_interval))
                    .with_retries(conf.general.tcp_keepalives_count)
                    .with_time(Duration::from_secs(conf.general.tcp_keepalives_idle)),
            ) {
                Ok(_) => (),
                Err(err) => error!("Could not configure tcp_keepalive for socket: {}", err),
            }
        }
        Err(err) => error!("Could not configure socket: {}", err),
    }
    match sock_ref.set_tcp_nodelay(true) {
        Ok(_) => (),
        Err(err) => error!("Could not configure TCP_NODELAY for socket: {}", err),
    }
}

pub trait BytesMutReader {
    fn read_string(&mut self) -> Result<String, Error>;
}

impl BytesMutReader for Cursor<&BytesMut> {
    /// Should only be used when reading strings from the message protocol.
    /// Can be used to read multiple strings from the same message which are separated by the null byte
    fn read_string(&mut self) -> Result<String, Error> {
        let mut buf = vec![];
        match self.read_until(b'\0', &mut buf) {
            Ok(_) if buf.last() == Some(&b'\0') => {
                Ok(String::from_utf8_lossy(&buf[..buf.len() - 1]).to_string())
            }
            Ok(_) => Err(Error::ParseBytesError("Could not read string".to_string())),
            Err(err) => Err(Error::ParseBytesError(err.to_string())),
        }
    }
}

/// Reads from a client's message that fail, instead of panicking, when the
/// message is shorter than it says.
trait CheckedReader {
    fn take_u8(&mut self) -> Result<u8, Error>;
    fn take_i16(&mut self) -> Result<i16, Error>;
    /// A count: Postgres reads them as unsigned 16-bit integers.
    fn take_count(&mut self) -> Result<usize, Error>;
    fn take_i32(&mut self) -> Result<i32, Error>;
    fn skip(&mut self, len: usize) -> Result<(), Error>;
}

impl CheckedReader for Cursor<&BytesMut> {
    fn take_u8(&mut self) -> Result<u8, Error> {
        check_remaining(self, 1)?;
        Ok(self.get_u8())
    }

    fn take_i16(&mut self) -> Result<i16, Error> {
        check_remaining(self, 2)?;
        Ok(self.get_i16())
    }

    fn take_count(&mut self) -> Result<usize, Error> {
        check_remaining(self, 2)?;
        Ok(self.get_u16() as usize)
    }

    fn take_i32(&mut self) -> Result<i32, Error> {
        check_remaining(self, 4)?;
        Ok(self.get_i32())
    }

    fn skip(&mut self, len: usize) -> Result<(), Error> {
        check_remaining(self, len)?;
        self.advance(len);
        Ok(())
    }
}

fn check_remaining(cursor: &Cursor<&BytesMut>, len: usize) -> Result<(), Error> {
    if cursor.remaining() < len {
        return Err(Error::ParseBytesError(
            "message is shorter than it says".into(),
        ));
    }
    Ok(())
}

impl BytesMutReader for BytesMut {
    /// Should only be used when reading strings from the message protocol.
    /// Can be used to read multiple strings from the same message which are separated by the null byte
    fn read_string(&mut self) -> Result<String, Error> {
        let null_index = self.iter().position(|&byte| byte == b'\0');

        match null_index {
            Some(index) => {
                let string_bytes = self.split_to(index + 1);
                Ok(String::from_utf8_lossy(&string_bytes[..string_bytes.len() - 1]).to_string())
            }
            None => Err(Error::ParseBytesError("Could not read string".to_string())),
        }
    }
}

pub enum ExtendedProtocolData {
    Parse {
        data: BytesMut,
        metadata: Option<(Arc<Parse>, u64)>,
    },
    Bind {
        data: BytesMut,
        metadata: Option<(String, Arc<Parse>, u64)>,
        plan: Option<Arc<StatementPlan>>,
    },
    Describe {
        data: BytesMut,
        metadata: Option<(String, Arc<Parse>, u64)>,
        plan: Option<Arc<StatementPlan>>,
    },
    Execute {
        data: BytesMut,
    },
    Close {
        data: BytesMut,
        close: Close,
    },
    /// Answered by pgcat without involving the server.
    Local {
        response: BytesMut,
    },
}

impl ExtendedProtocolData {
    pub fn create_new_parse(data: BytesMut, metadata: Option<(Arc<Parse>, u64)>) -> Self {
        Self::Parse { data, metadata }
    }

    pub fn create_new_bind(
        data: BytesMut,
        metadata: Option<(String, Arc<Parse>, u64)>,
        plan: Option<Arc<StatementPlan>>,
    ) -> Self {
        Self::Bind {
            data,
            metadata,
            plan,
        }
    }

    pub fn create_new_describe(
        data: BytesMut,
        metadata: Option<(String, Arc<Parse>, u64)>,
        plan: Option<Arc<StatementPlan>>,
    ) -> Self {
        Self::Describe {
            data,
            metadata,
            plan,
        }
    }

    pub fn create_new_execute(data: BytesMut) -> Self {
        Self::Execute { data }
    }

    pub fn create_new_close(data: BytesMut, close: Close) -> Self {
        Self::Close { data, close }
    }

    pub fn create_new_local(response: BytesMut) -> Self {
        Self::Local { response }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }
}

/// Parse (F) message.
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[derive(Clone, Debug)]
pub struct Parse {
    code: char,
    #[allow(dead_code)]
    len: i32,
    pub name: String,
    query: String,
    num_params: i16,
    param_types: Vec<i32>,
    /// The SQL observer's classification of `query`, shared by the clients
    /// holding this statement from the pool's cache. Never part of the frame.
    observed: SharedSnap,
}

impl TryFrom<&BytesMut> for Parse {
    type Error = Error;

    fn try_from(buf: &BytesMut) -> Result<Parse, Error> {
        let mut cursor = Cursor::new(buf);
        let code = cursor.take_u8()? as char;
        let len = cursor.take_i32()?;
        let name = cursor.read_string()?;
        let query = cursor.read_string()?;
        let num_params = cursor.take_count()?;
        check_remaining(&cursor, 4 * num_params)?;
        let mut param_types = Vec::with_capacity(num_params);

        for _ in 0..num_params {
            param_types.push(cursor.take_i32()?);
        }

        Ok(Parse {
            code,
            len,
            name,
            query,
            num_params: num_params as i16,
            param_types,
            observed: SharedSnap::default(),
        })
    }
}

impl TryFrom<Parse> for BytesMut {
    type Error = Error;

    fn try_from(parse: Parse) -> Result<BytesMut, Error> {
        let mut bytes = BytesMut::new();

        let name_binding = CString::new(parse.name)?;
        let name = name_binding.as_bytes_with_nul();

        let query_binding = CString::new(parse.query)?;
        let query = query_binding.as_bytes_with_nul();

        // Recompute length of the message.
        let len = 4 // self
            + name.len()
            + query.len()
            + 2
            + 4 * parse.param_types.len();

        bytes.put_u8(parse.code as u8);
        bytes.put_i32(len as i32);
        bytes.put_slice(name);
        bytes.put_slice(query);
        bytes.put_u16(parse.param_types.len() as u16);
        for param in parse.param_types {
            bytes.put_i32(param);
        }

        Ok(bytes)
    }
}

impl TryFrom<&Parse> for BytesMut {
    type Error = Error;

    fn try_from(parse: &Parse) -> Result<BytesMut, Error> {
        parse.clone().try_into()
    }
}

impl Parse {
    /// Renames the prepared statement to a new name based on the global counter
    pub fn rewrite(mut self) -> Self {
        self.name = format!(
            "PGCAT_{}",
            PREPARED_STATEMENT_COUNTER.fetch_add(1, Ordering::SeqCst)
        );
        self
    }

    /// Gets the name of the prepared statement from the buffer
    pub fn get_name(buf: &BytesMut) -> Result<String, Error> {
        let mut cursor = Cursor::new(buf);
        // Skip the code and length
        cursor.skip(mem::size_of::<u8>() + mem::size_of::<i32>())?;
        cursor.read_string()
    }

    /// Gets the query text from the buffer
    pub fn get_query(buf: &BytesMut) -> Result<String, Error> {
        let mut cursor = Cursor::new(buf);
        // Skip the code and length
        cursor.advance(mem::size_of::<u8>() + mem::size_of::<i32>());
        cursor.read_string()?;
        cursor.read_string()
    }

    /// Hashes the parse statement to be used as a key in the global cache
    pub fn get_hash(&self) -> u64 {
        // TODO_ZAIN: Take a look at which hashing function is being used
        let mut hasher = DefaultHasher::new();

        let concatenated = format!(
            "{}{}{}",
            self.query,
            self.num_params,
            self.param_types
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        );

        concatenated.hash(&mut hasher);

        hasher.finish()
    }

    pub fn anonymous(&self) -> bool {
        self.name.is_empty()
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn param_types(&self) -> &[i32] {
        &self.param_types
    }

    /// The same statement under another name.
    pub fn named(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    /// Replace the query text, keeping the statement name and parameter types.
    pub fn with_query(mut self, query: String) -> Self {
        self.query = query;
        self.observed = SharedSnap::default();
        self
    }

    /// The SQL observer's classification of the query, once a client has
    /// staged it (see `sql_observer::SharedSnap`).
    pub fn observed(&self) -> &SharedSnap {
        &self.observed
    }

    /// Declare the types of extra parameters `$n+1...` appended after the
    /// client's `n` parameters. Types the client didn't declare stay unspecified (0).
    pub fn with_appended_params(mut self, client_params: usize, types: &[i32]) -> Self {
        self.param_types.resize(client_params, 0);
        self.param_types.extend_from_slice(types);
        self.num_params = self.param_types.len() as i16;
        self
    }
}

/// Bind (B) message.
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[derive(Clone, Debug)]
pub struct Bind {
    code: char,
    #[allow(dead_code)]
    len: i64,
    portal: String,
    pub prepared_statement: String,
    num_param_format_codes: i16,
    param_format_codes: Vec<i16>,
    num_param_values: i16,
    param_values: Vec<(i32, BytesMut)>,
    num_result_column_format_codes: i16,
    result_columns_format_codes: Vec<i16>,
}

impl TryFrom<&BytesMut> for Bind {
    type Error = Error;

    fn try_from(buf: &BytesMut) -> Result<Bind, Error> {
        let mut cursor = Cursor::new(buf);
        let code = cursor.take_u8()? as char;
        let len = cursor.take_i32()?;
        let portal = cursor.read_string()?;
        let prepared_statement = cursor.read_string()?;
        let num_param_format_codes = cursor.take_count()?;
        check_remaining(&cursor, 2 * num_param_format_codes)?;
        let mut param_format_codes = Vec::with_capacity(num_param_format_codes);

        for _ in 0..num_param_format_codes {
            param_format_codes.push(cursor.take_i16()?);
        }

        let num_param_values = cursor.take_count()?;
        check_remaining(&cursor, 4 * num_param_values)?;
        let mut param_values = Vec::with_capacity(num_param_values);

        for _ in 0..num_param_values {
            let param_len = cursor.take_i32()?;
            // There is special occasion when the parameter is NULL
            // In that case, param length is defined as -1
            // So if the passed parameter len is over 0
            if param_len > 0 {
                check_remaining(&cursor, param_len as usize)?;
                let mut param = BytesMut::with_capacity(param_len as usize);
                param.resize(param_len as usize, b'0');
                cursor.copy_to_slice(&mut param);
                // we push and the length and the parameter into vector
                param_values.push((param_len, param));
            } else {
                // otherwise we push a tuple with -1 and 0-len BytesMut
                // which means that after encountering -1 postgres proceeds
                // to processing another parameter
                param_values.push((param_len, BytesMut::new()));
            }
        }

        let num_result_column_format_codes = cursor.take_count()?;
        check_remaining(&cursor, 2 * num_result_column_format_codes)?;
        let mut result_columns_format_codes = Vec::with_capacity(num_result_column_format_codes);

        for _ in 0..num_result_column_format_codes {
            result_columns_format_codes.push(cursor.take_i16()?);
        }

        Ok(Bind {
            code,
            len: len as i64,
            portal,
            prepared_statement,
            num_param_format_codes: num_param_format_codes as i16,
            param_format_codes,
            num_param_values: num_param_values as i16,
            param_values,
            num_result_column_format_codes: num_result_column_format_codes as i16,
            result_columns_format_codes,
        })
    }
}

impl TryFrom<Bind> for BytesMut {
    type Error = Error;

    fn try_from(bind: Bind) -> Result<BytesMut, Error> {
        let mut bytes = BytesMut::new();

        let portal_binding = CString::new(bind.portal)?;
        let portal = portal_binding.as_bytes_with_nul();

        let prepared_statement_binding = CString::new(bind.prepared_statement)?;
        let prepared_statement = prepared_statement_binding.as_bytes_with_nul();

        let mut len = 4 // self
            + portal.len()
            + prepared_statement.len()
            + 2 // num_param_format_codes
            + 2 * bind.param_format_codes.len() // num_param_format_codes
            + 2; // num_param_values

        for (param_len, _) in &bind.param_values {
            // NULL parameters have a length of -1 and no data.
            len += 4 + (*param_len).max(0) as usize;
        }
        len += 2; // num_result_column_format_codes
        len += 2 * bind.result_columns_format_codes.len();

        bytes.put_u8(bind.code as u8);
        bytes.put_i32(len as i32);
        bytes.put_slice(portal);
        bytes.put_slice(prepared_statement);
        bytes.put_u16(bind.param_format_codes.len() as u16);
        for param_format_code in bind.param_format_codes {
            bytes.put_i16(param_format_code);
        }
        bytes.put_u16(bind.param_values.len() as u16);
        for (param_len, param) in bind.param_values {
            bytes.put_i32(param_len);
            bytes.put_slice(&param);
        }
        bytes.put_u16(bind.result_columns_format_codes.len() as u16);
        for result_column_format_code in bind.result_columns_format_codes {
            bytes.put_i16(result_column_format_code);
        }

        Ok(bytes)
    }
}

impl Bind {
    pub fn portal(&self) -> &str {
        &self.portal
    }

    /// Gets the name of the portal from the buffer
    pub fn get_portal(buf: &BytesMut) -> Result<String, Error> {
        let mut cursor = Cursor::new(buf);
        // Skip the code and length
        cursor.skip(mem::size_of::<u8>() + mem::size_of::<i32>())?;
        cursor.read_string()
    }

    /// Gets the name of the prepared statement from the buffer
    pub fn get_name(buf: &BytesMut) -> Result<String, Error> {
        let mut cursor = Cursor::new(buf);
        // Skip the code and length
        cursor.skip(mem::size_of::<u8>() + mem::size_of::<i32>())?;
        cursor.read_string()?;
        cursor.read_string()
    }

    /// Renames the prepared statement to a new name
    pub fn rename(buf: BytesMut, new_name: &str) -> Result<BytesMut, Error> {
        let mut cursor = Cursor::new(&buf);
        // Read basic data from the cursor
        let code = cursor.take_u8()?;
        let current_len = cursor.take_i32()?;
        let portal = cursor.read_string()?;
        let prepared_statement = cursor.read_string()?;

        // Calculate new length
        let new_len = current_len + new_name.len() as i32 - prepared_statement.len() as i32;

        // Begin building the response buffer
        let mut response_buf = BytesMut::with_capacity(new_len as usize + 1);
        response_buf.put_u8(code);
        response_buf.put_i32(new_len);

        // Put the portal and new name into the buffer
        // Note: panic if the provided string contains null byte
        response_buf.put_slice(CString::new(portal)?.as_bytes_with_nul());
        response_buf.put_slice(CString::new(new_name)?.as_bytes_with_nul());

        // Add the remainder of the original buffer into the response
        response_buf.put_slice(&buf[cursor.position() as usize..]);

        // Return the buffer
        Ok(response_buf)
    }

    pub fn anonymous(&self) -> bool {
        self.prepared_statement.is_empty()
    }

    /// The parameter values with one format code per value.
    pub fn params(&self) -> BindParams {
        let formats = (0..self.param_values.len())
            .map(|i| match self.param_format_codes.len() {
                0 => 0,
                1 => self.param_format_codes[0],
                _ => self.param_format_codes.get(i).copied().unwrap_or(0),
            })
            .collect();

        let values = self
            .param_values
            .iter()
            .map(|(len, value)| {
                if *len < 0 {
                    None
                } else {
                    Some(value.clone().freeze())
                }
            })
            .collect();

        BindParams {
            formats,
            values,
            result_formats: self.result_columns_format_codes.clone(),
            modified: false,
        }
    }

    /// Replace the parameter values (and result format codes). Format codes are
    /// written with as few codes as possible: none if all are text, one if they
    /// are all the same, one per parameter otherwise.
    pub fn with_params(mut self, params: BindParams) -> Bind {
        self.param_format_codes = if params.formats.iter().all(|format| *format == 0) {
            Vec::new()
        } else if params.formats.windows(2).all(|pair| pair[0] == pair[1]) {
            vec![params.formats[0]]
        } else {
            params.formats.clone()
        };
        self.num_param_format_codes = self.param_format_codes.len() as i16;

        self.param_values = params
            .values
            .into_iter()
            .map(|value| match value {
                Some(value) => (value.len() as i32, BytesMut::from(&value[..])),
                None => (-1, BytesMut::new()),
            })
            .collect();
        self.num_param_values = self.param_values.len() as i16;

        self.num_result_column_format_codes = params.result_formats.len() as i16;
        self.result_columns_format_codes = params.result_formats;

        self
    }
}

/// Parameter values of a Bind message, as seen by plugins.
/// Formats are 0 (text) or 1 (binary); a `None` value is NULL.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BindParams {
    formats: Vec<i16>,
    values: Vec<Option<Bytes>>,
    result_formats: Vec<i16>,
    modified: bool,
}

impl BindParams {
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn format(&self, index: usize) -> i16 {
        self.formats[index]
    }

    pub fn value(&self, index: usize) -> Option<&[u8]> {
        self.values[index].as_deref()
    }

    /// Replace parameter `index`.
    pub fn set(&mut self, index: usize, format: i16, value: Option<Vec<u8>>) {
        self.formats[index] = format;
        self.values[index] = value.map(Bytes::from);
        self.modified = true;
    }

    /// Append a parameter after the existing ones.
    pub fn push(&mut self, format: i16, value: Option<Vec<u8>>) {
        self.formats.push(format);
        self.values.push(value.map(Bytes::from));
        self.modified = true;
    }

    /// Result column format codes as sent by the client (none, one for all
    /// columns, or one per column).
    pub fn result_formats(&self) -> &[i16] {
        &self.result_formats
    }

    pub fn set_result_formats(&mut self, formats: Vec<i16>) {
        self.result_formats = formats;
        self.modified = true;
    }

    /// The parameters were changed and the Bind must be rewritten.
    pub fn modified(&self) -> bool {
        self.modified
    }
}

#[derive(Debug, Clone)]
pub struct Describe {
    code: char,

    #[allow(dead_code)]
    len: i32,
    pub target: char,
    pub statement_name: String,
}

impl TryFrom<&BytesMut> for Describe {
    type Error = Error;

    fn try_from(bytes: &BytesMut) -> Result<Describe, Error> {
        let mut cursor = Cursor::new(bytes);
        let code = cursor.take_u8()? as char;
        let len = cursor.take_i32()?;
        let target = cursor.take_u8()? as char;
        let statement_name = cursor.read_string()?;

        Ok(Describe {
            code,
            len,
            target,
            statement_name,
        })
    }
}

impl TryFrom<Describe> for BytesMut {
    type Error = Error;

    fn try_from(describe: Describe) -> Result<BytesMut, Error> {
        let mut bytes = BytesMut::new();
        let statement_name_binding = CString::new(describe.statement_name)?;
        let statement_name = statement_name_binding.as_bytes_with_nul();
        let len = 4 + 1 + statement_name.len();

        bytes.put_u8(describe.code as u8);
        bytes.put_i32(len as i32);
        bytes.put_u8(describe.target as u8);
        bytes.put_slice(statement_name);

        Ok(bytes)
    }
}

impl Describe {
    pub fn empty_new() -> Describe {
        Describe {
            code: 'D',
            len: 4 + 1 + 1,
            target: 'S',
            statement_name: "".to_string(),
        }
    }

    pub fn rename(mut self, name: &str) -> Self {
        self.statement_name = name.to_string();
        self
    }

    pub fn anonymous(&self) -> bool {
        self.statement_name.is_empty()
    }
}

/// Execute (F) message.
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
pub struct Execute;

impl Execute {
    /// Gets the name of the portal from the buffer
    pub fn get_portal(buf: &BytesMut) -> Result<String, Error> {
        let mut cursor = Cursor::new(buf);
        // Skip the code and length
        cursor.skip(mem::size_of::<u8>() + mem::size_of::<i32>())?;
        cursor.read_string()
    }
}

/// Close (F) message.
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[derive(Clone, Debug)]
pub struct Close {
    code: char,
    #[allow(dead_code)]
    len: i32,
    close_type: char,
    pub name: String,
}

impl TryFrom<&BytesMut> for Close {
    type Error = Error;

    fn try_from(bytes: &BytesMut) -> Result<Close, Error> {
        let mut cursor = Cursor::new(bytes);
        let code = cursor.take_u8()? as char;
        let len = cursor.take_i32()?;
        let close_type = cursor.take_u8()? as char;
        let name = cursor.read_string()?;

        Ok(Close {
            code,
            len,
            close_type,
            name,
        })
    }
}

impl TryFrom<Close> for BytesMut {
    type Error = Error;

    fn try_from(close: Close) -> Result<BytesMut, Error> {
        debug!("Close: {:?}", close);

        let mut bytes = BytesMut::new();
        let name_binding = CString::new(close.name)?;
        let name = name_binding.as_bytes_with_nul();
        let len = 4 + 1 + name.len();

        bytes.put_u8(close.code as u8);
        bytes.put_i32(len as i32);
        bytes.put_u8(close.close_type as u8);
        bytes.put_slice(name);

        Ok(bytes)
    }
}

impl Close {
    pub fn is_portal(&self) -> bool {
        self.close_type == 'P'
    }

    pub fn new(name: &str) -> Close {
        let name = name.to_string();

        Close {
            code: 'C',
            len: 4 + 1 + name.len() as i32 + 1, // will be recalculated
            close_type: 'S',
            name,
        }
    }

    pub fn is_prepared_statement(&self) -> bool {
        self.close_type == 'S'
    }

    pub fn anonymous(&self) -> bool {
        self.name.is_empty()
    }
}

pub fn close_complete() -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.put_u8(b'3');
    bytes.put_i32(4);
    bytes
}

// from https://www.postgresql.org/docs/12/protocol-error-fields.html
#[derive(Debug, Default, PartialEq)]
pub struct PgErrorMsg {
    pub severity_localized: String,      // S
    pub severity: String,                // V
    pub code: String,                    // C
    pub message: String,                 // M
    pub detail: Option<String>,          // D
    pub hint: Option<String>,            // H
    pub position: Option<u32>,           // P
    pub internal_position: Option<u32>,  // p
    pub internal_query: Option<String>,  // q
    pub where_context: Option<String>,   // W
    pub schema_name: Option<String>,     // s
    pub table_name: Option<String>,      // t
    pub column_name: Option<String>,     // c
    pub data_type_name: Option<String>,  // d
    pub constraint_name: Option<String>, // n
    pub file_name: Option<String>,       // F
    pub line: Option<u32>,               // L
    pub routine: Option<String>,         // R
}

// TODO: implement with https://docs.rs/derive_more/latest/derive_more/
impl Display for PgErrorMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[severity: {}]", self.severity)?;
        write!(f, "[code: {}]", self.code)?;
        write!(f, "[message: {}]", self.message)?;
        if let Some(val) = &self.detail {
            write!(f, "[detail: {val}]")?;
        }
        if let Some(val) = &self.hint {
            write!(f, "[hint: {val}]")?;
        }
        if let Some(val) = &self.position {
            write!(f, "[position: {val}]")?;
        }
        if let Some(val) = &self.internal_position {
            write!(f, "[internal_position: {val}]")?;
        }
        if let Some(val) = &self.internal_query {
            write!(f, "[internal_query: {val}]")?;
        }
        if let Some(val) = &self.internal_query {
            write!(f, "[internal_query: {val}]")?;
        }
        if let Some(val) = &self.where_context {
            write!(f, "[where: {val}]")?;
        }
        if let Some(val) = &self.schema_name {
            write!(f, "[schema_name: {val}]")?;
        }
        if let Some(val) = &self.table_name {
            write!(f, "[table_name: {val}]")?;
        }
        if let Some(val) = &self.column_name {
            write!(f, "[column_name: {val}]")?;
        }
        if let Some(val) = &self.data_type_name {
            write!(f, "[data_type_name: {val}]")?;
        }
        if let Some(val) = &self.constraint_name {
            write!(f, "[constraint_name: {val}]")?;
        }
        if let Some(val) = &self.file_name {
            write!(f, "[file_name: {val}]")?;
        }
        if let Some(val) = &self.line {
            write!(f, "[line: {val}]")?;
        }
        if let Some(val) = &self.routine {
            write!(f, "[routine: {val}]")?;
        }

        write!(f, " ")?;

        Ok(())
    }
}

impl PgErrorMsg {
    pub fn parse(error_msg: &[u8]) -> Result<PgErrorMsg, Error> {
        let mut out = PgErrorMsg {
            severity_localized: "".to_string(),
            severity: "".to_string(),
            code: "".to_string(),
            message: "".to_string(),
            detail: None,
            hint: None,
            position: None,
            internal_position: None,
            internal_query: None,
            where_context: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            data_type_name: None,
            constraint_name: None,
            file_name: None,
            line: None,
            routine: None,
        };
        for msg_part in error_msg.split(|v| *v == MESSAGE_TERMINATOR) {
            if msg_part.is_empty() {
                continue;
            }

            let msg_content = match String::from_utf8_lossy(&msg_part[1..]).parse() {
                Ok(c) => c,
                Err(err) => {
                    return Err(Error::ServerMessageParserError(format!(
                        "could not parse server message field. err {:?}",
                        err
                    )))
                }
            };

            match &msg_part[0] {
                b'S' => {
                    out.severity_localized = msg_content;
                }
                b'V' => {
                    out.severity = msg_content;
                }
                b'C' => {
                    out.code = msg_content;
                }
                b'M' => {
                    out.message = msg_content;
                }
                b'D' => {
                    out.detail = Some(msg_content);
                }
                b'H' => {
                    out.hint = Some(msg_content);
                }
                b'P' => out.position = Some(u32::from_str(msg_content.as_str()).unwrap_or(0)),
                b'p' => {
                    out.internal_position = Some(u32::from_str(msg_content.as_str()).unwrap_or(0))
                }
                b'q' => {
                    out.internal_query = Some(msg_content);
                }
                b'W' => {
                    out.where_context = Some(msg_content);
                }
                b's' => {
                    out.schema_name = Some(msg_content);
                }
                b't' => {
                    out.table_name = Some(msg_content);
                }
                b'c' => {
                    out.column_name = Some(msg_content);
                }
                b'd' => {
                    out.data_type_name = Some(msg_content);
                }
                b'n' => {
                    out.constraint_name = Some(msg_content);
                }
                b'F' => {
                    out.file_name = Some(msg_content);
                }
                b'L' => out.line = Some(u32::from_str(msg_content.as_str()).unwrap_or(0)),
                b'R' => {
                    out.routine = Some(msg_content);
                }
                _ => {}
            }
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::messages::PgErrorMsg;
    use log::{error, info};

    /// Messages of several sizes, one larger than a client's read buffer.
    fn stream_of_messages() -> Vec<bytes::BytesMut> {
        let close: bytes::BytesMut = super::Close::new("PGCAT_1").try_into().unwrap();
        vec![
            super::simple_query("SELECT 1"),
            super::sync(),
            close,
            super::data_row(&vec!["x".repeat(10_000)]),
            super::flush(),
        ]
    }

    #[tokio::test]
    async fn test_read_message_whatever_each_read_returns() {
        use tokio::io::AsyncWriteExt;

        let messages = stream_of_messages();
        let stream = messages.concat();
        for read_size in [1, 2, 3, 5, 4096] {
            // Each read of the duplex returns at most `read_size` bytes.
            let (mut client, mut pgcat) = tokio::io::duplex(read_size);
            let send = async {
                client.write_all(&stream).await.unwrap();
                client.shutdown().await.unwrap();
            };
            let receive = async {
                let mut received = Vec::new();
                for _ in 0..messages.len() {
                    received.push(super::read_message(&mut pgcat).await.unwrap());
                }
                assert!(super::read_message(&mut pgcat).await.is_err());
                received
            };
            let ((), received) = tokio::join!(send, receive);
            assert_eq!(received, messages, "reads of {} bytes", read_size);
        }
    }

    #[tokio::test]
    async fn test_read_message_reserves_as_the_body_arrives() {
        use crate::test_allocator::take_largest;
        use tokio::io::AsyncWriteExt;

        // Claims about 1 GB, sends 100 bytes and hangs up.
        let mut stream = vec![b'Q'];
        stream.extend((1i32 << 30).to_be_bytes());
        stream.extend([b'x'; 100]);
        take_largest();
        assert!(super::read_message(&mut &stream[..]).await.is_err());
        assert!(take_largest() <= super::MESSAGE_PREALLOCATION);

        // A body larger than that is still read whole, in memory that grows
        // with it.
        let body = vec![b'x'; 1 << 20];
        let message = super::data_row(&vec![String::from_utf8(body).unwrap()]);
        let (mut client, mut pgcat) = tokio::io::duplex(4096);
        let send = async {
            client.write_all(&message).await.unwrap();
        };
        let ((), read) = tokio::join!(send, super::read_message(&mut pgcat));
        assert_eq!(read.unwrap(), message);
    }

    #[tokio::test]
    async fn test_read_message_fuzz() {
        for seed in 0..5_000 {
            let mut rng = crate::fuzz::Rng::new(seed);
            // Messages end to end, the last length maybe invalid, the stream
            // cut anywhere.
            let mut stream = Vec::new();
            for _ in 0..1 + rng.below(3) {
                stream.push(rng.next_u64() as u8);
                let len = rng.below(48) as i32 - 8;
                stream.extend(len.to_be_bytes());
                if len < 4 {
                    break;
                }
                stream.extend((4..len).map(|_| rng.next_u64() as u8));
            }
            stream.truncate(rng.below(stream.len() + 1));

            let mut reader = &stream[..];
            while let Ok(message) = super::read_message(&mut reader).await {
                let len = i32::from_be_bytes([message[1], message[2], message[3], message[4]]);
                assert!(len >= 4, "seed {}", seed);
                assert_eq!(message.len(), len as usize + 1, "seed {}", seed);
            }
        }
    }

    #[test]
    fn test_message_parsers_fuzz() {
        use super::*;

        fn message(code: u8, body: &[u8]) -> Vec<u8> {
            let mut message = vec![code];
            message.extend(((body.len() + 4) as i32).to_be_bytes());
            message.extend(body);
            message
        }
        let parse = message(b'P', b"s\0SELECT $1, $2\0\0\x02\0\0\0\x17\0\0\0\x19");
        let bind = message(
            b'B',
            b"p\0s\0\0\x01\0\x01\0\x02\0\0\0\x02hi\xff\xff\xff\xff\0\x01\0\0",
        );
        let seeds = [
            parse,
            bind,
            message(b'D', b"Ss\0"),
            message(b'E', b"p\0\0\0\0\0"),
            message(b'C', b"Pp\0"),
            row_description(&vec![("a", DataType::Text), ("b", DataType::Int4)]).to_vec(),
            data_row_nullable(&vec![Some("x".into()), None]).to_vec(),
        ];

        for seed in 0..20_000 {
            let mut rng = crate::fuzz::Rng::new(seed);
            let original = rng.pick(&seeds).clone();
            let mutant = BytesMut::from(&rng.mutate(&original)[..]);
            // Whatever the bytes, each returns, Ok or Err.
            let _ = Parse::try_from(&mutant);
            let _ = Parse::get_name(&mutant);
            let _ = Bind::try_from(&mutant);
            let _ = Bind::get_name(&mutant);
            let _ = Bind::get_portal(&mutant);
            let _ = Describe::try_from(&mutant);
            let _ = Execute::get_portal(&mutant);
            let _ = Close::try_from(&mutant);
            let _ = RowDescription::try_from(&mutant[..]);
            let _ = DataRow::try_from(&mutant[..]);
            let _ = split_messages(&mutant);
        }
    }

    #[tokio::test]
    async fn test_read_message_stream_cut_anywhere() {
        let messages = stream_of_messages();
        let stream = messages.concat();
        let ends: Vec<usize> = messages
            .iter()
            .scan(0, |end, message| {
                *end += message.len();
                Some(*end)
            })
            .collect();
        for cut in 0..stream.len() {
            let whole = ends.iter().filter(|end| **end <= cut).count();
            let mut cut_stream = &stream[..cut];
            for message in &messages[..whole] {
                assert_eq!(
                    &super::read_message(&mut cut_stream).await.unwrap(),
                    message
                );
            }
            assert!(
                super::read_message(&mut cut_stream).await.is_err(),
                "cut at byte {}",
                cut
            );
        }
    }

    #[test]
    fn test_split_messages() {
        use super::{bind_complete, parse_complete, ready_for_query, split_messages};
        use bytes::BufMut;

        let mut buf = bytes::BytesMut::new();
        buf.put(parse_complete());
        buf.put(bind_complete());
        buf.put(ready_for_query(true));

        let messages = split_messages(&buf).unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0], parse_complete());
        assert_eq!(messages[1], bind_complete());
        assert_eq!(messages[2], ready_for_query(true));

        // Truncated input is an error, not a panic.
        assert!(split_messages(&buf[..buf.len() - 1]).is_err());
        assert!(split_messages(&buf[..3]).is_err());
        assert!(split_messages(&[]).unwrap().is_empty());
    }

    /// Bind for `statement` with (format, value) parameters and result format codes.
    fn bind_message(
        statement: &str,
        format_codes: &[i16],
        values: &[Option<&[u8]>],
        result_formats: &[i16],
    ) -> bytes::BytesMut {
        use bytes::BufMut;

        let mut payload = bytes::BytesMut::new();
        payload.put_slice(format!("portal\0{}\0", statement).as_bytes());
        payload.put_i16(format_codes.len() as i16);
        for code in format_codes {
            payload.put_i16(*code);
        }
        payload.put_i16(values.len() as i16);
        for value in values {
            match value {
                Some(value) => {
                    payload.put_i32(value.len() as i32);
                    payload.put_slice(value);
                }
                None => payload.put_i32(-1),
            }
        }
        payload.put_i16(result_formats.len() as i16);
        for code in result_formats {
            payload.put_i16(*code);
        }

        let mut message = bytes::BytesMut::from(&b"B"[..]);
        message.put_i32(payload.len() as i32 + 4);
        message.put(payload);
        message
    }

    #[test]
    fn test_bind_params_round_trip() {
        use super::Bind;

        // No format codes: everything is text. NULL and empty values survive.
        let message = bind_message("s", &[], &[Some(b"a"), None, Some(b"")], &[1]);
        let bind: Bind = (&message).try_into().unwrap();
        let params = bind.params();
        assert_eq!(params.len(), 3);
        assert_eq!(params.format(0), 0);
        assert_eq!(params.value(0), Some(&b"a"[..]));
        assert_eq!(params.value(1), None);
        assert_eq!(params.value(2), Some(&b""[..]));
        assert_eq!(params.result_formats(), &[1]);
        assert!(!params.modified());

        // Unchanged params serialize back to the original message.
        let rewritten: bytes::BytesMut = bind.with_params(params).try_into().unwrap();
        assert_eq!(rewritten, message);
    }

    #[test]
    fn test_bind_params_format_codes() {
        use super::Bind;

        // One code applies to all parameters.
        let message = bind_message("s", &[1], &[Some(b"\x00\x01"), Some(b"\x02")], &[]);
        let bind: Bind = (&message).try_into().unwrap();
        let mut params = bind.params();
        assert_eq!(params.format(1), 1);

        // Appending a binary parameter keeps a single code.
        params.push(1, Some(vec![9]));
        let rewritten: bytes::BytesMut = bind.clone().with_params(params).try_into().unwrap();
        assert_eq!(
            rewritten,
            bind_message(
                "s",
                &[1],
                &[Some(b"\x00\x01"), Some(b"\x02"), Some(&[9])],
                &[]
            )
        );

        // Appending a text parameter needs one code per parameter.
        let mut params = bind.clone().params();
        params.push(0, None);
        let rewritten: bytes::BytesMut = bind.clone().with_params(params).try_into().unwrap();
        assert_eq!(
            rewritten,
            bind_message(
                "s",
                &[1, 1, 0],
                &[Some(b"\x00\x01"), Some(b"\x02"), None],
                &[]
            )
        );

        // Per-parameter codes that end up all text collapse to none.
        let message = bind_message("s", &[0, 1], &[Some(b"a"), Some(b"b")], &[0, 1]);
        let bind: Bind = (&message).try_into().unwrap();
        let mut params = bind.params();
        params.set(1, 0, Some(b"c".to_vec()));
        params.set_result_formats(vec![1, 1, 1]);
        let rewritten: bytes::BytesMut = bind.with_params(params).try_into().unwrap();
        assert_eq!(
            rewritten,
            bind_message("s", &[], &[Some(b"a"), Some(b"c")], &[1, 1, 1])
        );
    }

    #[test]
    fn test_parse_with_appended_params() {
        use super::Parse;
        use bytes::BufMut;

        let mut message = bytes::BytesMut::from(&b"P"[..]);
        let payload = b"s\0INSERT INTO t (a, b) VALUES ($1, $2)\0";
        message.put_i32(4 + payload.len() as i32 + 2 + 4);
        message.put_slice(payload);
        message.put_i16(1);
        message.put_i32(25);

        let parse: Parse = (&message).try_into().unwrap();
        let parse = parse.with_appended_params(2, &[17, 23]);
        assert_eq!(parse.param_types(), &[25, 0, 17, 23]);

        let bytes: bytes::BytesMut = parse.clone().try_into().unwrap();
        let reparsed: Parse = (&bytes).try_into().unwrap();
        assert_eq!(reparsed.param_types(), &[25, 0, 17, 23]);
        assert_eq!(reparsed.query(), parse.query());
    }

    #[test]
    fn test_parameter_description() {
        use super::parameter_description;

        let message = parameter_description(&[23, 25]);
        assert_eq!(
            &message[..],
            &[b't', 0, 0, 0, 14, 0, 2, 0, 0, 0, 23, 0, 0, 0, 25]
        );
        assert_eq!(&parameter_description(&[])[..], &[b't', 0, 0, 0, 6, 0, 0]);
    }

    fn field(kind: char, content: &str) -> Vec<u8> {
        format!("{kind}{content}\0").as_bytes().to_vec()
    }

    #[test]
    fn parse_fields() {
        let mut complete_msg = vec![];
        let severity = "FATAL";
        complete_msg.extend(field('S', severity));
        complete_msg.extend(field('V', severity));

        let error_code = "29P02";
        complete_msg.extend(field('C', error_code));
        let message = "password authentication failed for user \"wrong_user\"";
        complete_msg.extend(field('M', message));
        let detail_msg = "super detailed message";
        complete_msg.extend(field('D', detail_msg));
        let hint_msg = "hint detail here";
        complete_msg.extend(field('H', hint_msg));
        complete_msg.extend(field('P', "123"));
        complete_msg.extend(field('p', "234"));
        let internal_query = "SELECT * from foo;";
        complete_msg.extend(field('q', internal_query));
        let where_msg = "where goes here";
        complete_msg.extend(field('W', where_msg));
        let schema_msg = "schema_name";
        complete_msg.extend(field('s', schema_msg));
        let table_msg = "table_name";
        complete_msg.extend(field('t', table_msg));
        let column_msg = "column_name";
        complete_msg.extend(field('c', column_msg));
        let data_type_msg = "type_name";
        complete_msg.extend(field('d', data_type_msg));
        let constraint_msg = "constraint_name";
        complete_msg.extend(field('n', constraint_msg));
        let file_msg = "pgcat.c";
        complete_msg.extend(field('F', file_msg));
        complete_msg.extend(field('L', "335"));
        let routine_msg = "my_failing_routine";
        complete_msg.extend(field('R', routine_msg));

        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(true)
            .init();

        info!(
            "full message: {}",
            PgErrorMsg::parse(&complete_msg).unwrap()
        );
        assert_eq!(
            PgErrorMsg {
                severity_localized: severity.to_string(),
                severity: severity.to_string(),
                code: error_code.to_string(),
                message: message.to_string(),
                detail: Some(detail_msg.to_string()),
                hint: Some(hint_msg.to_string()),
                position: Some(123),
                internal_position: Some(234),
                internal_query: Some(internal_query.to_string()),
                where_context: Some(where_msg.to_string()),
                schema_name: Some(schema_msg.to_string()),
                table_name: Some(table_msg.to_string()),
                column_name: Some(column_msg.to_string()),
                data_type_name: Some(data_type_msg.to_string()),
                constraint_name: Some(constraint_msg.to_string()),
                file_name: Some(file_msg.to_string()),
                line: Some(335),
                routine: Some(routine_msg.to_string()),
            },
            PgErrorMsg::parse(&complete_msg).unwrap()
        );

        let mut only_mandatory_msg = vec![];
        only_mandatory_msg.extend(field('S', severity));
        only_mandatory_msg.extend(field('V', severity));
        only_mandatory_msg.extend(field('C', error_code));
        only_mandatory_msg.extend(field('M', message));
        only_mandatory_msg.extend(field('D', detail_msg));

        let err_fields = PgErrorMsg::parse(&only_mandatory_msg).unwrap();
        info!("only mandatory fields: {}", &err_fields);
        error!(
            "server error: {}: {}",
            err_fields.severity, err_fields.message
        );
        assert_eq!(
            PgErrorMsg {
                severity_localized: severity.to_string(),
                severity: severity.to_string(),
                code: error_code.to_string(),
                message: message.to_string(),
                detail: Some(detail_msg.to_string()),
                hint: None,
                position: None,
                internal_position: None,
                internal_query: None,
                where_context: None,
                schema_name: None,
                table_name: None,
                column_name: None,
                data_type_name: None,
                constraint_name: None,
                file_name: None,
                line: None,
                routine: None,
            },
            PgErrorMsg::parse(&only_mandatory_msg).unwrap()
        );
    }

    /// Severity (S and V), SQLSTATE, and message of an ErrorResponse, which
    /// must be the whole of `message`.
    fn fields(message: &[u8]) -> (String, String, String, String) {
        assert_eq!(message[0], b'E');
        let length = i32::from_be_bytes(message[1..5].try_into().unwrap()) as usize;
        assert_eq!(length + 1, message.len());
        let error = PgErrorMsg::parse(&message[5..]).unwrap();
        (
            error.severity_localized,
            error.severity,
            error.code,
            error.message,
        )
    }

    fn expect(severity: &str, code: &str, message: &str) -> (String, String, String, String) {
        (
            severity.to_string(),
            severity.to_string(),
            code.to_string(),
            message.to_string(),
        )
    }

    // Clients match on these codes and severities, so each one is pinned.
    #[test]
    fn test_codes_pgcat_sends_on_its_own_behalf() {
        use super::*;

        let shutdown = "terminating connection due to administrator command";
        assert_eq!(
            fields(&fatal_message(SHUTTING_DOWN, shutdown)),
            expect("FATAL", "PS001", shutdown)
        );
        let lost = "error receiving data from server: SocketError(\"closed\")";
        assert_eq!(
            fields(&fatal_message(SERVER_CONNECTION_LOST, lost)),
            expect("FATAL", "08006", lost)
        );
        let checkout = "could not get connection from the pool - AllServersDown";
        assert_eq!(
            fields(&error_message(&with_sqlstate(
                NO_SERVER_CONNECTION,
                checkout
            ))),
            expect("ERROR", "53300", checkout)
        );
        let limit = "checkout failure limit reached (2 / 2)";
        assert_eq!(
            fields(&fatal_message(NO_SERVER_CONNECTION, limit)),
            expect("FATAL", "53300", limit)
        );
        // An error pgcat gives no code keeps system_error, its connection
        // closed.
        assert_eq!(
            fields(&error_message("pool statement timeout")),
            expect("FATAL", "58000", "pool statement timeout")
        );
    }

    #[test]
    fn test_fatal_message_keeps_the_message_one_field() {
        use super::*;

        // A NUL would end the field early and make the rest a new field.
        assert_eq!(
            fields(&fatal_message(SHUTTING_DOWN, "a\0Cbad")),
            expect("FATAL", "PS001", "a Cbad")
        );
        // A message that looks like with_sqlstate's encoding is only text.
        assert_eq!(
            fields(&fatal_message(SHUTTING_DOWN, &with_sqlstate("XX000", "m"))),
            expect("FATAL", "PS001", " XX000 m")
        );
    }
}
