//! The queries plugin hooks run on the statement's server connection, before
//! it's sent: framed with the extended protocol under pgcat's own statement
//! and portal names, so they can't disturb the client's.

use bytes::{BufMut, BytesMut};

use crate::errors::Error;
use crate::messages::DataRow;
use crate::server::Server;

/// Run a query with the extended protocol on a server connection and return the rows.
/// It uses its own named statement and portal so it can't disturb the client's.
/// `begin`: open a transaction first, in the same round trip.
pub async fn server_query(
    server: &mut Server,
    sql: &str,
    params: &[Option<&str>],
    begin: bool,
) -> Result<Vec<Vec<Option<String>>>, Error> {
    const NAME: &str = "pgcat_hook_query";

    let mut request = BytesMut::new();

    if begin {
        request.put(crate::messages::simple_query("BEGIN"));
    }

    // Leftovers from a query that failed half way, if any. Closing what
    // doesn't exist isn't an error.
    request.put(close_message(b'P', NAME));
    request.put(close_message(b'S', NAME));

    let mut parse = BytesMut::new();
    parse.put_slice(NAME.as_bytes());
    parse.put_u8(0);
    parse.put_slice(sql.as_bytes());
    parse.put_u8(0);
    parse.put_i16(0);
    request.put(message(b'P', &parse));

    let mut bind = BytesMut::new();
    for _ in 0..2 {
        bind.put_slice(NAME.as_bytes());
        bind.put_u8(0);
    }
    bind.put_i16(0);
    bind.put_i16(params.len() as i16);
    for param in params {
        match param {
            Some(value) => {
                bind.put_i32(value.len() as i32);
                bind.put_slice(value.as_bytes());
            }
            None => bind.put_i32(-1),
        }
    }
    bind.put_i16(0);
    request.put(message(b'B', &bind));

    let mut execute = BytesMut::new();
    execute.put_slice(NAME.as_bytes());
    execute.put_u8(0);
    execute.put_i32(0);
    request.put(message(b'E', &execute));

    request.put(close_message(b'P', NAME));
    request.put(close_message(b'S', NAME));
    request.put(crate::messages::sync());

    server.send(&request).await?;

    let mut rows = Vec::new();
    let mut error = None;

    // The BEGIN's reply comes first. Had it failed, the query ran outside a
    // transaction: its result can't be used.
    if begin {
        loop {
            let response = server.recv(None).await?;
            for message in crate::messages::split_messages(&response)? {
                if message[0] == b'E' && error.is_none() {
                    error = Some(
                        crate::messages::PgErrorMsg::parse(&message[5..])
                            .map(|error| error.message)
                            .unwrap_or_default(),
                    );
                }
            }
            if !server.is_data_available() {
                break;
            }
        }
    }

    loop {
        let response = server.recv(None).await?;

        for message in crate::messages::split_messages(&response)? {
            match message[0] {
                b'D' => {
                    let row: DataRow = message[..].try_into()?;
                    rows.push(
                        row.values
                            .into_iter()
                            .map(|value| value.map(|value| String::from_utf8_lossy(&value).into()))
                            .collect(),
                    );
                }
                b'E' => {
                    error = Some(
                        crate::messages::PgErrorMsg::parse(&message[5..])
                            .map(|error| error.message)
                            .unwrap_or_default(),
                    );
                }
                _ => (),
            }
        }

        if !server.is_data_available() {
            break;
        }
    }

    match error {
        Some(error) => Err(Error::PluginError(format!(
            "plugin query failed: {}",
            error
        ))),
        None => Ok(rows),
    }
}

fn message(code: u8, body: &[u8]) -> BytesMut {
    let mut message = BytesMut::with_capacity(body.len() + 5);
    message.put_u8(code);
    message.put_i32(body.len() as i32 + 4);
    message.put_slice(body);
    message
}

fn close_message(target: u8, name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u8(target);
    body.put_slice(name.as_bytes());
    body.put_u8(0);
    message(b'C', &body)
}
