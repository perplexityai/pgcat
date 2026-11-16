//! Server errors and notices on pools whose rows hold protected values.
//!
//! Postgres quotes data in its diagnostics: a constraint violation's DETAIL
//! is "Failing row contains (...)", the whole row, protected columns
//! included. A statement pgcat lets through unplanned (an UPDATE of an
//! ordinary column) can therefore return a legacy row's plaintext to a caller
//! that may not read it. Parsing that text isn't reliable, so only fields
//! that never carry values reach the client: severity, SQLSTATE and the
//! names of the objects involved. The message becomes a fixed one.

use bytes::{BufMut, BytesMut};

/// Fields kept: severity (localized and not), SQLSTATE, and schema, table,
/// column, data type and constraint names. Any other field, one a later
/// Postgres adds included, is dropped.
const KEPT: &[u8] = b"SVCstcdn";

const WITHHELD: &str = "the server's message is withheld: it can quote protected values";

/// Withhold the text of every ErrorResponse and NoticeResponse in a reply of
/// whole messages. A reply without one is left as it is.
pub fn withhold_server_text(reply: &mut BytesMut) {
    if !frames(reply).any(|(code, _)| is_diagnostic(code)) {
        return;
    }

    let mut out = BytesMut::with_capacity(reply.len());
    for (code, frame) in frames(reply) {
        match is_diagnostic(code) {
            true => withhold(frame, &mut out),
            false => out.put_slice(frame),
        }
    }
    *reply = out;
}

fn is_diagnostic(code: u8) -> bool {
    code == b'E' || code == b'N'
}

/// Each message's code and its whole frame. Stops at a frame that runs past
/// the reply, which a reply of whole messages never has.
fn frames(reply: &[u8]) -> impl Iterator<Item = (u8, &[u8])> {
    let mut rest = reply;
    std::iter::from_fn(move || {
        let header = rest.get(..5)?;
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        let end = usize::try_from(len).ok()?.checked_add(1)?;
        let frame = rest.get(..end)?;
        rest = &rest[end..];
        Some((header[0], frame))
    })
}

fn withhold(frame: &[u8], out: &mut BytesMut) {
    let mut body = BytesMut::new();
    for field in frame[5..].split(|byte| *byte == 0) {
        if let Some((&kind, _)) = field.split_first() {
            if KEPT.contains(&kind) {
                body.put_slice(field);
                body.put_u8(0);
            }
        }
    }
    body.put_u8(b'M');
    body.put_slice(WITHHELD.as_bytes());
    body.put_u8(0);
    body.put_u8(0);

    out.put_u8(frame[0]);
    out.put_i32(body.len() as i32 + 4);
    out.put_slice(&body);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(code: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = vec![code];
        frame.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        frame.extend_from_slice(body);
        frame
    }

    fn diagnostic(code: u8, fields: &[(u8, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (kind, value) in fields {
            body.push(*kind);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        frame(code, &body)
    }

    #[test]
    fn test_errors_keep_only_fields_without_values() {
        let error = diagnostic(
            b'E',
            &[
                (b'S', "ERROR"),
                (b'V', "ERROR"),
                (b'C', "23514"),
                (
                    b'M',
                    "new row for relation \"t\" violates check constraint \"t_check\"",
                ),
                (b'D', "Failing row contains (1, secret)."),
                (b'W', "SQL statement \"UPDATE t SET a = 'secret'\""),
                (b't', "t"),
                (b'n', "t_check"),
            ],
        );
        let ready = frame(b'Z', b"I");
        let mut reply = BytesMut::from(&[error, ready.clone()].concat()[..]);

        withhold_server_text(&mut reply);

        let expected = diagnostic(
            b'E',
            &[
                (b'S', "ERROR"),
                (b'V', "ERROR"),
                (b'C', "23514"),
                (b't', "t"),
                (b'n', "t_check"),
                (b'M', WITHHELD),
            ],
        );
        assert_eq!(&reply[..], &[expected, ready].concat()[..]);
    }

    #[test]
    fn test_notices_are_withheld_and_other_messages_kept() {
        let complete = frame(b'C', b"UPDATE 1\0");
        let notice = diagnostic(b'N', &[(b'S', "NOTICE"), (b'C', "00000"), (b'M', "secret")]);
        let mut reply = BytesMut::from(&[complete.clone(), notice].concat()[..]);

        withhold_server_text(&mut reply);

        let expected = diagnostic(b'N', &[(b'S', "NOTICE"), (b'C', "00000"), (b'M', WITHHELD)]);
        assert_eq!(&reply[..], &[complete, expected].concat()[..]);
    }

    #[test]
    fn test_a_reply_without_diagnostics_is_untouched() {
        let reply = [frame(b'C', b"SELECT 1\0"), frame(b'Z', b"I")].concat();
        let mut withheld = BytesMut::from(&reply[..]);

        withhold_server_text(&mut withheld);

        assert_eq!(&withheld[..], &reply[..]);
    }
}
