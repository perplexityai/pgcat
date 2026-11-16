//! Replies pgcat gives the client itself for messages of an extended protocol
//! batch that it doesn't send to the server: a Parse the server already has, a
//! Close of a named statement.
//!
//! Postgres answers a batch's messages in order, and clients match replies to
//! messages by position (pgx's pipelined batches do). So such a reply goes
//! after the server's replies to the messages sent before it, and not at all
//! once one of those failed: the server skips the rest of the batch until Sync.

use std::collections::VecDeque;

use bytes::{BufMut, BytesMut};

use crate::errors::Error;
use crate::messages::split_messages;

#[derive(Default)]
pub struct HeldReplies {
    /// Each reply, with the number of the batch's messages the server must
    /// have answered before it.
    held: VecDeque<(usize, BytesMut)>,

    /// Messages of the batch the server has answered in full.
    answered: usize,
}

impl HeldReplies {
    /// A new batch is being sent.
    pub fn clear(&mut self) {
        self.held.clear();
        self.answered = 0;
    }

    /// Send `reply` once the server has answered `after` of the batch's messages.
    pub fn hold(&mut self, after: usize, reply: BytesMut) {
        self.held.push_back((after, reply));
    }

    /// Put the held replies into a chunk of the server's reply, made of whole
    /// messages. A chunk can end anywhere between messages.
    pub fn splice(&mut self, response: BytesMut) -> Result<BytesMut, Error> {
        if self.held.is_empty() {
            return Ok(response);
        }

        let mut spliced = BytesMut::with_capacity(response.len());
        for message in split_messages(&response)? {
            let code = message[0];
            spliced.put(message);
            match code {
                // The last message of the reply to a Parse, Bind, Close,
                // Describe or Execute.
                b'1' | b'2' | b'3' | b'T' | b'n' | b'C' | b'I' | b's' => {
                    self.answered += 1;
                    let answered = self.answered;
                    while let Some((_, reply)) =
                        self.held.pop_front_if(|(after, _)| *after == answered)
                    {
                        spliced.put(reply);
                    }
                }
                b'E' | b'Z' => self.held.clear(),
                _ => (),
            }
        }
        Ok(spliced)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{close_complete, parse_complete};

    fn message(code: u8) -> BytesMut {
        let mut message = BytesMut::new();
        message.put_u8(code);
        message.put_i32(4);
        message
    }

    fn reply(codes: &[u8]) -> BytesMut {
        let mut reply = BytesMut::new();
        for code in codes {
            reply.put(message(*code));
        }
        reply
    }

    fn codes(reply: &[u8]) -> String {
        split_messages(reply)
            .unwrap()
            .iter()
            .map(|message| message[0] as char)
            .collect()
    }

    /// The server's reply to Bind, Execute (one row), Bind, Execute, Sync.
    const BATCH: &[u8] = b"2DC2DCZ";

    #[test]
    fn nothing_held_forwards_the_reply() {
        let mut held = HeldReplies::default();
        assert_eq!(&held.splice(reply(BATCH)).unwrap()[..], &reply(BATCH)[..]);
    }

    #[test]
    fn a_reply_follows_the_messages_before_it() {
        let mut held = HeldReplies::default();
        held.hold(2, parse_complete());
        held.hold(4, close_complete());
        assert_eq!(codes(&held.splice(reply(BATCH)).unwrap()), "2DC12DC3Z");
    }

    #[test]
    fn only_a_message_s_last_reply_counts() {
        // Describe of a statement (ParameterDescription, RowDescription), with
        // a notice and a parameter status in between.
        let mut held = HeldReplies::default();
        held.hold(1, parse_complete());
        assert_eq!(codes(&held.splice(reply(b"tNTSZ")).unwrap()), "tNT1SZ");
    }

    #[test]
    fn an_error_drops_the_replies_after_it() {
        let mut held = HeldReplies::default();
        held.hold(1, parse_complete());
        held.hold(2, close_complete());
        assert_eq!(codes(&held.splice(reply(b"2EZ")).unwrap()), "21EZ");
        assert!(held.held.is_empty());
    }

    #[test]
    fn chunks_split_anywhere_between_messages() {
        let whole = reply(BATCH);
        for split in 0..=BATCH.len() {
            let mut held = HeldReplies::default();
            held.hold(2, parse_complete());
            held.hold(4, close_complete());
            let mut spliced = held.splice(whole.clone().split_to(split * 5)).unwrap();
            spliced.put(held.splice(whole.clone().split_off(split * 5)).unwrap());
            assert_eq!(
                codes(&spliced),
                "2DC12DC3Z",
                "split after {} messages",
                split
            );
        }
    }

    #[test]
    fn a_new_batch_starts_counting_again() {
        let mut held = HeldReplies::default();
        held.hold(3, parse_complete());
        held.splice(reply(b"2")).unwrap();
        held.clear();
        held.hold(1, parse_complete());
        assert_eq!(codes(&held.splice(reply(b"2Z")).unwrap()), "21Z");
    }
}
