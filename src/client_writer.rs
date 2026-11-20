//! The client's end of its connection to pgcat.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{ready, Context, Poll};

use tokio::io::AsyncWrite;

/// Writes to the client, and notes whether the client has been sent a
/// ReadyForQuery since pgcat last forwarded its messages to a server. Until it
/// has, what pgcat forwarded may have run, whatever the client was told.
pub struct ClientWriter<T> {
    inner: T,
    /// Set when pgcat forwards the client's messages to a server, cleared once
    /// a whole ReadyForQuery is written to the client. Atomic because pgcat
    /// forwards through a shared reference to the client.
    unanswered: AtomicBool,
    frames: Frames,
}

impl<T> ClientWriter<T> {
    /// `inner` must be at a message boundary, as it is once startup is over.
    pub fn new(inner: T) -> ClientWriter<T> {
        ClientWriter {
            inner,
            unanswered: AtomicBool::new(false),
            frames: Frames::default(),
        }
    }

    /// pgcat is forwarding the client's messages to a server.
    pub fn forwarded(&self) {
        self.unanswered.store(true, Ordering::Relaxed);
    }

    /// pgcat forwarded something the client hasn't been sent a ReadyForQuery
    /// for since.
    pub fn unanswered(&self) -> bool {
        self.unanswered.load(Ordering::Relaxed)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ClientWriter<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = ready!(Pin::new(&mut this.inner).poll_write(cx, buf))?;
        if this.frames.ready_for_query_in(&buf[..written]) {
            *this.unanswered.get_mut() = false;
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// A message's type byte and length.
const HEADER: usize = 5;

/// Where the bytes written so far leave off in the messages they carry.
#[derive(Default)]
struct Frames {
    header: [u8; HEADER],
    /// How much of the current message's header has been written.
    header_len: usize,
    /// How much of the current message's body is still to come.
    body_left: usize,
}

impl Frames {
    /// Follow `bytes` through the messages they carry: whether a ReadyForQuery
    /// ends in them.
    fn ready_for_query_in(&mut self, mut bytes: &[u8]) -> bool {
        let mut ready = false;
        while !bytes.is_empty() {
            if self.header_len < HEADER {
                let n = (HEADER - self.header_len).min(bytes.len());
                self.header[self.header_len..self.header_len + n].copy_from_slice(&bytes[..n]);
                self.header_len += n;
                bytes = &bytes[n..];
                if self.header_len < HEADER {
                    break;
                }
                let len = i32::from_be_bytes([
                    self.header[1],
                    self.header[2],
                    self.header[3],
                    self.header[4],
                ]);
                self.body_left = usize::try_from(len).unwrap_or(0).saturating_sub(4);
            } else {
                let n = self.body_left.min(bytes.len());
                self.body_left -= n;
                bytes = &bytes[n..];
            }
            if self.body_left == 0 {
                ready |= self.header[0] == b'Z';
                self.header_len = 0;
            }
        }
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn message(code: u8, body: &[u8]) -> Vec<u8> {
        let mut message = vec![code];
        message.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        message.extend_from_slice(body);
        message
    }

    #[test]
    fn a_ready_for_query_is_found_however_the_bytes_are_split() {
        let mut bytes = message(b'C', b"INSERT 0 1\0");
        bytes.extend(message(b'2', b""));
        bytes.extend(message(b'Z', b"I"));
        for split in 0..=bytes.len() {
            let mut frames = Frames::default();
            let first = frames.ready_for_query_in(&bytes[..split]);
            let second = frames.ready_for_query_in(&bytes[split..]);
            assert_eq!((first, second), (split == bytes.len(), split < bytes.len()));
        }
    }

    #[test]
    fn a_z_inside_a_message_is_not_a_ready_for_query() {
        let mut frames = Frames::default();
        let row = message(b'd', &message(b'Z', b"I"));
        assert!(!frames.ready_for_query_in(&row));
        assert!(!frames.ready_for_query_in(&message(b'C', b"COPY 1\0")));
        assert!(frames.ready_for_query_in(&message(b'Z', b"I")));
    }

    #[tokio::test]
    async fn only_a_whole_ready_for_query_answers_what_was_forwarded() {
        let mut writer = ClientWriter::new(Vec::new());
        assert!(!writer.unanswered());
        writer.forwarded();
        let complete = message(b'C', b"INSERT 0 1\0");
        writer.write_all(&complete).await.unwrap();
        assert!(writer.unanswered());
        let ready = message(b'Z', b"I");
        writer.write_all(&ready[..HEADER]).await.unwrap();
        assert!(writer.unanswered());
        writer.write_all(&ready[HEADER..]).await.unwrap();
        assert!(!writer.unanswered());
        assert_eq!(writer.inner, [complete, ready].concat());
    }
}
