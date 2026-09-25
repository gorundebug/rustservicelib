use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use servicelib::datasink::http::ResponseBody;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

struct GeneratedBody {
    remaining: usize,
    bytes_read: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    fail_at_end: bool,
}

impl AsyncRead for GeneratedBody {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining == 0 && self.fail_at_end {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "body connection reset",
            )));
        }
        let count = self.remaining.min(buffer.remaining()).min(1024);
        buffer.put_slice(&[b'x'; 1024][..count]);
        self.remaining -= count;
        self.bytes_read.fetch_add(count, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

impl Drop for GeneratedBody {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn body(size: usize, fail_at_end: bool) -> (ResponseBody, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let body = ResponseBody::new(GeneratedBody {
        remaining: size,
        bytes_read: bytes_read.clone(),
        drops: drops.clone(),
        fail_at_end,
    });
    (body, bytes_read, drops)
}

#[tokio::test]
async fn partial_read_does_not_drain_large_body() {
    let (mut body, bytes_read, drops) = body(64 * 1024 * 1024, false);
    assert_eq!(bytes_read.load(Ordering::SeqCst), 0);
    let mut prefix = [0; 4096];
    body.read_exact(&mut prefix).await.unwrap();
    assert_eq!(prefix, [b'x'; 4096]);
    assert_eq!(bytes_read.load(Ordering::SeqCst), 4096);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    body.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(bytes_read.load(Ordering::SeqCst), 4096);
    assert_eq!(
        body.read(&mut prefix).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(body);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn read_error_is_not_converted_to_successful_eof() {
    let (mut body, bytes_read, drops) = body(1500, true);
    let error = body.bytes().await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(error.to_string(), "body connection reset");
    assert_eq!(bytes_read.load(Ordering::SeqCst), 1500);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_pending_read_allows_later_read() {
    let (mut writer, reader) = tokio::io::duplex(16);
    let mut body = ResponseBody::new(reader);
    let mut buffer = [0; 4];
    {
        let mut pending = Box::pin(body.read(&mut buffer));
        assert!(futures::poll!(&mut pending).is_pending());
    }
    writer.write_all(b"next").await.unwrap();
    body.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"next");
    drop(writer);
    assert_eq!(body.read(&mut buffer).await.unwrap(), 0);
}

#[tokio::test]
async fn dropping_unread_body_releases_reader_without_reading() {
    let (body, bytes_read, drops) = body(64 * 1024 * 1024, false);
    drop(body);
    assert_eq!(bytes_read.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
