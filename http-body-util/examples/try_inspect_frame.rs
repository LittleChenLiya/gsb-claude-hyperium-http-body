//! Demonstrates `BodyExt::try_inspect_frame`: a fallible frame observer that
//! terminates the body (dropping the inner one) when the observer rejects a
//! frame.
//!
//! Run with: `cargo run --example try_inspect_frame`

use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A manually driven body that announces when it is dropped.
struct MockBody {
    frames: VecDeque<Result<Frame<Bytes>, &'static str>>,
}

impl MockBody {
    fn new(frames: impl IntoIterator<Item = Result<Frame<Bytes>, &'static str>>) -> Self {
        Self {
            frames: frames.into_iter().collect(),
        }
    }
}

impl Body for MockBody {
    type Data = Bytes;
    type Error = &'static str;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
        Poll::Ready(self.frames.pop_front())
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.frames.len() as u64)
    }
}

impl Drop for MockBody {
    fn drop(&mut self) {
        println!("    [drop] inner body dropped ({} frame(s) left)", self.frames.len());
    }
}

fn data(frame: &'static [u8]) -> Result<Frame<Bytes>, &'static str> {
    Ok(Frame::data(Bytes::from_static(frame)))
}

fn poll_once<B: Body + Unpin>(body: &mut B) -> Option<Result<Frame<B::Data>, B::Error>> {
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match Pin::new(body).poll_frame(&mut cx) {
        Poll::Ready(item) => item,
        Poll::Pending => panic!("unexpected pending"),
    }
}

fn describe(frame: &Frame<Bytes>) -> String {
    if let Some(data) = frame.data_ref() {
        format!("data {:?}", String::from_utf8_lossy(data))
    } else {
        format!("trailers {:?}", frame.trailers_ref().unwrap())
    }
}

fn main() {
    println!("case 1: observer rejects the second of three frames");
    let body = MockBody::new([data(b"one"), data(b"two"), data(b"three")]);
    let mut body = body.try_inspect_frame(|frame: &Frame<Bytes>| {
        if frame.data_ref() == Some(&Bytes::from_static(b"two")) {
            Err("observer rejected frame `two`")
        } else {
            Ok(())
        }
    });
    println!("  size_hint before polling: {:?}", body.size_hint());
    for n in 1..=4 {
        match poll_once(&mut body) {
            Some(Ok(frame)) => println!("  poll {n}: delivered frame: {}", describe(&frame)),
            Some(Err(cause)) => println!("  poll {n}: error (original cause): {cause:?}"),
            None => println!("  poll {n}: None (terminated)"),
        }
    }
    println!("  size_hint after termination: {:?}", body.size_hint());
    println!("  is_end_stream after termination: {}", body.is_end_stream());
    drop(body);

    println!();
    println!("case 2: accepting observer lets frames and trailers through");
    let mut trailers = HeaderMap::new();
    trailers.insert("foo", http::HeaderValue::from_static("bar"));
    let body = MockBody::new([data(b"one"), data(b"two"), Ok(Frame::trailers(trailers))]);
    let mut body = body.try_inspect_frame(|_: &Frame<Bytes>| Ok(()));
    for n in 1..=4 {
        match poll_once(&mut body) {
            Some(Ok(frame)) => println!("  poll {n}: delivered frame: {}", describe(&frame)),
            Some(Err(cause)) => println!("  poll {n}: error: {cause:?}"),
            None => println!("  poll {n}: None (end of stream)"),
        }
    }
    drop(body);
}
