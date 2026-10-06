//! Example: collect a five-frame body with explicit frame receipts.
//!
//! Run with:
//!
//! ```text
//! cargo run -p http-body-util --example collect_frames
//! ```

use bytes::Bytes;
use http::HeaderMap;
use http_body::Frame;
use http_body_util::{BodyExt, FrameReceipt, StreamBody};

#[tokio::main]
async fn main() {
    let mut trailers = HeaderMap::new();
    trailers.insert("checksum", "0xbeef".parse().unwrap());

    // A body of exactly five frames. Note that the second frame is an *empty*
    // DATA frame: it is recorded but is not treated as the end of the body.
    // Completion is signaled only by end-of-stream after the fifth frame.
    let frames = vec![
        Frame::data(Bytes::from_static(b"hello ")),
        Frame::data(Bytes::new()),
        Frame::data(Bytes::from_static(b"world")),
        Frame::data(Bytes::from_static(b"!")),
        Frame::trailers(trailers),
    ];
    let body = StreamBody::new(futures_util::stream::iter(
        frames.into_iter().map(Ok::<_, std::convert::Infallible>),
    ));

    let collected = body.collect_frames().await.expect("collection failed");

    for receipt in collected.receipts() {
        match receipt {
            FrameReceipt::Data { len } => println!("receipt: DATA frame ({} bytes)", len),
            FrameReceipt::Trailers { fields } => {
                println!("receipt: trailers ({} fields)", fields)
            }
            FrameReceipt::EndOfStream => println!("receipt: end-of-stream (body complete)"),
        }
    }

    // Five frames were observed, plus the end-of-stream receipt.
    assert_eq!(collected.receipts().len(), 6);
    assert_eq!(collected.bytes_received(), 12);
    assert_eq!(
        collected.trailers().map(|t| &t["checksum"]),
        Some(&"0xbeef".parse().unwrap())
    );
    assert_eq!(collected.to_bytes(), &b"hello world!"[..]);

    println!("body: hello world! (12 bytes received)");
}
