use bytes::Buf;
use http_body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;
use std::error::Error;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    /// A body that verifies the content length against a declared value.
    ///
    /// This body counts the bytes of successfully delivered DATA frames and
    /// compares the total against the declared content length:
    ///
    /// * If a DATA frame would push the total past the declared length, an
    ///   error is returned *instead of* that frame.
    /// * If the inner body ends before the declared length is reached, an
    ///   error is returned at end of stream.
    /// * Trailers frames are passed through and do not count as content.
    /// * Errors returned by the inner body are propagated unchanged, taking
    ///   priority over content length errors.
    #[derive(Clone, Copy, Debug)]
    pub struct ContentLength<B> {
        declared: u64,
        received: u64,
        #[pin]
        inner: B,
    }
}

impl<B> ContentLength<B> {
    /// Create a new `ContentLength` checking `inner` against `declared` bytes.
    pub fn new(inner: B, declared: u64) -> Self {
        Self {
            declared,
            received: 0,
            inner,
        }
    }
}

impl<B> Body for ContentLength<B>
where
    B: Body,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Data = B::Data;
    type Error = Box<dyn Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let res = match this.inner.poll_frame(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {
                if *this.received < *this.declared {
                    Some(Err(ContentLengthError::TooShort.into()))
                } else {
                    None
                }
            }
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let len = data.remaining() as u64;
                    if *this.received + len > *this.declared {
                        // Refuse to deliver the out-of-bounds frame.
                        Some(Err(ContentLengthError::TooLong.into()))
                    } else {
                        *this.received += len;
                        Some(Ok(frame))
                    }
                } else {
                    // Trailers are not content and are passed through.
                    Some(Ok(frame))
                }
            }
            // Inner errors take priority and are preserved as-is.
            Poll::Ready(Some(Err(err))) => Some(Err(err.into())),
        };

        Poll::Ready(res)
    }

    fn is_end_stream(&self) -> bool {
        // Never fabricate completion from the byte count alone; only the
        // inner body knows whether the stream has ended.
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        // Intersect the inner hint with the remaining declared length.
        let remaining = self.declared.saturating_sub(self.received);
        let inner = self.inner.size_hint();

        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower().min(remaining));
        match inner.upper() {
            Some(upper) => hint.set_upper(upper.min(remaining)),
            None => hint.set_upper(remaining),
        }
        hint
    }
}

/// An error returned when the body length does not match the declared
/// content length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContentLengthError {
    /// The body produced more content bytes than the declared length.
    TooLong,
    /// The body ended before producing the declared length.
    TooShort,
}

impl fmt::Display for ContentLengthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ContentLengthError::TooLong => f.write_str("content length exceeded declared length"),
            ContentLengthError::TooShort => {
                f.write_str("body ended before declared content length")
            }
        }
    }
}

impl Error for ContentLengthError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BodyExt, Full, StreamBody};
    use bytes::Bytes;
    use std::convert::Infallible;

    fn body_from_iter<I>(into_iter: I) -> impl Body<Data = Bytes, Error = Infallible>
    where
        I: IntoIterator,
        I::Item: Into<Bytes> + 'static,
        I::IntoIter: Send + 'static,
    {
        let iter = into_iter
            .into_iter()
            .map(|it| Frame::data(it.into()))
            .map(Ok::<_, Infallible>);

        StreamBody::new(futures_util::stream::iter(iter))
    }

    #[tokio::test]
    async fn exact_length_delivers_all_frames() {
        const DATA: [&[u8]; 2] = [b"ab", b"c"];
        let inner = body_from_iter(DATA);
        let body = &mut ContentLength::new(inner, 3);

        assert_eq!(body.size_hint().upper(), Some(3));

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, DATA[0]);
        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, DATA[1]);

        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn short_body_errors_at_eof() {
        const DATA: &[u8] = b"ab";
        let inner = Full::new(Bytes::from(DATA));
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, DATA);

        let err = body.frame().await.unwrap().unwrap_err();
        assert_eq!(err.downcast_ref(), Some(&ContentLengthError::TooShort));
    }

    #[tokio::test]
    async fn long_frame_errors_before_delivery() {
        const DATA: &[u8] = b"abcd";
        let inner = Full::new(Bytes::from(DATA));
        let body = &mut ContentLength::new(inner, 3);

        // The 4-byte frame exceeds the declared 3 bytes and must not be
        // delivered.
        let err = body.frame().await.unwrap().unwrap_err();
        assert_eq!(err.downcast_ref(), Some(&ContentLengthError::TooLong));
    }

    #[tokio::test]
    async fn long_second_frame_errors_before_delivery() {
        const DATA: [&[u8]; 2] = [b"abc", b"d"];
        let inner = body_from_iter(DATA);
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, DATA[0]);

        let err = body.frame().await.unwrap().unwrap_err();
        assert_eq!(err.downcast_ref(), Some(&ContentLengthError::TooLong));
    }

    #[derive(Debug)]
    struct InnerError;

    impl fmt::Display for InnerError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("inner error")
        }
    }

    impl Error for InnerError {}

    #[tokio::test]
    async fn inner_error_mid_stream_takes_priority() {
        // Declared 3 bytes; the body yields 2 bytes and then fails. The
        // inner error must be preserved instead of a `TooShort` error.
        let iter = vec![Ok(Frame::data(Bytes::from(&b"ab"[..]))), Err(InnerError)];
        let inner = StreamBody::new(futures_util::stream::iter(iter));
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, &b"ab"[..]);

        let err = body.frame().await.unwrap().unwrap_err();
        assert!(err.downcast_ref::<InnerError>().is_some());
        assert!(err.downcast_ref::<ContentLengthError>().is_none());
    }

    #[tokio::test]
    async fn trailers_do_not_count_as_content() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-trailer", "yes".parse().unwrap());

        let iter = vec![
            Ok::<_, Infallible>(Frame::data(Bytes::from(&b"abc"[..]))),
            Ok(Frame::trailers(trailers)),
        ];
        let inner = StreamBody::new(futures_util::stream::iter(iter));
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, &b"abc"[..]);

        let frame = body.frame().await.unwrap().unwrap();
        assert!(frame.is_trailers());

        // The stream ends cleanly: trailers did not affect the count.
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn size_hint_intersects_declared_length() {
        // Inner hint smaller than declared: inner wins.
        let inner = Full::new(Bytes::from(&b"abc"[..]));
        let body = ContentLength::new(inner, 5);
        assert_eq!(body.size_hint().lower(), 3);
        assert_eq!(body.size_hint().upper(), Some(3));

        // Inner hint larger than declared: declared remaining wins.
        let inner = Full::new(Bytes::from(&b"abcdef"[..]));
        let body = &mut ContentLength::new(inner, 3);
        assert_eq!(body.size_hint().upper(), Some(3));

        // The over-long frame errors before delivery. The inner body was
        // consumed by the poll, so the intersected hint reflects that the
        // inner body has nothing left rather than faking remaining content.
        let err = body.frame().await.unwrap().unwrap_err();
        assert_eq!(err.downcast_ref(), Some(&ContentLengthError::TooLong));
        assert_eq!(body.size_hint().upper(), Some(0));

        // Unknown inner upper bound is capped at the declared length.
        let inner = body_from_iter([b"ab" as &[u8]]);
        let body = ContentLength::new(inner, 3);
        assert_eq!(body.size_hint().upper(), Some(3));
    }

    #[tokio::test]
    async fn end_stream_is_not_faked_by_byte_count() {
        // Even after the declared number of bytes has been received, the
        // combinator must not claim the stream has ended on its own.
        let inner = body_from_iter([b"abc" as &[u8], b"d"]);
        let body = &mut ContentLength::new(inner, 3);
        assert!(!body.is_end_stream());

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, &b"abc"[..]);
        assert!(!body.is_end_stream());
    }
}
