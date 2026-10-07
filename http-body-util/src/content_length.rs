use bytes::Buf;
use http_body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;
use std::error::Error;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    /// A body that checks the actual content length against a declared
    /// `Content-Length` value.
    ///
    /// This body counts the bytes of successfully delivered DATA frames
    /// (trailers do not count as content) and returns an error if the
    /// wrapped body does not match the declared length:
    ///
    /// * if a DATA frame would push the total past the declared length, an
    ///   error is returned instead of that frame;
    /// * if the wrapped body ends before the declared length is reached, an
    ///   error is returned at end of stream.
    ///
    /// Errors returned by the wrapped body are passed through unchanged.
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
                    Some(Err(ContentLengthError::too_short().into()))
                } else {
                    None
                }
            }
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let len = data.remaining() as u64;
                    if len > *this.declared - *this.received {
                        // The frame would exceed the declared length; report
                        // the error without delivering the out-of-bounds frame.
                        Some(Err(ContentLengthError::too_long().into()))
                    } else {
                        *this.received += len;
                        Some(Ok(frame))
                    }
                } else {
                    // Trailers are not content and do not count.
                    Some(Ok(frame))
                }
            }
            Poll::Ready(Some(Err(err))) => Some(Err(err.into())),
        };

        Poll::Ready(res)
    }

    fn is_end_stream(&self) -> bool {
        // The stream is only cleanly over once the declared length has
        // been received in full.
        self.received == self.declared && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        // Intersect the inner hint with the remaining declared bytes. The
        // declared length only bounds the upper side; it must not fabricate
        // an exact hint (i.e. pretend the body completes) that the wrapped
        // body cannot back up.
        let remaining = self.declared - self.received;
        let inner = self.inner.size_hint();

        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower().min(remaining));
        hint.set_upper(inner.upper().map_or(remaining, |upper| upper.min(remaining)));
        hint
    }
}

/// An error returned when the body length does not match the declared
/// content length.
#[derive(Debug)]
#[non_exhaustive]
pub struct ContentLengthError {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    TooLong,
    TooShort,
}

impl ContentLengthError {
    fn too_long() -> Self {
        Self { kind: Kind::TooLong }
    }

    fn too_short() -> Self {
        Self { kind: Kind::TooShort }
    }

    /// Returns true if the body produced more bytes than declared.
    pub fn is_too_long(&self) -> bool {
        matches!(self.kind, Kind::TooLong)
    }

    /// Returns true if the body ended before the declared length.
    pub fn is_too_short(&self) -> bool {
        matches!(self.kind, Kind::TooShort)
    }
}

impl fmt::Display for ContentLengthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            Kind::TooLong => f.write_str("body exceeds declared content length"),
            Kind::TooShort => f.write_str("body ended before declared content length"),
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
    async fn exact_length_body_delivers_all_frames() {
        const DATA: [&[u8]; 2] = [b"ab", b"c"];
        let inner = body_from_iter(DATA);
        let body = &mut ContentLength::new(inner, 3);

        assert_eq!(body.size_hint().upper(), Some(3));

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, DATA[0]);
        assert_eq!(body.size_hint().upper(), Some(1));

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, DATA[1]);
        assert_eq!(body.size_hint().upper(), Some(0));

        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn short_body_errors_at_end_of_stream() {
        let inner = Full::new(Bytes::from(&b"ab"[..]));
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(&data[..], b"ab");

        let error = body.frame().await.unwrap().unwrap_err();
        let error = error.downcast_ref::<ContentLengthError>().unwrap();
        assert!(error.is_too_short());
        assert!(!error.is_too_long());
    }

    #[tokio::test]
    async fn oversized_frame_errors_before_delivery() {
        let inner = Full::new(Bytes::from(&b"abcd"[..]));
        let body = &mut ContentLength::new(inner, 3);

        let error = body.frame().await.unwrap().unwrap_err();
        let error = error.downcast_ref::<ContentLengthError>().unwrap();
        assert!(error.is_too_long());
        assert!(!error.is_too_short());
    }

    #[tokio::test]
    async fn oversized_frame_after_partial_body_errors_before_delivery() {
        const DATA: [&[u8]; 2] = [b"ab", b"cd"];
        let inner = body_from_iter(DATA);
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, DATA[0]);

        let error = body.frame().await.unwrap().unwrap_err();
        assert!(error
            .downcast_ref::<ContentLengthError>()
            .unwrap()
            .is_too_long());
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
    async fn inner_error_is_preserved() {
        let iter = vec![
            Ok(Frame::data(Bytes::from(&b"ab"[..]))),
            Err(InnerError),
        ]
        .into_iter();
        let inner = StreamBody::new(futures_util::stream::iter(iter));
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(&data[..], b"ab");

        // The inner error takes priority over the (also unmet) length check.
        let error = body.frame().await.unwrap().unwrap_err();
        assert!(matches!(error.downcast_ref(), Some(InnerError)));
    }

    #[tokio::test]
    async fn trailers_do_not_count_towards_content_length() {
        let iter = vec![
            Ok::<_, Infallible>(Frame::data(Bytes::from(&b"abc"[..]))),
            Ok(Frame::<Bytes>::trailers(http::HeaderMap::new())),
        ]
        .into_iter();
        let inner = StreamBody::new(futures_util::stream::iter(iter));
        let body = &mut ContentLength::new(inner, 3);

        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(&data[..], b"abc");

        let frame = body.frame().await.unwrap().unwrap();
        assert!(frame.is_trailers());

        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn trailers_only_body_still_errors_when_short() {
        let iter = vec![Ok::<_, Infallible>(Frame::<Bytes>::trailers(http::HeaderMap::new()))].into_iter();
        let inner = StreamBody::new(futures_util::stream::iter(iter));
        let body = &mut ContentLength::new(inner, 3);

        let frame = body.frame().await.unwrap().unwrap();
        assert!(frame.is_trailers());

        let error = body.frame().await.unwrap().unwrap_err();
        assert!(error
            .downcast_ref::<ContentLengthError>()
            .unwrap()
            .is_too_short());
    }

    struct UnknownLength;

    impl Body for UnknownLength {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(None)
        }
    }

    #[test]
    fn size_hint_does_not_fabricate_exact_completion() {
        let body = ContentLength::new(UnknownLength, 3);
        let hint = body.size_hint();
        // The declared length bounds the upper side only; with an unknown
        // inner length no exact hint may be invented.
        assert_eq!(hint.lower(), 0);
        assert_eq!(hint.upper(), Some(3));
        assert_eq!(hint.exact(), None);
        assert!(!body.is_end_stream());
    }
}
