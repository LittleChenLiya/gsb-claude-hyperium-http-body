use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use futures_core::ready;
use http::HeaderMap;
use http_body::Body;
use pin_project_lite::pin_project;

use crate::util::BufList;

/// The kind of error that caused a [`CollectReceipt`] to fail.
///
/// [`CollectReceipt`]: crate::combinators::CollectReceipt
#[derive(Debug)]
pub enum CollectErrorKind<E> {
    /// The underlying body returned an error.
    Body(E),
    /// A trailers frame was received after a trailers frame had already been
    /// received.
    DuplicateTrailers,
    /// A data frame was received after a trailers frame had already been
    /// received.
    DataAfterTrailers,
}

/// An error returned when [`CollectReceipt`] fails.
///
/// This error carries the number of bytes received before the failure, but
/// does not deliver the complete body.
///
/// [`CollectReceipt`]: crate::combinators::CollectReceipt
#[derive(Debug)]
pub struct CollectError<E> {
    received: usize,
    kind: CollectErrorKind<E>,
}

impl<E> CollectError<E> {
    /// Returns the number of data bytes received before the failure.
    pub fn received(&self) -> usize {
        self.received
    }

    /// Returns the kind of error that caused the failure.
    pub fn kind(&self) -> &CollectErrorKind<E> {
        &self.kind
    }

    /// Consumes the error, returning the underlying body error if the failure
    /// was caused by the body.
    pub fn into_body_error(self) -> Option<E> {
        match self.kind {
            CollectErrorKind::Body(e) => Some(e),
            _ => None,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for CollectError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            CollectErrorKind::Body(e) => {
                write!(f, "body error after {} bytes: {}", self.received, e)
            }
            CollectErrorKind::DuplicateTrailers => {
                write!(f, "duplicate trailers after {} bytes", self.received)
            }
            CollectErrorKind::DataAfterTrailers => {
                write!(f, "data after trailers after {} bytes", self.received)
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CollectError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            CollectErrorKind::Body(e) => Some(e),
            _ => None,
        }
    }
}

/// A collected body produced by [`BodyExt::collect_receipt`].
///
/// Unlike [`Collected`], a `Receipt` tracks data, trailers, and EOF as distinct
/// concerns. It is the successful result of a strict collection that only
/// completes when the underlying body signals EOF via [`Body::poll_frame`]
/// returning `None`.
///
/// [`BodyExt::collect_receipt`]: crate::BodyExt::collect_receipt
/// [`Collected`]: crate::Collected
#[derive(Debug)]
pub struct Receipt<B> {
    bufs: BufList<B>,
    trailers: Option<HeaderMap>,
    eof: bool,
    received: usize,
}

impl<B: Buf> Receipt<B> {
    /// Returns the trailers frame, if one was received.
    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.as_ref()
    }

    /// Removes and returns the trailers frame, if one was received.
    pub fn take_trailers(&mut self) -> Option<HeaderMap> {
        self.trailers.take()
    }

    /// Returns whether the underlying body reached EOF.
    ///
    /// This is the only true completeness signal: a `Receipt` is only
    /// returned with `true` after [`Body::poll_frame`] has returned `None`.
    pub fn is_eof(&self) -> bool {
        self.eof
    }

    /// Returns the number of data bytes received.
    pub fn received(&self) -> usize {
        self.received
    }

    /// Aggregates the collected data into a [`Buf`].
    pub fn aggregate(self) -> impl Buf {
        self.bufs
    }

    /// Converts the collected data into [`Bytes`].
    pub fn to_bytes(mut self) -> Bytes {
        self.bufs.copy_to_bytes(self.bufs.remaining())
    }
}

impl<B> Default for Receipt<B> {
    fn default() -> Self {
        Self {
            bufs: BufList::default(),
            trailers: None,
            eof: false,
            received: 0,
        }
    }
}

impl<B> Unpin for Receipt<B> {}

pin_project! {
    /// Future that collects a body into a [`Receipt`] with strict frame
    /// validation.
    ///
    /// This combinator differs from [`Collect`] in the following ways:
    ///
    /// - Data, trailers, and EOF are tracked as distinct concerns in the
    ///   [`Receipt`].
    /// - Empty data frames do **not** end the stream; collection continues
    ///   until [`Body::poll_frame`] returns `None`.
    /// - Duplicate trailers frames are rejected.
    /// - Data frames received after a trailers frame are rejected.
    /// - `size_hint` is never used as a completeness signal; only EOF is.
    /// - On failure, the received byte count is returned but no body is
    ///   delivered.
    ///
    /// [`Collect`]: crate::combinators::Collect
    pub struct CollectReceipt<T>
    where
        T: Body,
        T: ?Sized,
    {
        bufs: BufList<T::Data>,
        trailers: Option<HeaderMap>,
        received: usize,
        eof: bool,
        #[pin]
        body: T,
    }
}

impl<T: Body + ?Sized> CollectReceipt<T> {
    pub(crate) fn new(body: T) -> Self
    where
        T: Sized,
    {
        Self {
            bufs: BufList::default(),
            trailers: None,
            received: 0,
            eof: false,
            body,
        }
    }
}

impl<T: Body + ?Sized> Future for CollectReceipt<T> {
    type Output = Result<Receipt<T::Data>, CollectError<T::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();

        loop {
            match ready!(me.body.as_mut().poll_frame(cx)) {
                Some(Ok(frame)) => {
                    if frame.is_data() {
                        let data = frame.into_data().map_err(|_| ()).unwrap();
                        let len = data.remaining();

                        if me.trailers.is_some() {
                            return Poll::Ready(Err(CollectError {
                                received: *me.received,
                                kind: CollectErrorKind::DataAfterTrailers,
                            }));
                        }

                        *me.received += len;

                        // An empty data frame is still a data frame; it does
                        // not signal EOF. Only `poll_frame` returning `None`
                        // does that.
                        if len > 0 {
                            me.bufs.push(data);
                        }
                    } else if frame.is_trailers() {
                        if me.trailers.is_some() {
                            return Poll::Ready(Err(CollectError {
                                received: *me.received,
                                kind: CollectErrorKind::DuplicateTrailers,
                            }));
                        }

                        *me.trailers = Some(frame.into_trailers().map_err(|_| ()).unwrap());
                    } else {
                        // Unknown frame kind; ignore it but keep polling.
                    }
                }
                Some(Err(e)) => {
                    return Poll::Ready(Err(CollectError {
                        received: *me.received,
                        kind: CollectErrorKind::Body(e),
                    }));
                }
                None => {
                    // EOF reached. This is the *only* true completeness
                    // signal. `size_hint` and `is_end_stream` are hints only
                    // and must not be used to short-circuit collection.
                    *me.eof = true;

                    return Poll::Ready(Ok(Receipt {
                        bufs: std::mem::take(me.bufs),
                        trailers: me.trailers.take(),
                        eof: true,
                        received: *me.received,
                    }));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::{Infallible, TryInto};

    use bytes::{Buf, Bytes};
    use futures_util::stream;
    use futures_util::StreamExt;
    use http::HeaderMap;
    use http_body::Frame;

    use crate::{BodyExt, Full, StreamBody};

    use super::*;

    /// A poll fixture that drives a body frame-by-frame using a noop waker,
    /// recording each poll outcome.
    ///
    /// This is the poll fixture referenced in the acceptance criteria: it
    /// lets tests assert exactly how a body behaves when polled manually,
    /// without an async runtime.
    ///
    /// Run all poll fixture tests with:
    ///
    /// ```sh
    /// cargo test -p http-body-util -- poll_fixture
    /// ```
    struct PollFixture<B> {
        body: Pin<Box<B>>,
        outcomes: Vec<PollOutcome>,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum PollOutcome {
        Data(usize),
        Trailers,
        Error,
        Eof,
        Pending,
    }

    impl<B: Body> PollFixture<B> {
        fn new(body: B) -> Self {
            Self {
                body: Box::pin(body),
                outcomes: Vec::new(),
            }
        }

        fn poll_once(&mut self) -> PollOutcome {
            let waker = futures_util::task::noop_waker();
            let mut cx = Context::from_waker(&waker);

            match self.body.as_mut().poll_frame(&mut cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if frame.is_data() {
                        let len = match frame.into_data() {
                            Ok(data) => data.remaining(),
                            Err(_) => unreachable!("is_data was true"),
                        };
                        self.outcomes.push(PollOutcome::Data(len));
                        PollOutcome::Data(len)
                    } else if frame.is_trailers() {
                        self.outcomes.push(PollOutcome::Trailers);
                        PollOutcome::Trailers
                    } else {
                        self.outcomes.push(PollOutcome::Data(0));
                        PollOutcome::Data(0)
                    }
                }
                Poll::Ready(Some(Err(_))) => {
                    self.outcomes.push(PollOutcome::Error);
                    PollOutcome::Error
                }
                Poll::Ready(None) => {
                    self.outcomes.push(PollOutcome::Eof);
                    PollOutcome::Eof
                }
                Poll::Pending => {
                    self.outcomes.push(PollOutcome::Pending);
                    PollOutcome::Pending
                }
            }
        }

        fn outcomes(&self) -> &[PollOutcome] {
            &self.outcomes
        }
    }

    /// Build a five-frame body: data, empty data, data, trailers, and then
    /// EOF. This is the five-frame example used to exercise the poll fixture
    /// and the strict collector.
    fn five_frame_body() -> StreamBody<
        impl futures_core::Stream<Item = Result<Frame<Bytes>, std::convert::Infallible>>,
    > {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "value".try_into().unwrap());

        let frames = vec![
            Frame::data(Bytes::from("one")),
            Frame::data(Bytes::from("")),
            Frame::data(Bytes::from("three")),
            Frame::trailers(trailers),
        ];

        StreamBody::new(stream::iter(frames).map(Ok))
    }

    #[tokio::test]
    async fn collect_receipt_completes_on_eof() {
        let body = five_frame_body();
        let receipt = body.collect_receipt().await.unwrap();

        assert!(receipt.is_eof(), "EOF must be the complete flag");
        assert_eq!(receipt.received(), "one".len() + "three".len());
        assert_eq!(receipt.trailers().unwrap()["x-trailer"], "value");
        assert_eq!(receipt.to_bytes(), "onethree");
    }

    #[tokio::test]
    async fn empty_data_does_not_end_stream() {
        let frames = vec![
            Frame::data(Bytes::from("")),
            Frame::data(Bytes::from("")),
            Frame::data(Bytes::from("real")),
        ];
        let body = StreamBody::new(stream::iter(frames).map(Ok::<_, Infallible>));
        let receipt = body.collect_receipt().await.unwrap();

        assert!(receipt.is_eof());
        assert_eq!(receipt.received(), "real".len());
        assert_eq!(receipt.to_bytes(), "real");
    }

    #[tokio::test]
    async fn data_after_trailers_is_rejected() {
        let mut trailers = HeaderMap::new();
        trailers.insert("x", "y".try_into().unwrap());

        let frames = vec![
            Frame::data(Bytes::from("before")),
            Frame::trailers(trailers),
            Frame::data(Bytes::from("after")),
        ];
        let body = StreamBody::new(stream::iter(frames).map(Ok::<_, Infallible>));

        let err = body.collect_receipt().await.unwrap_err();
        assert_eq!(err.received(), "before".len());
        assert!(matches!(err.kind(), CollectErrorKind::DataAfterTrailers));
    }

    #[tokio::test]
    async fn duplicate_trailers_are_rejected() {
        let mut t1 = HeaderMap::new();
        t1.insert("a", "1".try_into().unwrap());
        let mut t2 = HeaderMap::new();
        t2.insert("b", "2".try_into().unwrap());

        let frames = vec![
            Frame::data(Bytes::from("data")),
            Frame::trailers(t1),
            Frame::trailers(t2),
        ];
        let body = StreamBody::new(stream::iter(frames).map(Ok::<_, Infallible>));

        let err = body.collect_receipt().await.unwrap_err();
        assert_eq!(err.received(), "data".len());
        assert!(matches!(err.kind(), CollectErrorKind::DuplicateTrailers));
    }

    #[tokio::test]
    async fn wrong_size_hint_does_not_truncate() {
        // A body whose size_hint claims 0 bytes but actually yields data.
        struct WrongHintBody {
            data: Option<Bytes>,
        }

        impl Body for WrongHintBody {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(self.data.take().map(|d| Ok(Frame::data(d))))
            }

            fn size_hint(&self) -> http_body::SizeHint {
                http_body::SizeHint::with_exact(0)
            }
        }

        let body = WrongHintBody {
            data: Some(Bytes::from("actual data")),
        };
        let receipt = body.collect_receipt().await.unwrap();

        assert!(receipt.is_eof());
        assert_eq!(receipt.received(), "actual data".len());
        assert_eq!(receipt.to_bytes(), "actual data");
    }

    /// Five-frame example driven by the poll fixture.
    ///
    /// Run with:
    ///
    /// ```sh
    /// cargo test -p http-body-util -- poll_fixture_five_frame_example
    /// ```
    #[test]
    fn poll_fixture_five_frame_example() {
        let mut fixture = PollFixture::new(five_frame_body());

        assert_eq!(fixture.poll_once(), PollOutcome::Data(3));
        assert_eq!(fixture.poll_once(), PollOutcome::Data(0));
        assert_eq!(fixture.poll_once(), PollOutcome::Data(5));
        assert_eq!(fixture.poll_once(), PollOutcome::Trailers);
        assert_eq!(fixture.poll_once(), PollOutcome::Eof);

        assert_eq!(
            fixture.outcomes(),
            &[
                PollOutcome::Data(3),
                PollOutcome::Data(0),
                PollOutcome::Data(5),
                PollOutcome::Trailers,
                PollOutcome::Eof,
            ]
        );
    }

    #[test]
    fn poll_fixture_empty_body() {
        let mut fixture = PollFixture::new(Full::<Bytes>::default());
        assert_eq!(fixture.poll_once(), PollOutcome::Eof);
    }
}
