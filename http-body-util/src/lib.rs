#![deny(missing_debug_implementations, missing_docs, unreachable_pub)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Utilities for [`http_body::Body`].
//!
//! [`BodyExt`] adds extensions to the common trait.
//!
//! [`Empty`] and [`Full`] provide simple implementations.

mod collected;
pub mod combinators;
mod either;
mod frame_receipt;
mod empty;
mod error;
mod full;
mod future;
mod limited;
mod pending;
mod stream;

#[cfg(feature = "channel")]
pub mod channel;

mod util;

use self::combinators::{BoxBody, MapErr, MapFrame, UnsyncBoxBody};

pub use self::collected::Collected;
pub use self::either::Either;
pub use self::empty::Empty;
pub use self::error::ErrorBody;
pub use self::frame_receipt::{
    CollectFramesError, CollectFramesErrorKind, FrameReceipt, FramesCollected,
};
pub use self::full::Full;
pub use self::future::TryFutureBody;
pub use self::limited::{LengthLimitError, Limited};
pub use self::pending::Pending;
pub use self::stream::{BodyDataStream, BodyStream, StreamBody};

#[cfg(feature = "channel")]
pub use self::channel::Channel;

/// An extension trait for [`http_body::Body`] adding various combinators and adapters
pub trait BodyExt: http_body::Body {
    /// Returns a future that resolves to the next [`Frame`], if any.
    ///
    /// [`Frame`]: combinators::Frame
    fn frame(&mut self) -> combinators::Frame<'_, Self>
    where
        Self: Unpin,
    {
        combinators::Frame(self)
    }

    /// Maps this body's frame to a different kind.
    fn map_frame<F, B>(self, f: F) -> MapFrame<Self, F>
    where
        Self: Sized,
        F: FnMut(http_body::Frame<Self::Data>) -> http_body::Frame<B>,
        B: bytes::Buf,
    {
        MapFrame::new(self, f)
    }

    /// A body that calls a function with a reference to each frame before yielding it.
    fn inspect_frame<F>(self, f: F) -> combinators::InspectFrame<Self, F>
    where
        Self: Sized,
        F: FnMut(&http_body::Frame<Self::Data>),
    {
        combinators::InspectFrame::new(self, f)
    }

    /// Maps this body's error value to a different value.
    fn map_err<F, E>(self, f: F) -> MapErr<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Error) -> E,
    {
        MapErr::new(self, f)
    }

    /// A body that calls a function with a reference to an error before yielding it.
    fn inspect_err<F>(self, f: F) -> combinators::InspectErr<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Error),
    {
        combinators::InspectErr::new(self, f)
    }

    /// Turn this body into a boxed trait object.
    fn boxed(self) -> BoxBody<Self::Data, Self::Error>
    where
        Self: Sized + Send + Sync + 'static,
    {
        BoxBody::new(self)
    }

    /// Turn this body into a boxed trait object that is !Sync.
    fn boxed_unsync(self) -> UnsyncBoxBody<Self::Data, Self::Error>
    where
        Self: Sized + Send + 'static,
    {
        UnsyncBoxBody::new(self)
    }

    /// Turn this body into a [`Limited`] body with the given limit.
    fn limited(self, limit: usize) -> Limited<Self>
    where
        Self: Sized,
    {
        Limited::new(self, limit)
    }

    /// Turn this body into [`Collected`] body which will collect all the DATA frames
    /// and trailers.
    fn collect(self) -> combinators::Collect<Self>
    where
        Self: Sized,
    {
        combinators::Collect {
            body: self,
            collected: Some(crate::Collected::default()),
        }
    }

    /// Turn this body into a future that collects all frames into a
    /// [`FramesCollected`], recording an explicit [`FrameReceipt`] for every
    /// observed event.
    ///
    /// Unlike [`BodyExt::collect`], this combinator attributes DATA frames,
    /// trailers and end-of-stream separately:
    ///
    /// - Every DATA frame (including empty ones) is recorded as
    ///   [`FrameReceipt::Data`]. An empty DATA frame is *not* treated as the
    ///   end of the body.
    /// - Trailers are recorded as [`FrameReceipt::Trailers`]. A second
    ///   trailers frame, or any DATA frame after trailers, is rejected with a
    ///   [`CollectFramesError`] per the declared strict policy.
    /// - Only end-of-stream (`Poll::Ready(None)`) completes the collection,
    ///   recorded as [`FrameReceipt::EndOfStream`]. [`http_body::SizeHint`] is
    ///   used solely as an allocation estimate and never as a completeness
    ///   signal, so a wrong hint cannot truncate the body.
    ///
    /// On failure the returned [`CollectFramesError`] reports how many bytes
    /// had been received; the partially collected body is not delivered.
    ///
    /// # Example
    ///
    /// ```
    /// use bytes::Bytes;
    /// use http_body_util::{BodyExt, FrameReceipt, Full};
    ///
    /// # async fn doc() {
    /// let collected = Full::new(Bytes::from_static(b"hello"))
    ///     .collect_frames()
    ///     .await
    ///     .unwrap();
    ///
    /// assert_eq!(
    ///     collected.receipts(),
    ///     &[FrameReceipt::Data { len: 5 }, FrameReceipt::EndOfStream],
    /// );
    /// assert_eq!(collected.to_bytes(), &b"hello"[..]);
    /// # }
    /// ```
    ///
    /// [`FramesCollected`]: crate::FramesCollected
    /// [`FrameReceipt`]: crate::FrameReceipt
    /// [`CollectFramesError`]: crate::CollectFramesError
    fn collect_frames(self) -> combinators::CollectFrames<Self>
    where
        Self: Sized,
    {
        combinators::CollectFrames::new(self)
    }

    /// Add trailers to the body.
    ///
    /// The trailers will be sent when all previous frames have been sent and the `trailers` future
    /// resolves.
    ///
    /// # Example
    ///
    /// ```
    /// use http::HeaderMap;
    /// use http_body_util::{Full, BodyExt};
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// async fn main() {
    /// let (tx, rx) = tokio::sync::oneshot::channel::<HeaderMap>();
    ///
    /// let body = Full::<Bytes>::from("Hello, World!")
    ///     // add trailers via a future
    ///     .with_trailers(async move {
    ///         match rx.await {
    ///             Ok(trailers) => Some(Ok(trailers)),
    ///             Err(_err) => None,
    ///         }
    ///     });
    ///
    /// // compute the trailers in the background
    /// tokio::spawn(async move {
    ///     let _ = tx.send(compute_trailers().await);
    /// });
    ///
    /// async fn compute_trailers() -> HeaderMap {
    ///     // ...
    ///     # unimplemented!()
    /// }
    /// # }
    /// ```
    fn with_trailers<F>(self, trailers: F) -> combinators::WithTrailers<Self, F>
    where
        Self: Sized,
        F: std::future::Future<Output = Option<Result<http::HeaderMap, Self::Error>>>,
    {
        combinators::WithTrailers::new(self, trailers)
    }

    /// Turn this body into [`BodyStream`].
    fn into_stream(self) -> BodyStream<Self>
    where
        Self: Sized,
    {
        BodyStream::new(self)
    }

    /// Turn this body into [`BodyDataStream`].
    fn into_data_stream(self) -> BodyDataStream<Self>
    where
        Self: Sized,
    {
        BodyDataStream::new(self)
    }

    /// Creates a "fused" body.
    ///
    /// This [`Body`][http_body::Body] yields `Poll::Ready(None)` forever after the underlying
    /// body yields `Poll::Ready(None)`, or an error `Poll::Ready(Some(Err(_)))`, once.
    ///
    /// See [`Fuse<B>`][combinators::Fuse] for more information.
    fn fuse(self) -> combinators::Fuse<Self>
    where
        Self: Sized,
    {
        combinators::Fuse::new(self)
    }

    /// Takes two bodies and creates a new body "chaining" them together.
    ///
    /// Similar to [`std::iter::Iterator::chain()`], this method will return a new
    /// [`Body`][http_body::Body] that emits the contents of the first body, and then emits the
    /// contents of the second body.
    ///
    /// If the first body returns an error, the body will consider itself finished and will not
    /// poll the second body.
    ///
    /// Trailers yielded by the first body are buffered while the second body is polled, and then
    /// merged with any trailers yielded by the second body via [`http::HeaderMap::extend()`].
    /// Header values from the second body take precedence in the event of any conflicts.
    ///
    /// # Examples
    ///
    /// ```
    /// # use bytes::Bytes;
    /// # use http_body_util::{BodyExt, Full};
    /// #
    /// #[tokio::main]
    /// async fn main() {
    ///     let first = Full::new(Bytes::from("hello "));
    ///     let second = Full::new(Bytes::from("world!"));
    ///     let chained = first.chain(second);
    ///
    ///     let collected = chained.collect().await.unwrap();
    ///     assert_eq!(collected.to_bytes(), "hello world!");
    /// }
    /// ```
    ///
    /// ```
    /// # use bytes::Bytes;
    /// # use http::{HeaderMap, HeaderName, HeaderValue};
    /// # use http_body_util::{BodyExt, Full, Empty};
    /// #
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut trailers = HeaderMap::new();
    ///     trailers.insert(
    ///         HeaderName::from_static("name"),
    ///         HeaderValue::from_static("value"),
    ///     );
    ///     let trailers = std::future::ready(Some(Ok(trailers)));
    ///
    ///     let first = Full::new(Bytes::from("trailers"));
    ///     let second = Full::new(Bytes::from(" too!"));
    ///     let chained = first.with_trailers(trailers).chain(second);
    ///
    ///     let collected = chained.collect().await.unwrap();
    ///     assert_eq!(collected.trailers().unwrap()["name"], "value");
    ///     assert_eq!(collected.to_bytes(), "trailers too!");
    /// }
    /// ```
    fn chain<B>(self, other: B) -> combinators::Chain<Self, B>
    where
        Self: Sized,
    {
        combinators::Chain::new(self, other)
    }
}

impl<T: ?Sized> BodyExt for T where T: http_body::Body {}
