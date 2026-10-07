use http_body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;
use std::{
    any::type_name,
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

pin_project! {
    /// Body returned by the [`try_inspect_frame()`] combinator.
    ///
    /// The inner body is stored in an `Option` so that it can be dropped as
    /// soon as the observer rejects a frame. `None` is the terminated state.
    ///
    /// [`try_inspect_frame()`]: crate::BodyExt::try_inspect_frame
    #[derive(Clone, Copy)]
    pub struct TryInspectFrame<B, F> {
        #[pin]
        inner: Option<B>,
        f: F,
    }
}

impl<B, F> TryInspectFrame<B, F> {
    #[inline]
    pub(crate) fn new(body: B, f: F) -> Self {
        Self {
            inner: Some(body),
            f,
        }
    }

    /// Get a reference to the inner body, or `None` if this body has
    /// terminated after a rejected frame.
    pub fn get_ref(&self) -> Option<&B> {
        self.inner.as_ref()
    }

    /// Get a mutable reference to the inner body, or `None` if this body has
    /// terminated after a rejected frame.
    pub fn get_mut(&mut self) -> Option<&mut B> {
        self.inner.as_mut()
    }

    /// Get a pinned mutable reference to the inner body, or `None` if this
    /// body has terminated after a rejected frame.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> Option<Pin<&mut B>> {
        self.project().inner.as_pin_mut()
    }

    /// Consume `self`, returning the inner body, or `None` if this body has
    /// terminated after a rejected frame.
    pub fn into_inner(self) -> Option<B> {
        self.inner
    }
}

impl<B, F> Body for TryInspectFrame<B, F>
where
    B: Body,
    F: FnMut(&Frame<B::Data>) -> Result<(), B::Error>,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        let inner = match this.inner.as_mut().as_pin_mut() {
            Some(inner) => inner,
            // Terminated: a previous frame was rejected.
            None => return Poll::Ready(None),
        };
        match inner.poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(Some(Ok(frame))) => match (this.f)(&frame) {
                Ok(()) => Poll::Ready(Some(Ok(frame))),
                Err(cause) => {
                    // Rejected: do not yield the frame, yield the cause as an
                    // error exactly once, then terminate by dropping the
                    // inner body.
                    this.inner.set(None);
                    Poll::Ready(Some(Err(cause)))
                }
            },
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map(B::size_hint)
            .unwrap_or_else(|| SizeHint::with_exact(0))
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().map_or(true, B::is_end_stream)
    }
}

impl<B, F> fmt::Debug for TryInspectFrame<B, F>
where
    B: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TryInspectFrame")
            .field("inner", &self.inner)
            .field("f", &type_name::<F>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BodyExt;
    use bytes::Bytes;
    use http::HeaderMap;
    use std::collections::VecDeque;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    /// A manual body yielding a fixed sequence of frames, recording drops.
    struct MockBody {
        frames: VecDeque<Frame<Bytes>>,
        dropped: Arc<AtomicBool>,
    }

    impl MockBody {
        fn new(frames: impl IntoIterator<Item = Frame<Bytes>>, dropped: Arc<AtomicBool>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                dropped,
            }
        }
    }

    impl Drop for MockBody {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl Body for MockBody {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.frames.pop_front().map(Ok))
        }

        fn is_end_stream(&self) -> bool {
            self.frames.is_empty()
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.frames.len() as u64)
        }
    }

    fn data(frame: Frame<Bytes>) -> Bytes {
        frame.into_data().expect("data frame")
    }

    #[test]
    fn rejected_frame_errors_once_terminates_and_drops_inner() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mock = MockBody::new(
            ["one", "two", "three"]
                .iter()
                .map(|s| Frame::data(Bytes::from_static(s.as_bytes()))),
            dropped.clone(),
        );

        // Reject the second frame; the rejection cause is the yielded error.
        let mut calls = 0_u8;
        let mut body = mock.try_inspect_frame(move |frame| {
            calls += 1;
            if calls == 2 {
                assert_eq!(frame.data_ref().expect("data"), &b"two"[..]);
                Err("rejected")
            } else {
                Ok(())
            }
        });

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // size_hint is delegated before termination.
        assert_eq!(body.size_hint().lower(), 3);
        assert_eq!(body.size_hint().upper(), Some(3));
        assert!(!body.is_end_stream());

        // First frame is delivered to the consumer.
        let mut delivered = Vec::new();
        match Pin::new(&mut body).poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => delivered.push(data(frame)),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        assert!(!dropped.load(Ordering::SeqCst));

        // Second frame is rejected: the error is returned exactly once and
        // the rejected frame is not delivered.
        match Pin::new(&mut body).poll_frame(&mut cx) {
            Poll::Ready(Some(Err("rejected"))) => {}
            other => panic!("unexpected poll outcome: {:?}", other),
        }

        // The inner body has been dropped.
        assert!(dropped.load(Ordering::SeqCst));
        assert!(body.get_ref().is_none());

        // size_hint is zero after termination.
        assert_eq!(body.size_hint().lower(), 0);
        assert_eq!(body.size_hint().upper(), Some(0));
        assert!(body.is_end_stream());

        // Subsequent polls yield `None`; the third frame is never delivered.
        for _ in 0..2 {
            match Pin::new(&mut body).poll_frame(&mut cx) {
                Poll::Ready(None) => {}
                other => panic!("unexpected poll outcome: {:?}", other),
            }
        }

        assert_eq!(delivered, [Bytes::from_static(b"one")]);
    }

    #[test]
    fn accepted_frames_and_trailers_pass_through() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", http::HeaderValue::from_static("value"));
        let mock = MockBody::new(
            [
                Frame::data(Bytes::from_static(b"one")),
                Frame::data(Bytes::from_static(b"two")),
                Frame::trailers(trailers.clone()),
            ],
            dropped,
        );

        let mut body = mock.try_inspect_frame(|_| Ok(()));

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        match Pin::new(&mut body).poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => assert_eq!(data(frame), "one"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        match Pin::new(&mut body).poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => assert_eq!(data(frame), "two"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        // Trailers are passed through unchanged.
        match Pin::new(&mut body).poll_frame(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => {
                assert_eq!(frame.into_trailers().expect("trailers"), trailers)
            }
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        match Pin::new(&mut body).poll_frame(&mut cx) {
            Poll::Ready(None) => {}
            other => panic!("unexpected poll outcome: {:?}", other),
        }
    }
}
