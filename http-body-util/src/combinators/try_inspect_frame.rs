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

    /// Get a reference to the inner body, if it has not been terminated yet.
    pub fn get_ref(&self) -> Option<&B> {
        self.inner.as_ref()
    }

    /// Get a mutable reference to the inner body, if it has not been terminated yet.
    pub fn get_mut(&mut self) -> Option<&mut B> {
        self.inner.as_mut()
    }

    /// Get a pinned mutable reference to the inner body, if it has not been terminated yet.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> Option<Pin<&mut B>> {
        self.project().inner.as_pin_mut()
    }

    /// Consume `self`, returning the inner body, if it has not been terminated yet.
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
            // Terminated: the observer rejected a frame earlier.
            None => return Poll::Ready(None),
        };
        match inner.poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(Some(Ok(frame))) => match (this.f)(&frame) {
                Ok(()) => Poll::Ready(Some(Ok(frame))),
                Err(cause) => {
                    // The observer rejected the frame: drop the inner body,
                    // yield the error once, and terminate the stream.
                    this.inner.set(None);
                    Poll::Ready(Some(Err(cause)))
                }
            },
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Some(inner) => inner.size_hint(),
            None => SizeHint::with_exact(0),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Some(inner) => inner.is_end_stream(),
            None => true,
        }
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
    use bytes::Bytes;
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    type Error = &'static str;

    /// A manually driven body that records when it is dropped.
    struct MockBody {
        frames: VecDeque<Result<Frame<Bytes>, Error>>,
        dropped: Rc<Cell<bool>>,
    }

    impl MockBody {
        fn new(
            frames: impl IntoIterator<Item = Result<Frame<Bytes>, Error>>,
        ) -> (Self, Rc<Cell<bool>>) {
            let dropped = Rc::new(Cell::new(false));
            (
                Self {
                    frames: frames.into_iter().collect(),
                    dropped: dropped.clone(),
                },
                dropped,
            )
        }
    }

    impl Drop for MockBody {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    impl Body for MockBody {
        type Data = Bytes;
        type Error = Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.get_mut().frames.pop_front())
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.frames.len() as u64)
        }
    }

    fn data(frame: &'static [u8]) -> Result<Frame<Bytes>, Error> {
        Ok(Frame::data(Bytes::from_static(frame)))
    }

    fn unwrap_ready<T>(poll: Poll<T>) -> T {
        match poll {
            Poll::Ready(t) => t,
            Poll::Pending => panic!("pending"),
        }
    }

    #[test]
    fn rejects_frame_and_terminates() {
        let (body, dropped) = MockBody::new([data(b"one"), data(b"two"), data(b"three")]);
        let mut body = TryInspectFrame::new(body, |frame: &Frame<Bytes>| {
            // reject the second frame
            if frame.data_ref() == Some(&Bytes::from_static(b"two")) {
                Err("rejected")
            } else {
                Ok(())
            }
        });

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // The first frame passes the observer and is delivered as-is.
        assert_eq!(body.size_hint().exact(), Some(3));
        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Ok(frame)) => assert_eq!(frame.into_data().unwrap(), "one"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }

        // The second frame is rejected: it is not delivered to the consumer,
        // the original cause is yielded as the body's error exactly once,
        // and the inner body is dropped.
        assert!(!dropped.get());
        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Err("rejected")) => {}
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        assert!(dropped.get());
        assert!(body.get_ref().is_none());

        // The body is terminated: it yields `None` forever and reports a
        // zero size hint.
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(body.is_end_stream());
        assert!(unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)).is_none());
        assert!(unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)).is_none());
    }

    #[test]
    fn passes_frames_and_trailers_through() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("foo", http::HeaderValue::from_static("bar"));

        let (body, dropped) = MockBody::new([
            data(b"one"),
            data(b"two"),
            Ok(Frame::trailers(trailers.clone())),
        ]);
        let mut body =
            TryInspectFrame::new(body, |_: &Frame<Bytes>| -> Result<(), Error> { Ok(()) });

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Successful frames are delivered unchanged.
        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Ok(frame)) => assert_eq!(frame.into_data().unwrap(), "one"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Ok(frame)) => assert_eq!(frame.into_data().unwrap(), "two"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }

        // Trailers pass through unchanged as well.
        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Ok(frame)) => assert_eq!(frame.into_trailers().unwrap(), trailers),
            other => panic!("unexpected poll outcome: {:?}", other),
        }

        assert!(unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)).is_none());
        drop(body);
        assert!(dropped.get());
    }

    #[test]
    fn inner_errors_pass_through() {
        let (body, dropped) = MockBody::new([data(b"one"), Err("inner error"), data(b"three")]);
        let mut body =
            TryInspectFrame::new(body, |_: &Frame<Bytes>| -> Result<(), Error> { Ok(()) });

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Ok(frame)) => assert_eq!(frame.into_data().unwrap(), "one"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Err("inner error")) => {}
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        // Errors from the inner body do not terminate the combinator.
        assert!(!dropped.get());
        match unwrap_ready(Pin::new(&mut body).poll_frame(&mut cx)) {
            Some(Ok(frame)) => assert_eq!(frame.into_data().unwrap(), "three"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
    }
}
