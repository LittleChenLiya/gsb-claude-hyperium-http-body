//! A sequential, single-consumer entry point for an owned [`Body`].

use std::pin::Pin;
use std::task::{Context, Poll};

use http_body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;

pin_project! {
    /// A sequential consumer that exclusively polls an owned [`Body`].
    ///
    /// `Consumer` takes ownership of a body and becomes its single consumer. It
    /// guarantees that:
    ///
    /// - the wrapped body is polled by this consumer only, in frame order;
    /// - the end of the stream, or an error, is reported exactly once -- after
    ///   either has been reported the consumer keeps returning
    ///   `Poll::Ready(None)` without polling the body again;
    /// - `Poll::Pending` is never counted as consumption; only frames actually
    ///   delivered to the caller increase [`Consumer::delivered`].
    ///
    /// Consumption can be stopped explicitly with [`Consumer::stop`], which
    /// returns the remaining body together with any frame that was handed back
    /// via [`Consumer::restore`] and therefore has not been delivered yet. The
    /// returned [`Stopped`] can be turned back into a `Consumer` with
    /// [`Stopped::resume`] to continue where consumption left off, without
    /// repeating frames that were already delivered.
    ///
    /// Only owned bodies are supported; borrow the body elsewhere and move it
    /// in if shared access is needed.
    #[derive(Debug)]
    pub struct Consumer<B: Body> {
        #[pin]
        body: B,
        restored: Option<Frame<B::Data>>,
        terminated: bool,
        delivered: u64,
    }
}

/// The result of explicitly stopping a [`Consumer`].
///
/// Contains the remaining body, a frame that was restored but not yet
/// delivered again (if any), and the number of frames that were delivered
/// before stopping.
#[derive(Debug)]
pub struct Stopped<B: Body> {
    body: B,
    undelivered: Option<Frame<B::Data>>,
    delivered: u64,
}

impl<B: Body> Consumer<B> {
    /// Creates a new `Consumer` that takes ownership of `body`.
    pub fn new(body: B) -> Self {
        Self {
            body,
            restored: None,
            terminated: false,
            delivered: 0,
        }
    }

    /// Returns the number of frames delivered to the caller and not restored.
    ///
    /// `Poll::Pending` and the terminal end-of-stream or error report do not
    /// affect this count.
    pub fn delivered(&self) -> u64 {
        self.delivered
    }

    /// Returns `true` once the end of the stream or an error has been
    /// reported. A terminated consumer never polls the body again.
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    /// Attempts to pull out the next frame of this body.
    ///
    /// Frames restored with [`Consumer::restore`] are delivered again before
    /// the body is polled. Once the end of the stream or an error has been
    /// reported, this returns `Poll::Ready(None)` without polling the body.
    pub fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, B::Error>>> {
        let this = self.project();

        if let Some(frame) = this.restored.take() {
            *this.delivered += 1;
            return Poll::Ready(Some(Ok(frame)));
        }

        if *this.terminated {
            return Poll::Ready(None);
        }

        match this.body.poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(frame))) => {
                *this.delivered += 1;
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(err))) => {
                *this.terminated = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                *this.terminated = true;
                Poll::Ready(None)
            }
        }
    }

    /// Hands a previously delivered frame back to the consumer.
    ///
    /// The frame is no longer counted as delivered and will be delivered
    /// again by the next call to [`Consumer::poll_frame`], or returned as
    /// undelivered by [`Consumer::stop`].
    ///
    /// # Panics
    ///
    /// Panics if no frame has been delivered, or if another restored frame is
    /// still waiting to be delivered.
    pub fn restore(&mut self, frame: Frame<B::Data>) {
        assert!(self.delivered > 0, "no delivered frame to restore");
        assert!(
            self.restored.is_none(),
            "a restored frame is already pending"
        );
        self.delivered -= 1;
        self.restored = Some(frame);
    }

    /// Stops consumption explicitly.
    ///
    /// Returns the remaining body together with any restored frame that has
    /// not been delivered again and the number of delivered frames. The body
    /// is returned as-is; frames already delivered are not repeated when the
    /// body is consumed again.
    pub fn stop(self) -> Stopped<B> {
        Stopped {
            body: self.body,
            undelivered: self.restored,
            delivered: self.delivered,
        }
    }
}

impl<B: Body> Stopped<B> {
    /// Returns a reference to the remaining body.
    pub fn body(&self) -> &B {
        &self.body
    }

    /// Returns a reference to the restored frame that has not been delivered
    /// again, if any.
    pub fn undelivered(&self) -> Option<&Frame<B::Data>> {
        self.undelivered.as_ref()
    }

    /// Returns the number of frames delivered before stopping.
    pub fn delivered(&self) -> u64 {
        self.delivered
    }

    /// Splits into the remaining body and the undelivered frame, if any.
    pub fn into_parts(self) -> (B, Option<Frame<B::Data>>) {
        (self.body, self.undelivered)
    }

    /// Resumes sequential consumption of the remaining body.
    ///
    /// The undelivered frame, if any, is delivered first, so no frame is
    /// repeated or skipped.
    pub fn resume(self) -> Consumer<B> {
        Consumer {
            body: self.body,
            restored: self.undelivered,
            terminated: false,
            delivered: self.delivered,
        }
    }
}

impl<B> Body for Consumer<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Consumer::poll_frame(self, cx)
    }

    fn is_end_stream(&self) -> bool {
        self.restored.is_none() && (self.terminated || self.body.is_end_stream())
    }

    fn size_hint(&self) -> SizeHint {
        if self.terminated && self.restored.is_none() {
            SizeHint::with_exact(0)
        } else {
            self.body.size_hint()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;

    /// A value returned by a call to [`Body::poll_frame()`].
    type PollFrame = Poll<Option<Result<Frame<Bytes>, Error>>>;

    type Error = &'static str;

    /// A small scripted [`Body`] fixture.
    ///
    /// Each poll pops the next scripted outcome; once the script is exhausted
    /// the body reports the end of the stream. The number of times the body
    /// was polled is recorded so tests can assert that a terminated consumer
    /// never polls again.
    struct ScriptedBody {
        script: VecDeque<PollFrame>,
        polls: usize,
    }

    impl ScriptedBody {
        fn new(script: impl IntoIterator<Item = PollFrame>) -> Self {
            Self {
                script: script.into_iter().collect(),
                polls: 0,
            }
        }

        fn data(bytes: &'static [u8]) -> PollFrame {
            Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(bytes)))))
        }
    }

    impl Body for ScriptedBody {
        type Data = Bytes;
        type Error = Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();
            this.polls += 1;
            this.script.pop_front().unwrap_or(Poll::Ready(None))
        }

        fn is_end_stream(&self) -> bool {
            self.script.is_empty()
        }
    }

    fn with_cx<R>(f: impl FnOnce(&mut Context<'_>) -> R) -> R {
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        f(&mut cx)
    }

    fn poll(consumer: &mut Consumer<ScriptedBody>) -> PollFrame {
        with_cx(|cx| Pin::new(&mut *consumer).poll_frame(cx))
    }

    fn expect_data(poll: PollFrame) -> Bytes {
        match poll {
            Poll::Ready(Some(Ok(frame))) => frame.into_data().expect("data frame"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
    }

    #[test]
    fn stop_after_first_frame_resume_does_not_repeat_it() {
        let body = ScriptedBody::new([
            ScriptedBody::data(b"first"),
            ScriptedBody::data(b"second"),
        ]);
        let mut consumer = Consumer::new(body);

        assert_eq!(expect_data(poll(&mut consumer)), "first");
        assert_eq!(consumer.delivered(), 1);

        let stopped = consumer.stop();
        assert_eq!(stopped.delivered(), 1);
        assert!(stopped.undelivered().is_none());

        let mut consumer = stopped.resume();
        // The first frame must not be repeated.
        assert_eq!(expect_data(poll(&mut consumer)), "second");
        assert_eq!(consumer.delivered(), 2);
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
    }

    #[test]
    fn pending_is_not_counted_as_consumed() {
        let body = ScriptedBody::new([Poll::Pending, ScriptedBody::data(b"data")]);
        let mut consumer = Consumer::new(body);

        assert!(matches!(poll(&mut consumer), Poll::Pending));
        assert_eq!(consumer.delivered(), 0);

        assert_eq!(expect_data(poll(&mut consumer)), "data");
        assert_eq!(consumer.delivered(), 1);
    }

    #[test]
    fn error_is_reported_exactly_once() {
        let body = ScriptedBody::new([
            Poll::Ready(Some(Err("boom"))),
            ScriptedBody::data(b"unreachable"),
        ]);
        let mut consumer = Consumer::new(body);

        match poll(&mut consumer) {
            Poll::Ready(Some(Err("boom"))) => {}
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        assert!(consumer.is_terminated());
        assert_eq!(consumer.delivered(), 0);

        // The error is not reported again and the body is not polled again.
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert_eq!(consumer.body.polls, 1);
    }

    #[test]
    fn end_of_stream_is_reported_exactly_once() {
        let body = ScriptedBody::new([ScriptedBody::data(b"only")]);
        let mut consumer = Consumer::new(body);

        assert_eq!(expect_data(poll(&mut consumer)), "only");
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert!(consumer.is_terminated());

        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert_eq!(consumer.body.polls, 2);
    }

    #[test]
    fn stop_returns_restored_frame_as_undelivered() {
        let body = ScriptedBody::new([
            ScriptedBody::data(b"first"),
            ScriptedBody::data(b"second"),
        ]);
        let mut consumer = Consumer::new(body);

        let first = expect_data(poll(&mut consumer));
        assert_eq!(consumer.delivered(), 1);

        // The caller decides not to consume the frame and hands it back.
        consumer.restore(Frame::data(first));
        assert_eq!(consumer.delivered(), 0);

        let stopped = consumer.stop();
        let undelivered = stopped
            .undelivered()
            .expect("restored frame is returned as undelivered");
        assert_eq!(
            undelivered.data_ref().map(|d| &d[..]),
            Some(&b"first"[..])
        );

        // Resuming delivers the restored frame first, then continues in order.
        let mut consumer = stopped.resume();
        assert_eq!(expect_data(poll(&mut consumer)), "first");
        assert_eq!(expect_data(poll(&mut consumer)), "second");
        assert_eq!(consumer.delivered(), 2);
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
    }

    #[test]
    fn stop_into_parts_returns_remaining_body() {
        let body = ScriptedBody::new([
            ScriptedBody::data(b"first"),
            ScriptedBody::data(b"second"),
        ]);
        let mut consumer = Consumer::new(body);

        assert_eq!(expect_data(poll(&mut consumer)), "first");
        let (mut body, undelivered) = consumer.stop().into_parts();
        assert!(undelivered.is_none());

        // The remaining body continues where the consumer left off.
        let next = with_cx(|cx| Pin::new(&mut body).poll_frame(cx));
        match next {
            Poll::Ready(Some(Ok(frame))) => {
                assert_eq!(frame.into_data().expect("data"), "second")
            }
            other => panic!("unexpected poll outcome: {:?}", other),
        }
    }
}
