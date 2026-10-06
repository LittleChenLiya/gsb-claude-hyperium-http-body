use http_body::{Body, Frame};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A sequential consumer for an owned [`Body`].
///
/// `Consumer` takes exclusive ownership of a body and is the only poller of
/// that body: frames are pulled in order, one at a time, and the end of the
/// stream or an error is reported to the caller exactly once. After
/// termination the consumer keeps returning `Poll::Ready(None)` without
/// polling the body again.
///
/// Consumption can be stopped explicitly with [`Consumer::stop`], which
/// returns the remaining body together with any frame that was pulled from
/// the body but not yet delivered to the caller. Consumption can later be
/// resumed with [`Consumer::resume`] without losing or repeating frames.
///
/// Only frames delivered to the caller are counted by
/// [`Consumer::consumed`]; `Poll::Pending` outcomes and
/// buffered-but-undelivered frames do not increase the count.
///
/// Only owned bodies are supported: the consumer stores the body by value
/// and requires `B: Unpin` for polling.
#[derive(Debug)]
pub struct Consumer<B: Body> {
    body: Option<B>,
    buffered: Option<Frame<B::Data>>,
    consumed: u64,
    terminated: bool,
}

impl<B: Body> Consumer<B> {
    /// Creates a new `Consumer` that sequentially consumes `body`.
    pub fn new(body: B) -> Self {
        Self {
            body: Some(body),
            buffered: None,
            consumed: 0,
            terminated: false,
        }
    }

    /// Resumes consumption from a remaining `body` and an optional
    /// not-yet-delivered `frame`, as returned by [`Consumer::stop`].
    ///
    /// The buffered `frame`, if any, is delivered by the next
    /// [`Consumer::poll_frame`] call before the body is polled again, so
    /// resuming neither loses nor repeats frames.
    pub fn resume(body: B, frame: Option<Frame<B::Data>>) -> Self {
        Self {
            body: Some(body),
            buffered: frame,
            consumed: 0,
            terminated: false,
        }
    }

    /// Returns the number of frames delivered to the caller so far.
    ///
    /// `Poll::Pending` outcomes and frames buffered with
    /// [`Consumer::poll_buffer`] but not yet delivered are not counted.
    pub fn consumed(&self) -> u64 {
        self.consumed
    }

    /// Returns `true` once the end of the stream or an error has been
    /// reported to the caller.
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    /// Stops consuming, returning the remaining body and any frame that was
    /// pulled from the body but not yet delivered to the caller.
    ///
    /// The returned parts can be passed to [`Consumer::resume`] to continue
    /// consumption later without losing or repeating frames.
    ///
    /// The body is returned as `None` if the stream has already terminated
    /// (the end of the stream or an error was reported), since such a body
    /// is exhausted or must be discarded.
    pub fn stop(mut self) -> (Option<B>, Option<Frame<B::Data>>) {
        (self.body.take(), self.buffered.take())
    }
}

impl<B> Consumer<B>
where
    B: Body + Unpin,
{
    /// Polls for the next frame of the body, delivering it to the caller.
    ///
    /// A frame previously buffered with [`Consumer::poll_buffer`] is
    /// delivered first, without polling the body. Otherwise the body is
    /// polled once:
    ///
    /// - `Poll::Pending` is forwarded as-is and does not count as consumed.
    /// - `Poll::Ready(Some(Ok(frame)))` delivers the frame and counts it as
    ///   consumed.
    /// - `Poll::Ready(Some(Err(error)))` and `Poll::Ready(None)` terminate
    ///   the consumer; the error or end of stream is reported exactly once.
    ///
    /// After termination this returns `Poll::Ready(None)` without polling
    /// the body again.
    pub fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, B::Error>>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        if let Some(frame) = self.buffered.take() {
            self.consumed += 1;
            return Poll::Ready(Some(Ok(frame)));
        }
        match self.poll_inner(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                self.consumed += 1;
                Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }

    /// Polls the body once and buffers the resulting frame without
    /// delivering it to the caller.
    ///
    /// The buffered frame is delivered by the next [`Consumer::poll_frame`]
    /// call or returned by [`Consumer::stop`]; until then it is not counted
    /// as consumed. If a frame is already buffered, it is kept and
    /// `Poll::Ready(Some(Ok(())))` is returned without polling the body.
    ///
    /// `Poll::Pending` is forwarded as-is and leaves the consumer
    /// unchanged. The end of the stream and errors are reported exactly
    /// once, as with [`Consumer::poll_frame`].
    pub fn poll_buffer(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), B::Error>>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        if self.buffered.is_some() {
            return Poll::Ready(Some(Ok(())));
        }
        match self.poll_inner(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(frame))) => {
                self.buffered = Some(frame);
                Poll::Ready(Some(Ok(())))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
        }
    }

    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, B::Error>>> {
        let body = self
            .body
            .as_mut()
            .expect("body is present until termination");
        match Pin::new(body).poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(err))) => {
                self.terminated = true;
                // A body that errored must be discarded.
                self.body = None;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                self.terminated = true;
                self.body = None;
                Poll::Ready(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Consumer;
    use bytes::Bytes;
    use futures_util::task::noop_waker;
    use http_body::{Body, Frame};
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::task::{Context, Poll};

    /// A small scripted [`Body`] fixture.
    ///
    /// Each poll pops one step from the script; once the script is
    /// exhausted the body yields `Poll::Ready(None)`. The number of polls
    /// is shared with the test so it can assert that the body is not
    /// polled again after termination.
    #[derive(Debug)]
    struct ScriptBody {
        script: VecDeque<Step>,
        polls: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    enum Step {
        Data(&'static str),
        Err(&'static str),
        Pending,
    }

    impl ScriptBody {
        fn new(script: Vec<Step>) -> (Self, Arc<AtomicUsize>) {
            let polls = Arc::new(AtomicUsize::new(0));
            let body = Self {
                script: script.into(),
                polls: polls.clone(),
            };
            (body, polls)
        }
    }

    impl Body for ScriptBody {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            match self.script.pop_front() {
                Some(Step::Data(data)) => Poll::Ready(Some(Ok(Frame::data(Bytes::from(data))))),
                Some(Step::Err(err)) => Poll::Ready(Some(Err(err))),
                Some(Step::Pending) => Poll::Pending,
                None => Poll::Ready(None),
            }
        }
    }

    fn poll(
        consumer: &mut Consumer<ScriptBody>,
    ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        consumer.poll_frame(&mut cx)
    }

    fn expect_data(poll: Poll<Option<Result<Frame<Bytes>, &'static str>>>) -> Bytes {
        match poll {
            Poll::Ready(Some(Ok(frame))) => frame.into_data().expect("a data frame"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
    }

    #[test]
    fn stop_after_first_frame_and_resume_does_not_repeat_it() {
        let (body, _) = ScriptBody::new(vec![Step::Data("a"), Step::Data("b")]);
        let mut consumer = Consumer::new(body);

        // Read the first frame, then stop explicitly.
        assert_eq!(expect_data(poll(&mut consumer)), Bytes::from("a"));
        assert_eq!(consumer.consumed(), 1);
        assert!(!consumer.is_terminated());

        let (body, buffered) = consumer.stop();
        assert!(buffered.is_none(), "no frame was left undelivered");
        let body = body.expect("the remaining body is returned on stop");

        // Resuming continues with the second frame; the first frame is not
        // delivered again.
        let mut consumer = Consumer::resume(body, buffered);
        assert_eq!(expect_data(poll(&mut consumer)), Bytes::from("b"));
        assert_eq!(consumer.consumed(), 1);
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert!(consumer.is_terminated());
    }

    #[test]
    fn stop_returns_frame_buffered_but_not_yet_delivered() {
        let (body, _) = ScriptBody::new(vec![Step::Data("a"), Step::Data("b")]);
        let mut consumer = Consumer::new(body);

        // Pull the first frame into the buffer without delivering it.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            consumer.poll_buffer(&mut cx),
            Poll::Ready(Some(Ok(())))
        ));
        // A buffered frame is not delivered yet, so it is not consumed.
        assert_eq!(consumer.consumed(), 0);

        // Stopping returns the remaining body and the undelivered frame.
        let (body, buffered) = consumer.stop();
        let buffered = buffered.expect("the undelivered frame is returned on stop");
        assert_eq!(buffered.data_ref(), Some(&Bytes::from("a")));
        let body = body.expect("the remaining body is returned on stop");

        // Resuming delivers the buffered frame first, then the rest of the
        // body: nothing is lost and nothing is repeated.
        let mut consumer = Consumer::resume(body, Some(buffered));
        assert_eq!(expect_data(poll(&mut consumer)), Bytes::from("a"));
        assert_eq!(consumer.consumed(), 1);
        assert_eq!(expect_data(poll(&mut consumer)), Bytes::from("b"));
        assert_eq!(consumer.consumed(), 2);
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
    }

    #[test]
    fn pending_is_not_counted_as_consumed() {
        let (body, polls) =
            ScriptBody::new(vec![Step::Pending, Step::Pending, Step::Data("a")]);
        let mut consumer = Consumer::new(body);

        assert!(matches!(poll(&mut consumer), Poll::Pending));
        assert!(matches!(poll(&mut consumer), Poll::Pending));
        assert_eq!(consumer.consumed(), 0, "pending is not consumed");
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(!consumer.is_terminated());

        assert_eq!(expect_data(poll(&mut consumer)), Bytes::from("a"));
        assert_eq!(consumer.consumed(), 1);
    }

    #[test]
    fn error_is_reported_only_once() {
        let (body, polls) =
            ScriptBody::new(vec![Step::Err("boom"), Step::Data("unreachable")]);
        let mut consumer = Consumer::new(body);

        match poll(&mut consumer) {
            Poll::Ready(Some(Err(err))) => assert_eq!(err, "boom"),
            other => panic!("unexpected poll outcome: {:?}", other),
        }
        assert!(consumer.is_terminated());

        // The error is reported once: further polls yield `None` and the
        // body is never polled again.
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert_eq!(polls.load(Ordering::SeqCst), 1);

        // An errored body is discarded, so nothing remains to return.
        let (body, buffered) = consumer.stop();
        assert!(body.is_none());
        assert!(buffered.is_none());
    }

    #[test]
    fn end_of_stream_is_reported_only_once() {
        let (body, polls) = ScriptBody::new(vec![Step::Data("a")]);
        let mut consumer = Consumer::new(body);

        assert_eq!(expect_data(poll(&mut consumer)), Bytes::from("a"));
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert!(consumer.is_terminated());

        // The end of stream is reported once: further polls yield `None`
        // without polling the body again.
        assert!(matches!(poll(&mut consumer), Poll::Ready(None)));
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }
}
