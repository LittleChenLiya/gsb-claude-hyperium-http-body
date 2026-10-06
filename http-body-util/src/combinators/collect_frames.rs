use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Buf;
use futures_core::ready;
use http_body::Body;
use pin_project_lite::pin_project;

use crate::{
    util::BufList, CollectFramesError, CollectFramesErrorKind, FrameReceipt, FramesCollected,
};

pin_project! {
    /// Future that resolves into a [`FramesCollected`] by driving a body to
    /// end-of-stream while recording an explicit [`FrameReceipt`] for every
    /// observed event.
    ///
    /// Created by [`BodyExt::collect_frames`].
    ///
    /// [`FramesCollected`]: crate::FramesCollected
    /// [`BodyExt::collect_frames`]: crate::BodyExt::collect_frames
    #[derive(Debug)]
    pub struct CollectFrames<T>
    where
        T: Body,
    {
        #[pin]
        pub(crate) body: T,
        pub(crate) state: Option<State<T::Data>>,
    }
}

/// In-progress collection state.
#[derive(Debug)]
pub(crate) struct State<B> {
    receipts: Vec<FrameReceipt>,
    bufs: BufList<B>,
    trailers: Option<http::HeaderMap>,
    bytes_received: u64,
    saw_trailers: bool,
}

impl<B> State<B> {
    fn into_error<E>(self, kind: CollectFramesErrorKind<E>) -> CollectFramesError<E> {
        CollectFramesError {
            kind,
            bytes_received: self.bytes_received,
            receipts: self.receipts,
        }
    }
}

impl<T: Body> CollectFrames<T> {
    pub(crate) fn new(body: T) -> Self {
        // `SizeHint` is only an estimate: use it to pre-size the receipt log,
        // never to decide when the body is complete. Only the end-of-stream
        // signal (`Poll::Ready(None)`) completes collection, so a wrong hint
        // can neither truncate nor extend the collected body.
        let lower = body.size_hint().lower();
        let estimated_frames = lower.saturating_add(1).min(1024) as usize;
        CollectFrames {
            body,
            state: Some(State {
                receipts: Vec::with_capacity(estimated_frames),
                bufs: BufList::default(),
                trailers: None,
                bytes_received: 0,
                saw_trailers: false,
            }),
        }
    }
}

impl<T: Body> Future for CollectFrames<T> {
    type Output = Result<FramesCollected<T::Data>, CollectFramesError<T::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();

        loop {
            let frame = ready!(me.body.as_mut().poll_frame(cx));

            let frame = match frame {
                Some(frame) => frame,
                None => {
                    // End-of-stream: the only completeness marker.
                    let mut state = me.state.take().expect("polled after complete");
                    state.receipts.push(FrameReceipt::EndOfStream);
                    return Poll::Ready(Ok(FramesCollected {
                        receipts: state.receipts,
                        bufs: state.bufs,
                        trailers: state.trailers,
                    }));
                }
            };

            let frame = match frame {
                Ok(frame) => frame,
                Err(err) => {
                    let state = me.state.take().expect("polled after complete");
                    return Poll::Ready(Err(state.into_error(CollectFramesErrorKind::Body(err))));
                }
            };

            let frame = match frame.into_data() {
                Ok(data) => {
                    if me.state.as_ref().expect("polled after complete").saw_trailers {
                        // Strict policy: DATA after trailers is rejected.
                        let state = me.state.take().expect("polled after complete");
                        return Poll::Ready(Err(
                            state.into_error(CollectFramesErrorKind::DataAfterTrailers)
                        ));
                    }
                    let state = me.state.as_mut().expect("polled after complete");
                    let len = data.remaining();
                    state.bytes_received += len as u64;
                    // Empty DATA frames are recorded but are *not* treated as
                    // the end of the body.
                    state.receipts.push(FrameReceipt::Data { len });
                    // Only push this frame if it has some data in it, to avoid
                    // crashing on `BufList::push`.
                    if data.has_remaining() {
                        state.bufs.push(data);
                    }
                    continue;
                }
                Err(frame) => frame,
            };

            if let Ok(trailers) = frame.into_trailers() {
                if me.state.as_ref().expect("polled after complete").saw_trailers {
                    // Strict policy: repeated trailers are rejected.
                    let state = me.state.take().expect("polled after complete");
                    return Poll::Ready(Err(
                        state.into_error(CollectFramesErrorKind::RepeatedTrailers)
                    ));
                }
                let state = me.state.as_mut().expect("polled after complete");
                state.saw_trailers = true;
                state.receipts.push(FrameReceipt::Trailers {
                    fields: trailers.len(),
                });
                state.trailers = Some(trailers);
            }
        }
    }
}

/// Poll-fixture based tests.
///
/// Run with:
///
/// ```text
/// cargo test -p http-body-util collect_frames
/// ```
#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use http::HeaderMap;
    use http_body::{Body, Frame, SizeHint};

    use super::*;
    use crate::BodyExt;

    /// A poll fixture: a body driven manually, one scripted step per poll.
    ///
    /// Once the script is exhausted the fixture yields `Poll::Ready(None)`,
    /// i.e. end-of-stream.
    #[derive(Debug)]
    struct ScriptedBody {
        steps: VecDeque<Step>,
        hint: SizeHint,
    }

    #[derive(Debug)]
    enum Step {
        Frame(Frame<Bytes>),
        Err(&'static str),
        Pending,
    }

    impl ScriptedBody {
        fn new(steps: Vec<Step>) -> Self {
            ScriptedBody {
                steps: steps.into(),
                hint: SizeHint::new(),
            }
        }

        fn with_size_hint(mut self, hint: SizeHint) -> Self {
            self.hint = hint;
            self
        }
    }

    impl Body for ScriptedBody {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            match self.steps.pop_front() {
                Some(Step::Frame(frame)) => Poll::Ready(Some(Ok(frame))),
                Some(Step::Err(err)) => Poll::Ready(Some(Err(err))),
                Some(Step::Pending) => Poll::Pending,
                None => Poll::Ready(None),
            }
        }

        fn size_hint(&self) -> SizeHint {
            self.hint.clone()
        }
    }

    /// Drive a future to completion with a noop waker.
    fn block_on<F: Future + Unpin>(mut fut: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        loop {
            match Pin::new(&mut fut).poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => continue,
            }
        }
    }

    fn trailers() -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("checksum", "0xbeef".parse().unwrap());
        trailers
    }

    /// Empty DATA, real DATA, trailers and EOF each get their own receipt and
    /// the collection completes only on EOF.
    #[test]
    fn empty_data_real_data_trailers_and_eof_complete() {
        let body = ScriptedBody::new(vec![
            // An empty DATA frame is *not* the end of the body.
            Step::Frame(Frame::data(Bytes::new())),
            // Exercise a `Poll::Pending` round.
            Step::Pending,
            Step::Frame(Frame::data(Bytes::from_static(b"hello"))),
            Step::Frame(Frame::trailers(trailers())),
            // EOF is implicit once the script is exhausted.
        ]);

        let collected = block_on(body.collect_frames()).unwrap();

        assert_eq!(
            collected.receipts(),
            &[
                FrameReceipt::Data { len: 0 },
                FrameReceipt::Data { len: 5 },
                FrameReceipt::Trailers { fields: 1 },
                FrameReceipt::EndOfStream,
            ]
        );
        assert_eq!(collected.bytes_received(), 5);
        assert_eq!(collected.trailers(), Some(&trailers()));
        assert_eq!(collected.to_bytes(), &b"hello"[..]);
    }

    /// A body of only empty DATA frames completes (on EOF) with zero bytes.
    #[test]
    fn empty_data_alone_is_not_eof() {
        let body = ScriptedBody::new(vec![
            Step::Frame(Frame::data(Bytes::new())),
            Step::Frame(Frame::data(Bytes::new())),
        ]);

        let collected = block_on(body.collect_frames()).unwrap();

        assert_eq!(
            collected.receipts(),
            &[
                FrameReceipt::Data { len: 0 },
                FrameReceipt::Data { len: 0 },
                FrameReceipt::EndOfStream,
            ]
        );
        assert_eq!(collected.bytes_received(), 0);
        assert!(collected.trailers().is_none());
        assert_eq!(collected.to_bytes(), &b""[..]);
    }

    /// DATA after trailers is rejected by the strict policy; the received
    /// byte count is reported but the partial body is not delivered.
    #[test]
    fn data_after_trailers_rejected() {
        let body = ScriptedBody::new(vec![
            Step::Frame(Frame::data(Bytes::from_static(b"abc"))),
            Step::Frame(Frame::trailers(trailers())),
            Step::Frame(Frame::data(Bytes::from_static(b"nope"))),
        ]);

        let err = block_on(body.collect_frames()).unwrap_err();

        assert!(matches!(
            err.kind(),
            CollectFramesErrorKind::DataAfterTrailers
        ));
        assert_eq!(err.bytes_received(), 3);
        assert_eq!(
            err.receipts(),
            &[
                FrameReceipt::Data { len: 3 },
                FrameReceipt::Trailers { fields: 1 },
            ]
        );
    }

    /// A second trailers frame is rejected by the strict policy.
    #[test]
    fn repeated_trailers_rejected() {
        let body = ScriptedBody::new(vec![
            Step::Frame(Frame::trailers(trailers())),
            Step::Frame(Frame::trailers(HeaderMap::new())),
        ]);

        let err = block_on(body.collect_frames()).unwrap_err();

        assert!(matches!(
            err.kind(),
            CollectFramesErrorKind::RepeatedTrailers
        ));
        assert_eq!(err.bytes_received(), 0);
        assert_eq!(err.receipts(), &[FrameReceipt::Trailers { fields: 1 }]);
    }

    /// `SizeHint` is only an estimate: a wrong (too small) hint must not
    /// truncate the collected body.
    #[test]
    fn wrong_size_hint_does_not_truncate() {
        let body = ScriptedBody::new(vec![
            Step::Frame(Frame::data(Bytes::from_static(b"hello "))),
            Step::Frame(Frame::data(Bytes::from_static(b"world!"))),
        ])
        // Wrong on purpose: the body yields 12 bytes, not 2.
        .with_size_hint(SizeHint::with_exact(2));

        let collected = block_on(body.collect_frames()).unwrap();

        assert_eq!(collected.bytes_received(), 12);
        assert_eq!(collected.to_bytes(), &b"hello world!"[..]);
    }

    /// A body error reports the bytes received so far and does not deliver
    /// the partial body.
    #[test]
    fn body_error_reports_received_bytes() {
        let body = ScriptedBody::new(vec![
            Step::Frame(Frame::data(Bytes::from_static(b"abc"))),
            Step::Err("boom"),
        ]);

        let err = block_on(body.collect_frames()).unwrap_err();

        match err.kind() {
            CollectFramesErrorKind::Body(source) => assert_eq!(*source, "boom"),
            other => panic!("unexpected error kind: {:?}", other),
        }
        assert_eq!(err.bytes_received(), 3);
        assert_eq!(err.receipts(), &[FrameReceipt::Data { len: 3 }]);
    }
}
