use std::fmt;

use bytes::Buf;
use http::HeaderMap;

use crate::util::BufList;

/// An explicit receipt recorded for each event observed while driving a body
/// to completion with [`BodyExt::collect_frames`].
///
/// DATA frames, trailers frames and end-of-stream are attributed separately,
/// so callers can audit exactly what the body produced.
///
/// [`BodyExt::collect_frames`]: crate::BodyExt::collect_frames
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameReceipt {
    /// A DATA frame was received.
    ///
    /// Empty DATA frames are recorded with a length of zero. An empty DATA
    /// frame is *not* treated as the end of the body.
    Data {
        /// Length of the received DATA frame, in bytes.
        len: usize,
    },
    /// A trailers frame was received.
    Trailers {
        /// Number of header fields the received trailers frame carried.
        fields: usize,
    },
    /// The body signaled end-of-stream by returning `Poll::Ready(None)`.
    ///
    /// This is the only event that marks a body as complete. Neither an empty
    /// DATA frame nor the body's [`http_body::SizeHint`] is treated as a
    /// completeness signal.
    EndOfStream,
}

/// The result of successfully driving a body to end-of-stream with
/// [`BodyExt::collect_frames`].
///
/// Contains the aggregated DATA frames, the trailers (if any) and the
/// explicit [`FrameReceipt`] log of every observed event. The receipt log
/// always ends with [`FrameReceipt::EndOfStream`], the only completeness
/// marker.
///
/// [`BodyExt::collect_frames`]: crate::BodyExt::collect_frames
#[derive(Debug)]
pub struct FramesCollected<B> {
    pub(crate) receipts: Vec<FrameReceipt>,
    pub(crate) bufs: BufList<B>,
    pub(crate) trailers: Option<HeaderMap>,
}

impl<B: Buf> FramesCollected<B> {
    /// Returns the explicit receipt log recorded while collecting.
    ///
    /// The last entry is always [`FrameReceipt::EndOfStream`].
    pub fn receipts(&self) -> &[FrameReceipt] {
        &self.receipts
    }

    /// Returns the total number of DATA bytes received from the body.
    pub fn bytes_received(&self) -> u64 {
        self.receipts
            .iter()
            .map(|receipt| match receipt {
                FrameReceipt::Data { len } => *len as u64,
                _ => 0,
            })
            .sum()
    }

    /// If there is a trailers frame buffered, returns a reference to it.
    ///
    /// Returns `None` if the body contained no trailers.
    pub fn trailers(&self) -> Option<&HeaderMap> {
        self.trailers.as_ref()
    }

    /// If there is a trailers frame buffered, remove and return it.
    ///
    /// Returns `None` if the body contained no trailers.
    pub fn take_trailers(&mut self) -> Option<HeaderMap> {
        self.trailers.take()
    }

    /// Aggregate the buffered DATA frames into a [`Buf`].
    pub fn aggregate(self) -> impl Buf {
        self.bufs
    }

    /// Convert the buffered DATA frames into [`bytes::Bytes`].
    pub fn to_bytes(mut self) -> bytes::Bytes {
        self.bufs.copy_to_bytes(self.bufs.remaining())
    }
}

/// Error returned by [`BodyExt::collect_frames`] when collection fails.
///
/// The error always reports how many DATA bytes had been received before the
/// failure, together with the receipts recorded so far. The partially
/// collected body is *not* delivered.
///
/// [`BodyExt::collect_frames`]: crate::BodyExt::collect_frames
#[derive(Debug)]
pub struct CollectFramesError<E> {
    pub(crate) kind: CollectFramesErrorKind<E>,
    pub(crate) bytes_received: u64,
    pub(crate) receipts: Vec<FrameReceipt>,
}

/// The reason a [`CollectFramesError`] occurred.
#[derive(Debug)]
pub enum CollectFramesErrorKind<E> {
    /// The body itself returned an error.
    Body(E),
    /// A DATA frame was received after a trailers frame, which the declared
    /// strict collection policy rejects.
    DataAfterTrailers,
    /// A second trailers frame was received, which the declared strict
    /// collection policy rejects.
    RepeatedTrailers,
}

impl<E> CollectFramesError<E> {
    /// Returns the reason collection failed.
    pub fn kind(&self) -> &CollectFramesErrorKind<E> {
        &self.kind
    }

    /// Consumes the error and returns the reason collection failed.
    pub fn into_kind(self) -> CollectFramesErrorKind<E> {
        self.kind
    }

    /// Returns the number of DATA bytes received before the failure.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received
    }

    /// Returns the receipts recorded before the failure.
    pub fn receipts(&self) -> &[FrameReceipt] {
        &self.receipts
    }
}

impl<E: fmt::Display> fmt::Display for CollectFramesError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            CollectFramesErrorKind::Body(err) => write!(
                f,
                "body failed after {} received bytes: {}",
                self.bytes_received, err
            ),
            CollectFramesErrorKind::DataAfterTrailers => {
                write!(f, "received DATA frame after trailers")
            }
            CollectFramesErrorKind::RepeatedTrailers => {
                write!(f, "received a second trailers frame")
            }
        }
    }
}

impl<E> std::error::Error for CollectFramesError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            CollectFramesErrorKind::Body(err) => Some(err),
            _ => None,
        }
    }
}
