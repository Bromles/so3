use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;

use crate::domain::error::So3Result;

pub struct BlobStream(Pin<Box<dyn Stream<Item = So3Result<Bytes>> + Send + 'static>>);

impl BlobStream {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = So3Result<Bytes>> + Send + 'static,
    {
        Self(Box::pin(stream))
    }
}

impl Stream for BlobStream {
    type Item = So3Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().0.as_mut().poll_next(cx)
    }
}
