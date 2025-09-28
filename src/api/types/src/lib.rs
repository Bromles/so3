use std::{io::Error, pin::Pin};

use bytes::Bytes;
use futures_core::Stream;

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;
