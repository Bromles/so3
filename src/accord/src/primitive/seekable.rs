use crate::primitive::routable::Routable;

pub trait Seekable: Routable {
    fn as_key(&self) -> Key;
    fn as_range(&self) -> Range;
    fn slice(&self, range: Range) -> impl Seekable;
}