use crate::primitive::participants::Participants;
use crate::primitive::routable::Domain;
use crate::primitive::routables::{Routables, Slice};
use crate::primitive::seekable::Seekable;
use crate::primitive::unseekable::Unseekable;
use crate::primitive::unseekables::Unseekables;

pub trait Seekables<K: Seekable>: Routables<K> {
    fn slice<T: Seekable, U: Seekables<T>>(&self, ranges: Ranges) -> U {
        self.slice_from_slice(ranges, Slice::Overlapping)
    }

    fn intersecting<T: Seekable, L: Unseekable, U: Seekables<T>>(
        &self,
        intersecting: impl Unseekables<L>,
    ) -> U {
        self.intersecting_with_slice(intersecting, Slice::Overlapping)
    }

    fn slice_from_slice<T: Seekable, U: Seekables<T>>(&self, ranges: Ranges, slice: Slice) -> U;
    fn intersecting_with_slice<T: Seekable, L: Unseekable, U: Seekables<T>>(
        &self,
        intersecting: impl Unseekables<L>,
        slice: Slice,
    ) -> U;

    fn without(&self, ranges: Ranges) -> impl Seekables<K>;
    fn without_same<T: Seekable, U: Seekables<T>>(&self, without: U) -> impl Seekables<K>;
    fn with<T: Seekable, U: Seekables<T>>(&self, with: U) -> impl Seekables<K>;

    fn to_participants<T>(self) -> impl Participants<T>;
    fn to_route<T>(self, home_key: RoutingKey) -> impl FullRoute<T>;

    fn of<T: Seekable>(seekable: impl Seekable) -> impl Seekables<T> {
        if seekable.domain() == Domain::Range {
            Ranges::of(seekable.as_range())
        } else {
            Keys::of(seekable.as_key())
        }
    }
}
