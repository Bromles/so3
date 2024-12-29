use crate::primitive::routables::{Routables, Slice};
use crate::primitive::route::Route;
use crate::primitive::seekable::Seekable;
use crate::primitive::seekables::Seekables;
use crate::primitive::unseekable::Unseekable;
use crate::primitive::unseekables::UnseekablesKind::{
    FullKeyRoute, FullRangeRoute, RoutingKeys, RoutingRanges,
};

#[derive(Eq, PartialEq)]
enum UnseekablesKind {
    RoutingKeys,
    PartialKeyRoute,
    FullKeyRoute,
    RoutingRanges,
    PartialRangeRoute,
    FullRangeRoute,
}

impl UnseekablesKind {
    pub fn is_route(&self) -> bool {
        *self != RoutingKeys && *self != RoutingRanges
    }

    pub fn is_full_route(&self) -> bool {
        *self == FullKeyRoute || *self == FullRangeRoute
    }
}

pub trait Unseekables<K: Unseekable>: Routables<K> + Iterator {
    fn slice(&self, from: i32, to: i32) -> impl Unseekables<K>;
    fn slice_from_ranges(&self, ranges: Ranges) -> impl Unseekables<K>;
    fn slice_from_ranges_and_slice(&self, ranges: Ranges, slice: Slice) -> impl Unseekables<K>;

    fn intersecting_seekables<T: Seekable>(
        &self,
        intersecting: impl Seekables<T>,
    ) -> impl Unseekables<K>;
    fn intersecting_seekables_with_slice<T: Seekable>(
        &self,
        intersecting: impl Seekables<T>,
        slice: Slice,
    ) -> impl Unseekables<K>;
    fn intersecting_unseekables<T: Unseekable>(
        &self,
        intersecting: impl Unseekables<T>,
    ) -> impl Unseekables<K>;
    fn intersecting_unseekables_with_slice<T: Unseekable>(
        &self,
        intersecting: impl Unseekables<T>,
        slice: Slice,
    ) -> impl Unseekables<K>;

    fn without_ranges(&self, ranges: Ranges) -> impl Unseekables<K>;
    fn without_unseekables<T: Unseekable>(
        &self,
        subtract: impl Unseekables<T>,
    ) -> impl Unseekables<K>;

    fn with_unseekables(&self, with: impl Unseekables<K>) -> impl Unseekables<K>;
    fn with_key(&self, with_key: RoutingKey) -> impl Unseekables<K>;
    fn kind(&self) -> UnseekablesKind;

    fn merge(left: impl Unseekables<K>, right: impl Unseekables<K>) -> impl Unseekables<K> {
        let left_kind = left.kind();
        let right_kind = right.kind();

        if left_kind.is_route() || right_kind.is_route() {
            if left_kind.is_route() != right_kind.is_route() {
                if left_kind.is_route() && left.contains_all(right) {
                    return left;
                }
                if right_kind.is_route() && right.contains_all(left) {
                    return right;
                }

                return left.with(right);
            }

            if left_kind.is_full_route() {
                return left;
            }

            if right_kind.is_full_route() {
                return right;
            }

            return (left as Route).with(right as Route);
        }

        return left.with(right);
    }
}
