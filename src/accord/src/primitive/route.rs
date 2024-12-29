use crate::primitive::participants::Participants;
use crate::primitive::unseekable::Unseekable;

pub trait Route<K: Unseekable>: Participants<K> {
    fn home_key(&self) -> RoutingKey;
    fn home_key_only_route<T: Unseekable>(&self) -> impl Route<T>;

    fn is_home_key_only_route(&self) -> bool;
    fn is_route(&self) -> bool {
        true
    }
}
