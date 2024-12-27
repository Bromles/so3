use crate::primitive::routable::Routable;

pub trait Unseekable: Routable {
    fn as_range(&self) -> Range;
    fn as_routing_key(&self) -> RoutingKey;
}
