use crate::primitive::unseekable::Unseekable;

#[derive(Eq, PartialEq)]
pub enum Domain {
    Key,
    Range,
}

impl Domain {
    pub fn short_name(&self) -> &str {
        match self {
            Domain::Key => { "K" }
            Domain::Range => { "R" }
        }
    }
}

pub trait Routable {
    fn domain(&self) -> Domain;
    fn to_unseekable(self) -> impl Unseekable;
    /// Deterministically select a key that intersects this Routable and the provided Ranges
    fn some_intersecting_routing_key(ranges: Ranges) -> RoutingKey;
}