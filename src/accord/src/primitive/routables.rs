use crate::primitive::routable::Routable;

pub enum Slice {
    Overlapping,
    Minimal,
    Maximal
}

pub trait Routables<K: Routable>: Iterator {
    
}
