use crate::primitive::unseekable::Unseekable;
use crate::primitive::unseekables::Unseekables;

pub trait Participants<K: Unseekable>: Unseekables<K> {}
