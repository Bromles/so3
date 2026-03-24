use serde::{Serialize, Serializer};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum So3Error {

}

impl Serialize for So3Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        self.to_string().serialize(serializer)
    }
}