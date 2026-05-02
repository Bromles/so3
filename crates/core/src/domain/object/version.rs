#[derive(Debug)]
pub struct ObjectVersion(i64);

impl ObjectVersion {
    pub fn initial() -> Self {
        Self(1)
    }
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}
