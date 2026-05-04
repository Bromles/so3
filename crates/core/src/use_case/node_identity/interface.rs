use async_trait::async_trait;
use uuid::Uuid;
use crate::domain::error::So3Result;

#[async_trait]
pub trait NodeIdentityUseCase {
    /// Resolves the stable node identity.
    ///
    /// - `None`: load from storage, or generate and persist if absent.
    /// - `Some(id)` with no stored id: persist `id` and return it.
    /// - `Some(id)` with matching stored id: return it.
    /// - `Some(id)` with a *different* stored id: error — changing node identity
    ///   while consensus state exists corrupts the command stream.
    async fn ensure(&self, configured: Option<Uuid>) -> So3Result<Uuid>;
}