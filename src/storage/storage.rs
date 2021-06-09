use crate::storage::Result;

pub struct StoragePath {
    pub dir: String,
    pub file: String,
}

#[async_trait::async_trait]
pub trait Storage {
    // store should be idempotent
    async fn store(&self, path: &StoragePath, content_length: u64, data: &[u8]) -> Result<()>;
    async fn fetch(&self, path: &StoragePath, content_length: u64) -> Result<Vec<u8>>;
}
