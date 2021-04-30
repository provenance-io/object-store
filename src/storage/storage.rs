use crate::storage::Result;

// TODO example existing path
// os/os/dds-local-dev/20210426/836db4b0-690a-4a9e-a3fa-0f7d029c5bff

// TODO implement checksum
// TODO how to handle race conditions where attacker can post two requests with same dime
// but different bodies and thus different hashes?

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
