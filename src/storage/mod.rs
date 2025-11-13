mod error;
mod file_system;
mod google_cloud;

use std::{path::PathBuf, sync::Arc};

// forwarding declarations
pub use error::*;
pub use file_system::FileSystem;
pub use google_cloud::GoogleCloud;

use crate::{config::Config, types::OsError};

pub struct StoragePath {
    pub dir: String,
    pub file: String,
}

#[async_trait::async_trait]
pub trait Storage: Send + Sync + std::fmt::Debug {
    // store should be idempotent
    async fn store(&self, path: &StoragePath, content_length: u64, data: &[u8]) -> Result<()>;
    async fn fetch(&self, path: &StoragePath, content_length: u64) -> Result<Vec<u8>>;

    fn validate_content_length(
        &self,
        path: &StoragePath,
        content_length: u64,
        data: &[u8],
    ) -> Result<()> {
        if data.len() as u64 != content_length {
            Err(StorageError::ContentLengthError(format!(
                "expected content length of {} and fetched content length of {} for {}/{}",
                content_length,
                data.len(),
                &path.dir,
                &path.file,
            )))
        } else {
            Ok(())
        }
    }
}

pub fn new_storage(config: &Config) -> core::result::Result<Arc<Box<dyn Storage>>, OsError> {
    let storage = match config.storage_type.as_str() {
        "file_system" => Ok(Box::new(FileSystem::new(PathBuf::from(
            config.storage_base_path.as_str(),
        ))) as Box<dyn Storage>),
        "google_cloud" => Ok(Box::new(GoogleCloud::new(
            config.storage_base_url.clone(),
            config.storage_base_path.clone(),
        )) as Box<dyn Storage>),
        _ => Err(OsError::InvalidApplicationState("".to_owned())),
    }?;

    Ok(Arc::new(storage))
}
