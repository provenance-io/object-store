mod error;
mod file_system;
mod google_cloud;

use std::{fmt::Display, path::PathBuf, sync::Arc};

// forwarding declarations
pub use error::*;
pub use file_system::FileSystem;
pub use google_cloud::GoogleCloud;

use crate::{config::StorageConfig, types::OsError};

// TODO implement checksum in filestore

pub struct StoragePath {
    pub dir: String,
    pub file: String,
}

impl Display for StoragePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.dir, self.file)
    }
}

#[async_trait::async_trait]
pub trait Storage: Send + Sync + std::fmt::Debug {
    async fn health_check(&self) -> Result<()>;

    // store should be idempotent
    async fn store(&self, path: &StoragePath, content_length: usize, data: &[u8]) -> Result<()>;
    async fn fetch(&self, path: &StoragePath, content_length: usize) -> Result<Vec<u8>>;
    async fn delete(&self, path: &StoragePath) -> Result<()>;

    fn validate_content_length(
        &self,
        path: &StoragePath,
        content_length: usize,
        data: &[u8],
    ) -> Result<()> {
        if data.len() != content_length {
            Err(StorageError::ContentLengthError(format!(
                "expected ({}) and actual ({}) content lengths do not match for {}",
                content_length,
                data.len(),
                path,
            )))
        } else {
            Ok(())
        }
    }
}

// TODO fix await unwrap
pub async fn new_storage(
    config: &StorageConfig,
) -> core::result::Result<Arc<Box<dyn Storage>>, OsError> {
    let storage = match config.storage_type.as_str() {
        "file_system" => Ok(Box::new(FileSystem::new(PathBuf::from(
            config.storage_base_path.as_str(),
        ))) as Box<dyn Storage>),
        "google_cloud" => {
            let google_cloud = GoogleCloud::new(config.storage_base_path.clone())
                .await
                .unwrap();

            Ok(Box::new(google_cloud) as Box<dyn Storage>)
        }
        _ => Err(OsError::InvalidApplicationState("".to_owned())),
    }?;

    if config.health_check {
        storage.health_check().await?;
    }

    Ok(Arc::new(storage))
}
