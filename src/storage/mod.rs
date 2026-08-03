mod error;
mod file_system;
mod google_cloud;

use std::{fmt::Display, path::PathBuf, str::FromStr, sync::Arc};

// forwarding declarations
pub use error::*;
pub use file_system::FileSystem;
pub use google_cloud::GoogleCloud;

use crate::{config::StorageConfig, domain::OsError};

// TODO implement checksum in filestore

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageType {
    FileSystem = 0,
    GoogleCloud = 1,
}

impl FromStr for StorageType {
    type Err = String;

    fn from_str(s: &str) -> core::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "file_system" => Ok(StorageType::FileSystem),
            "google_cloud" => Ok(StorageType::GoogleCloud),
            _ => Err(format!("Invalid storage: {}", s)),
        }
    }
}

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

pub async fn new_storage(
    config: &StorageConfig,
) -> core::result::Result<Arc<dyn Storage>, OsError> {
    let storage: Arc<dyn Storage> = match config.storage_type {
        StorageType::FileSystem => Arc::new(FileSystem::new(PathBuf::from(&config.base_path))),
        StorageType::GoogleCloud => Arc::new(GoogleCloud::new(config.base_path.clone()).await?),
    };

    if config.health_check {
        storage.health_check().await?; // TODO connect to HealthReporter
    }

    Ok(storage)
}
