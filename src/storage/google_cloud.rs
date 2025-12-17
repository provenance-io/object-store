use std::path::{Path, PathBuf};

use bytes::Bytes;
use fastrace_macro::trace;
use google_cloud_storage::client::Storage as GcsStorage;

use crate::storage::{Result, Storage, StorageError, StoragePath};

#[derive(Debug)]
pub struct GoogleCloud {
    client: GcsStorage,
    base_url: Option<String>,
    bucket_id: String,
}

impl GoogleCloud {
    pub async fn new(
        base_url: Option<String>,
        bucket_id: String,
    ) -> std::result::Result<Self, google_cloud_gax::client_builder::Error> {
        let client: GcsStorage = GcsStorage::builder().build().await?;

        Ok(GoogleCloud {
            client,
            base_url,
            bucket_id,
        })
    }

    fn bucket(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket_id)
    }

    fn get_path(&self, path: &StoragePath) -> PathBuf {
        Path::new(&path.dir).join(&path.file)
    }
}

#[async_trait::async_trait]
impl Storage for GoogleCloud {
    #[trace(name = "google_cloud::store")]
    async fn store(&self, path: &StoragePath, content_length: u64, data: &[u8]) -> Result<()> {
        if let Err(e) = self.validate_content_length(path, content_length, data) {
            log::warn!("{:?}", e);
        }

        let bucket_path = self.bucket();
        let full_path = self.get_path(path);

        self.client
            .write_object(
                &bucket_path,
                full_path.to_str().unwrap(),
                Bytes::copy_from_slice(data),
            )
            .send_buffered()
            .await
            .map_err(|e| {
                StorageError::IoError(format!(
                    "Unable to store file: {:?} in bucket {}, {:?}",
                    full_path, bucket_path, e
                ))
            })?;

        Ok(())
    }

    #[trace(name = "google_cloud::fetch")]
    async fn fetch(&self, path: &StoragePath, content_length: u64) -> Result<Vec<u8>> {
        let bucket_path = self.bucket();
        let full_path = self.get_path(path);

        // TODO move map error into an impl and add extra info
        let mut reader = self
            .client
            .read_object(&bucket_path, full_path.to_str().unwrap())
            .send()
            .await
            .map_err(|e| {
                StorageError::IoError(format!(
                    "Unable to fetch file: {:?} from bucket {}, {:?}",
                    full_path, bucket_path, e
                ))
            })?;

        let data = {
            let mut data = Vec::new();

            while let Some(chunk) = reader.next().await.transpose().map_err(|e| {
                StorageError::IoError(format!(
                    "Unable to read file: {:?} from bucket {}, {:?}",
                    full_path, bucket_path, e
                ))
            })? {
                data.extend_from_slice(&chunk);
            }

            data
        };

        if let Err(e) = self.validate_content_length(path, content_length, &data) {
            log::warn!("{:?}", e);
        }

        Ok(data)
    }
}
