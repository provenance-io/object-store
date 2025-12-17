use bytes::Bytes;
use fastrace_macro::trace;
use google_cloud_storage::client::Storage as GcsStorage;

use crate::storage::{Result, Storage, StorageError, StoragePath};

#[derive(Debug)]
pub struct GoogleCloud {
    client: GcsStorage,
    bucket_id: String,
}

impl GoogleCloud {
    pub async fn new(
        bucket_id: String,
    ) -> std::result::Result<Self, google_cloud_gax::client_builder::Error> {
        let client: GcsStorage = GcsStorage::builder().build().await?;

        Ok(GoogleCloud { client, bucket_id })
    }

    fn bucket(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket_id)
    }
}

#[async_trait::async_trait]
impl Storage for GoogleCloud {
    #[trace(name = "google_cloud::store")]
    async fn store(&self, path: &StoragePath, content_length: u64, data: &[u8]) -> Result<()> {
        if let Err(e) = self.validate_content_length(path, content_length, data) {
            log::warn!("{:?}", e);
        }

        // TODO bucket format = projects/_/buckets/{bucket_id} format.
        self.client
            .write_object(self.bucket(), &path.file, Bytes::copy_from_slice(data))
            .send_buffered()
            .await
            .map_err(|e| StorageError::IoError(format!("{:?}", e)))?;

        Ok(())
    }

    #[trace(name = "google_cloud::fetch")]
    async fn fetch(&self, path: &StoragePath, content_length: u64) -> Result<Vec<u8>> {
        // TODO move map error into an impl and add extra info
        let mut reader = self
            .client
            .read_object(self.bucket(), &path.file)
            .send()
            .await
            .map_err(|e| StorageError::IoError(format!("{:?}", e)))?;

        let data = {
            let mut data = Vec::new();
            while let Some(chunk) = reader
                .next()
                .await
                .transpose()
                .map_err(|e| StorageError::IoError(format!("{:?}", e)))?
            {
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
